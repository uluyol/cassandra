/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.cassandra.compactlb.Coordination;
import org.apache.cassandra.compactlb.CoordinatorGrpc;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public final class CompactionCoordinatorService {
    private static final long UPDATE_PERIOD_MS = 1000L;

    private static final Logger logger = LoggerFactory.getLogger(CompactionCoordinatorService.class);
    private static CompactionCoordinatorService instance;

    private final Channel channel;
    private final CoordinatorGrpc.CoordinatorFutureStub stub;
    private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;

    private CompactionCoordinatorService(String addr) {
        channel = ManagedChannelBuilder.forAddress(addr, 50051).usePlaintext(true).build();
        stub = CoordinatorGrpc.newFutureStub(channel);
        blockingStub = CoordinatorGrpc.newBlockingStub(channel);
    }

    public static void init() {
        if (DatabaseDescriptor.compactionCoordinator().equals("")) {
            return;
        }
        instance = new CompactionCoordinatorService(DatabaseDescriptor.compactionCoordinator());
        String ip = DatabaseDescriptor.getBroadcastAddress().toString();
        instance.blockingStub.register(
                Coordination.RegisterReq.newBuilder()
                                        .setServerIp(ip)
                                        .build());
        instance.startWatchThread();
        instance.startUpdateThread();
    }

    private void startWatchThread() {
        new Thread(() -> {
            String ip = DatabaseDescriptor.getBroadcastAddress().toString();
            Coordination.WatchReq req = Coordination.WatchReq.newBuilder().setServerIp(ip).build();

            while (true) {
                logger.info("Watching for responses from coordinator");
                try {
                    Iterator<Coordination.WatchResp> reqs = blockingStub.watch(req);
                    reqs.forEachRemaining((watchResp) -> {
                        ListenableFuture<Coordination.Empty> sendPHFuture = Futures.immediateFuture(Coordination.Empty.getDefaultInstance());
                        if (watchResp.getSendReplicaSets()) {
                           sendPHFuture = stub.update(Coordination.UpdateReq.newBuilder()
                                                                            .addAllReplicaSets(getReplicaSets())
                                                                            .build());
                        }
                        if (watchResp.getReplicaSetWeightsCount() > 0) {
                            try {
                                ReplicaSetWeightMap.current.update(watchResp.getReplicaSetWeightsList());
                            } catch (UnknownHostException e) {
                                logger.warn("Unable to convert host to InetAddress", e);
                            }
                        }
                        try {
                            sendPHFuture.get();
                        } catch (Exception e) {
                            logger.warn("Encountered exception when sending parition host update");
                        }
                    });
                } catch (Throwable e) {
                    logger.warn("Encountered exception when listening to or executing watch", e);
                    // reregister
                    loopReregister();
                }
            }
        }).start();
    }

    private void loopReregister() {
        String ip = DatabaseDescriptor.getBroadcastAddress().toString();
        boolean success = false;
        while (!success)
        {
            try {
                instance.blockingStub.register(
                Coordination.RegisterReq.newBuilder()
                                        .setServerIp(ip)
                                        .build());
                success = true;
            } catch (Exception e) {
                logger.info("Retrying reregister in 1 second");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {}
            }
        }
    }

    static Comparator<ImmutableList<String>> rsComparator = new Comparator<ImmutableList<String>>() {
        public int compare(ImmutableList<String> o1, ImmutableList<String> o2) {
            // sort by size, then elements
            int cmp = Integer.compare(o1.size(), o2.size());
            if (cmp != 0) {
                return cmp;
            }
            for (int i=0; i < o1.size(); i++) {
                cmp = o1.get(i).compareTo(o2.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    };

    private static Iterable<Coordination.ReplicaSet> getReplicaSets() {
        NavigableSet<ImmutableList<String>> replicaSets = new TreeSet<>(rsComparator);

        for (Keyspace ks : Keyspace.all()) {
            Multimap<Range<Token>, InetAddress> rangeAddrs = ks.getReplicationStrategy().getRangeAddresses(StorageService.instance.getTokenMetadata().cloneOnlyTokenMap());
            for (Range<Token> key : rangeAddrs.keySet()) {
                List<String> p = Lists.newArrayList();
                for (InetAddress a : rangeAddrs.get(key)) {
                    p.add(a.getHostAddress());
                }
                Collections.sort(p);
                replicaSets.add(ImmutableList.copyOf(p));
            }
        }

        return Iterables.transform(replicaSets, (rs) ->
                                                Coordination.ReplicaSet.newBuilder().addAllHostIps(rs).build());
    }

    private void startUpdateThread() {
        if (SystemUtils.IS_OS_LINUX) {
            new Thread(() -> {
                String ip = DatabaseDescriptor.getBroadcastAddress().toString();
                Set<String> devNames = new HashSet<String>();
                for (String p : DatabaseDescriptor.getAllDataFileLocations()) {
                    String out;
                    try {
                        out = execGetOutput(new String[]{"df", p});
                    } catch (Exception e) {
                        logger.warn("Unable to get device name for %s: %s", p, e);
                        continue;
                    }
                    out = StringUtils.strip(out);
                    String[] lines = StringUtils.split(out, "\n");
                    if (lines.length < 2) {
                        logger.warn("Unexpected output from df: %s", out);
                        continue;
                    }
                    String[] fields = StringUtils.split(lines[1]);
                    String devPath = fields[0];
                    if (Files.isSymbolicLink(Paths.get(devPath))) {
                        try {
                            devPath = Files.readSymbolicLink(Paths.get(devPath)).toString();
                        } catch (IOException e) {
                            logger.warn("Unable to resolve link: " + e);
                            continue;
                        }
                    }
                    String name = FilenameUtils.getBaseName(devPath);
                    if (!Files.exists(Paths.get("/sys/block", name))) {
                        name = name.replaceAll("[0-9]*$", ""); // remove trailing numbers for sdXN -> sdX
                    }
                    if (!Files.exists(Paths.get("/sys/block", name))) {
                        logger.warn("Unable to get stats for %s", p);
                        continue;
                    }
                    devNames.add(name);
                }

                while (true) {
                    long readIOs = 0;
                    long writeIOs = 0;
                    long readBytes = 0;
                    long writeBytes = 0;
                    long startTime = System.currentTimeMillis();
                    for (String name : devNames) {
                        try {
                            String stats = new String(Files.readAllBytes(Paths.get("/sys/block", name, "stat")), "utf-8");
                            String[] fields = StringUtils.split(stats);
                            if (fields.length < 8) {
                                logger.warn("Invalid stats: %s", stats);
                                continue;
                            }
                            readIOs += Long.parseLong(fields[0]);
                            writeIOs += Long.parseLong(fields[4]);
                            readBytes += Long.parseLong(fields[2])*512;
                            writeBytes += Long.parseLong(fields[6])*512;
                        } catch (Exception e) {
                            logger.warn("Error occurred while getting stats for %s: %s", name, e);
                            continue;
                        }
                    }
                    logger.debug("Sending updated disk stats to coordinator");
                    blockingStub.update(Coordination.UpdateReq.newBuilder()
                                                              .setServerIp(ip)
                                                              .setNumPending(CompactionManager.instance.getPendingTasks())
                                                              .setIoStats(
                                                              Coordination.IOStats.newBuilder()
                                                                                  .setReadIos(readIOs)
                                                                                  .setWriteIos(writeIOs)
                                                                                  .setReadBytes(readBytes)
                                                                                  .setWriteBytes(writeBytes))
                                                              .build());
                    while (System.currentTimeMillis() < startTime + UPDATE_PERIOD_MS) {
                        try {
                            Thread.sleep(System.currentTimeMillis()-startTime);
                        } catch (InterruptedException e) {}
                    }
                }
            }).start();
        }
        // don't know what to do for other OS's
    }

    private static String execGetOutput(String[] cmd) throws Exception {
        Runtime rt = Runtime.getRuntime();
        Process proc = rt.exec(cmd);
        InputStream outSt = proc.getInputStream();
        InputStream errSt = proc.getErrorStream();
        String out = IOUtils.toString(outSt, "utf-8");
        String err = IOUtils.toString(errSt, "utf-8");
        proc.waitFor();
        if (proc.exitValue() != 0) {
            throw new Exception("Command failed: " + cmd[0] + ": " + err);
        }
        return out;
    }
}
