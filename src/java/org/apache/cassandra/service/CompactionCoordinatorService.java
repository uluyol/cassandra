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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umich.compaction.Coordination;
import edu.umich.compaction.CoordinatorGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;

public final class CompactionCoordinatorService {
    private static final long UPDATE_LOAD_PERIOD_MS = 500;

    private static final Logger logger = LoggerFactory.getLogger(CompactionCoordinatorService.class);
    private static CompactionCoordinatorService instance;

    private final String addr;
    private final Channel channel;
    private final CoordinatorGrpc.CoordinatorFutureStub stub;
    private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;

    private CompactionCoordinatorService(String addr) {
        this.addr = addr;
        channel = ManagedChannelBuilder.forAddress(addr, 50051).usePlaintext(true).build();
        stub = CoordinatorGrpc.newFutureStub(channel);
        blockingStub = CoordinatorGrpc.newBlockingStub(channel);
    }

    public static void init() {
        instance = new CompactionCoordinatorService(DatabaseDescriptor.compactionCoordinator());
        String ip = DatabaseDescriptor.getBroadcastAddress().toString();
        instance.blockingStub.register(
        Coordination.RegisterReq.newBuilder()
                                .setServerIp(ip)
                                .setWriteBatchSize(DatabaseDescriptor.getRateLimitWriteBatchSize())
                                .setMaxIops(DatabaseDescriptor.getRateLimitMaxIOPS())
                                .build());
        instance.startWatchThread();
        instance.startLoadUpdateThread();
    }

    private void startWatchThread() {
        new Thread(() -> {
            String ip = DatabaseDescriptor.getBroadcastAddress().toString();
            Coordination.WatchReq req = Coordination.WatchReq.newBuilder().setServerIp(ip).build();
            while (true)
            {
                Iterator<Coordination.ExecCompaction> reqs = blockingStub.watchCompactions(req);
                reqs.forEachRemaining((compaction) ->
                                      {
                                          CompactionManager.instance.runGivenTask(compaction.getCompactionId());
                                      });
            }
        }).start();
    }

    private void startLoadUpdateThread() {
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
                    stub.updateLoad(Coordination.UpdateLoadReq.newBuilder()
                                                              .setServerIp(ip)
                                                              .setReadIos(readIOs)
                                                              .setWriteIos(writeIOs)
                                                              .setReadBytes(readBytes)
                                                              .setWriteBytes(writeBytes)
                                                              .build());
                    while (System.currentTimeMillis() < startTime+UPDATE_LOAD_PERIOD_MS) {
                        try {
                            Thread.sleep(System.currentTimeMillis()-startTime);
                        } catch (InterruptedException e) {}
                    }
                }
            }).start();
        }
        // don't know what to do for other OS's
    }

    public static void queueCompaction(Coordination.QueueCompactionReq req) { instance.stub.queueCompaction(req); }

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
