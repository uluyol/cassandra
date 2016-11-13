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

import java.util.Iterator;

import edu.umich.compaction.Coordination;
import edu.umich.compaction.CoordinatorGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;

public final class CompactionCoordinatorService {
    private static CompactionCoordinatorService instance;

    private final String addr;
    private final Channel channel;
    private final CoordinatorGrpc.CoordinatorFutureStub stub;
    private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;

    private CompactionCoordinatorService(String addr) {
        this.addr = addr;
        channel = ManagedChannelBuilder.forAddress(addr, 50051).build();
        stub = CoordinatorGrpc.newFutureStub(channel);
        blockingStub = CoordinatorGrpc.newBlockingStub(channel);
    }

    public static void init() {
        instance = new CompactionCoordinatorService(DatabaseDescriptor.compactionCoordinator());
        instance.blockingStub.register(); // TODO: fill me
        instance.startWatchThread();
        instance.startLoadUpdateThread();
    }

    private void startWatchThread() {
        new Thread(() -> {
            String ip = DatabaseDescriptor.getBroadcastAddress().toString();
            Coordination.WatchReq req = Coordination.WatchReq.newBuilder().setServerIp(ip).build();
            Iterator<Coordination.ExecCompaction> reqs = blockingStub.watchCompactions(req);
            reqs.forEachRemaining((compaction) -> {
                System.out.println("x"); // TODO: fixme
            });
        });
    }

    private void startLoadUpdateThread() { }

    public static void queueCompaction(Coordination.QueueCompactionReq req) { instance.stub.queueCompaction(req); }
}
