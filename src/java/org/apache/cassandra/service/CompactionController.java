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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Controllers;
import org.apache.cassandra.hists.Hists;
import org.apache.cassandra.net.MessageIn;

public final class CompactionController {
    public static CompactionController instance;

    private final Controllers.Percentile ctlr;
    private final ArrayBlockingQueue<Double> recQ = new ArrayBlockingQueue(512);

    public final AtomicLong compactionRate = new AtomicLong();

    public static void init() {
        double stepSize = DatabaseDescriptor.compactionControllerStepSizeMBPS();
        double remainFrac = DatabaseDescriptor.compactionControllerRemainFrac();
        double refOut = DatabaseDescriptor.compactionControllerSLOMillis();
        double maxInput = DatabaseDescriptor.compactionControllerMaxRateMBPS();
        double initInput = DatabaseDescriptor.compactionControllerMaxRateMBPS();
        double pct = DatabaseDescriptor.compactionControllerSLOPercentile() / 100.0;
        int winSize = DatabaseDescriptor.compactionControllerPercentileWindow();
        double highFudgeFactor = DatabaseDescriptor.compactionControllerPercentileHighFudgeFactor();

        if (refOut == 0) {
            return;
        }

        instance = new CompactionController(stepSize, remainFrac, refOut, maxInput, initInput, pct, winSize, highFudgeFactor);
    }

    public CompactionController(double stepSize, double remainFrac, double refOut, double maxInput, double initInput, double pct, int winSize, double highFudgeFactor) {
        ctlr = Controllers.newPercentile(Controllers.newAIMD(stepSize, remainFrac, refOut, maxInput, initInput),
                                         pct, winSize, highFudgeFactor);

        Hists.reads.registerHook((meta, end) -> record(meta, end));

        new Thread(() -> {
            while (true)
            {
                double v = 0;
                try {
                    v = recQ.take();
                } catch (InterruptedException e) { continue; }
                double input;
                synchronized (ctlr) {
                    ctlr.record(CompactionManager.instance.getRateLimiter().getRate() / (1024*1024), v);
                    input = ctlr.getInput();
                }
                CompactionManager.instance.getRateLimiter().setRate(input * 1024 * 1024);
                compactionRate.set((long)(input * 1024 * 1024));
            }
        }).start();
    }

    private void record(MessageIn.MessageMeta meta, Instant end) {
        Double v = new Double(Duration.between(meta.getStart(), end).toNanos() / 1e6);
        while (true) {
            try {
                recQ.put(v);
                break;
            } catch (InterruptedException e) {}
        }
    }

    public void setPercentile(double pct) {
        synchronized (ctlr) {
            ctlr.setPercentile(pct);
        }
    }

    public void setReference(double ref) {
        synchronized (ctlr) {
            ctlr.setReference(ref);
        }
    }
}
