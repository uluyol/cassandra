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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Controllers;
import org.apache.cassandra.hists.Hists;
import org.apache.cassandra.hists.NanoClock;
import org.apache.cassandra.hists.OpLogger;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.net.MessageIn;

public class CompactionController {
    public static final Logger logger = Logger.getLogger(CompactionController.class);

    public static CompactionController instance;

    protected final Controllers.Percentile ctlr;
    private final ArrayBlockingQueue<Double> recQ = new ArrayBlockingQueue(512);

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
            instance = new DummyCompactionController();
            return;
        }

        instance = new CompactionController(stepSize, remainFrac, refOut, maxInput, initInput, pct, winSize, highFudgeFactor);
    }

    CompactionController() { ctlr = null; }

    public CompactionController(double stepSize, double remainFrac, double refOut, double maxInput, double initInput, double pct, int winSize, double highFudgeFactor) {
        ctlr = Controllers.newPercentile(Controllers.newAIMD(stepSize, remainFrac, refOut, maxInput, initInput),
                                         pct, winSize, highFudgeFactor);

        Hists.reads.registerHook(this::record);

        new Thread(() -> {
            Instant prevStart = null;
            double prevInput = 0;
            long prevCount = 0;
            while (true) {
                try {
                    double v = 0;
                    try {
                        v = recQ.take();
                    } catch (InterruptedException e) {
                        continue;
                    }
                    double input;
                    synchronized (ctlr) {
                        ctlr.record(CompactionManager.instance.getRateLimiter().getRate() / (1024 * 1024), v);
                        input = ctlr.getInput();
                    }

                    // Logging
                    //
                    // Log the current rate under any of these conditions:
                    // - The compaction rate has changed
                    // - A maximum number of affected requests has been reached
                    // - A maximum duration since the last log message
                    Instant now = Instant.now(NanoClock.instance);
                    if (input != prevInput || prevCount >= 1000 || (prevStart != null && Duration.between(prevStart, now).getSeconds() > 10)) {
                        if (prevStart != null) {
                            OpLogger.compactionRates().recordValue(prevStart,
                                                                   (long) (prevInput * 1024 * 1024),
                                                                   getAux(prevCount));
                        }
                        prevCount = 0;
                        prevStart = now;
                    }
                    if (input != prevInput) {
                        CompactionManager.instance.getRateLimiter().setRate(input * 1024 * 1024);
                        prevStart = now;
                        prevInput = input;
                        prevCount = 0;
                    }
                    prevCount++;
                } catch (Throwable t) {
                    logger.error("error occurred while consuming compaction latencies", t);
                }
            }
        }).start();
    }

    private static final Supplier<String> tablesPerLevelSupplier = Suppliers.memoizeWithExpiration(
        () -> {
            StringBuilder sb = new StringBuilder();
            for (String ks : Schema.instance.getKeyspaces()) {
                boolean printed = false;
                for (ColumnFamilyStore cfs : Keyspace.open(ks).getColumnFamilyStores()) {
                    int[] tpl = cfs.getCompactionStrategyManager().getSSTableCountPerLevel();
                    if (tpl == null || tpl.length == 0) {
                        continue;
                    }
                    printed = true;
                    sb.append(ks);
                    sb.append('|');
                    sb.append(cfs.getTableName());
                    sb.append('|');
                    for (int i=0; i < tpl.length; i++) {
                        if (tpl[i] != 0) {
                            sb.append(i);
                            sb.append(':');
                            sb.append(tpl[i]);
                            sb.append(';');
                        }
                    }
                    sb.append('!');
                }
                if (printed) {
                    sb.append('~');
                }
            }
            return sb.toString();
        },
        50,
        TimeUnit.MILLISECONDS);

    private static String getAux(long count) {
        int wipAndPendingCompactions = CompactionManager.instance.getPendingTasks();
        String tplMap = tablesPerLevelSupplier.get();
        return "count=" + count + ",levelCount=" + tplMap + ",pending=" + wipAndPendingCompactions;
    }

    private void record(MessageIn.MessageMeta meta, Instant end) {
        // Controller should only take action when a compaction is running.
        // Latencies taken at other times are meaningless.
        if (!Hists.overlapCompaction(meta.getStart(), end)) {
            return;
        }
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

    private static final class DummyCompactionController extends CompactionController {
        @Override
        public void setPercentile(double pct) {}
        @Override
        public void setReference(double ref) {}
    }
}
