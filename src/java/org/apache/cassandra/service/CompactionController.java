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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Controllers;
import org.apache.cassandra.hists.Hists;
import org.apache.cassandra.hists.NanoClock;
import org.apache.cassandra.hists.OpLoggers;
import org.apache.cassandra.net.MessageIn;

public abstract class CompactionController {
    public static final Logger logger = Logger.getLogger(CompactionController.class);

    public static CompactionController instance;

    public abstract double getCurRate();
    public abstract void setPercentile(double pct);
    public abstract void setReference(double pct);

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

        CompactionControllerImpl ctrlr = new CompactionControllerImpl(stepSize, remainFrac, refOut, 0.9,
                                                                      maxInput, initInput, pct, winSize,
                                                                      highFudgeFactor);
        ctrlr.startControlThread();
        ctrlr.startStatusThread();
        instance = ctrlr;
    }

    private static final class DummyCompactionController extends CompactionController {
        @Override
        public double getCurRate() {
            int tput = DatabaseDescriptor.getCompactionThroughputMbPerSec();
            if (tput == 0) {
                return Double.MAX_VALUE;
            }
            return tput;
        }
        @Override
        public void setPercentile(double pct) {}
        @Override
        public void setReference(double ref) {}
    }

    private static final class CompactionControllerImpl extends CompactionController {
        private static final double BAD_MODE_RATE_THRESH_MBPS = 0.25;

        private final double initInput;
        private final Controllers.Percentile ctlr;
        private final ArrayBlockingQueue<Double> recQ = new ArrayBlockingQueue<>(512);
        private final ArrayBlockingQueue<LoggingState> stateQ = new ArrayBlockingQueue<>(100);

        private CurState state = new CurState(getRateFromConfig());

        private static double getRateFromConfig() {
            int tput = DatabaseDescriptor.getCompactionThroughputMbPerSec();
            if (tput == 0) {
                return Double.MAX_VALUE;
            }
            return tput;
        }

        @Override
        public double getCurRate() {
            synchronized (state) {
                if (state.canMeetSLO.get()) {
                    return state.rateMBPS;
                }
                return Double.MAX_VALUE;
            }
        }

        CompactionControllerImpl(double stepSize, double remainFrac, double refOut, double fuzzyRefMatch,
                                 double maxInput, double initInput, double pct, int winSize, double highFudgeFactor) {
            ctlr = Controllers.newPercentile(
                    Controllers.newAIMD(stepSize, remainFrac, refOut, fuzzyRefMatch, 0, maxInput, initInput),
                    pct, winSize, highFudgeFactor);
            this.initInput = initInput;

            Hists.reads.registerHook(this::record);
            OpLoggers.compactions().registerHook(this::compactionDone);
        }

        final void startControlThread() {
            new Thread(() -> {
                while (true) {
                    try {
                        double v;
                        try {
                            v = recQ.take();
                        } catch (InterruptedException e) {
                            continue;
                        }
                        LoggingState logState;
                        boolean newLogState = true;
                        synchronized (state) {
                            double curRate = state.rateMBPS;
                            double input;
                            String ctlrAux;
                            synchronized (ctlr) {
                                ctlr.record(curRate, v);
                                input = ctlr.getInput();
                                ctlrAux = ctlr.getAux();
                            }
                            if (input <= BAD_MODE_RATE_THRESH_MBPS) {
                                state.canMeetSLO.set(false);
                                newLogState = true;
                            } else {
                                if (state.rateMBPS == input) {
                                    newLogState = false;
                                }
                                state.rateMBPS = input;
                            }
                            if (newLogState) {
                                logState = new LoggingState();
                                logState.start = Instant.now(NanoClock.instance);
                                logState.canMeetSLO = state.canMeetSLO.get();
                                logState.rateMBPS = state.rateMBPS;
                                logState.ctlrAux = ctlrAux;
                            } else {
                                logState = null;
                            }
                        }
                        if (newLogState) {
                            if (!stateQ.offer(logState)) {
                                logger.error("too many logging states, status thread is unable to consume states quickly enough");
                            }
                        }
                    } catch (Throwable t) {
                        logger.error("error occurred while consuming compaction latencies", t);
                    }
                }
            }).start();
        }

        final void startStatusThread() {
            // Status thread
            new Thread(() -> {
                LoggingState prevState = null;
                while (true) {
                    try {
                        LoggingState state = stateQ.poll(10, TimeUnit.SECONDS);
                        if (state == null) {
                            state = prevState;
                        }
                        if (state != null) {
                            state.log();
                            state.start = Instant.now(NanoClock.instance);
                        }
                        prevState = state;
                    } catch (Throwable t) {
                        logger.error("error occured while print compaction rate status", t);
                    }
                }
            }).start();
        }

        public void setPercentile(double pct) {
            synchronized (ctlr) {
                ctlr.setPercentile(pct);
            }
        }

        public void setReference(double ref)
        {
            synchronized (ctlr)
            {
                ctlr.setReference(ref);
            }
        }

        private static String getAux(boolean canMeetSLO, String ctlrAux) {
            int wipAndPendingCompactions = CompactionManager.instance.getPendingTasks();
            String tplMap = tablesPerLevelSupplier.get();
            StringBuilder aux = new StringBuilder();
            aux.append("levelCount=");
            aux.append(tplMap);
            aux.append(",pending=");
            aux.append(wipAndPendingCompactions);
            if (ctlrAux != null && !ctlrAux.isEmpty()) {
                aux.append(',');
                aux.append(ctlrAux);
            }
            if (!canMeetSLO) {
                aux.append(",recoveryMode");
            }
            return aux.toString();
        }

        private final void resetCanMeetSLOAndController() {
            // No data race here, but there is a semantic one.
            // The semantic race may fail to cause this to trigger,
            // or may cause it to opt-out of recovery mode too soon.
            // This is unlikely and should be corrected quickly anyway.
            if (!state.nowCanMeetSLO()) {
                synchronized (state) {
                    state.canMeetSLO.set(true);
                    synchronized (ctlr) {
                        ctlr.resetInput(initInput);
                    }
                }
            }
        }

        private final void record(MessageIn.MessageMeta meta, Instant end) {
            // Controller should only take action when a compaction is running.
            // Latencies taken at other times are meaningless.
            if (!Hists.overlapCompaction(meta.getStart(), end)) {
                resetCanMeetSLOAndController();
                return;
            }
            if (!state.canMeetSLO.get()) {
                return;
            }
            Double v = Duration.between(meta.getStart(), end).toNanos() / 1e6;
            while (true) {
                try {
                    recQ.put(v);
                    break;
                } catch (InterruptedException e) {}
            }
        }

        private final void compactionDone(OpLoggers.RecVal v) {
            resetCanMeetSLOAndController();
        }

        static final class LoggingState {
            Instant start = null;
            double rateMBPS = 0;
            boolean canMeetSLO = true;
            String ctlrAux = "";

            // log does not modify anything.
            public final void log() {
                if (start != null) {
                    long inputToReport = (long) (rateMBPS * 1024 * 1024);
                    if (!canMeetSLO) {
                        inputToReport = -1;
                    }
                    OpLoggers.compactionRates().recordValue(start, inputToReport,
                                                            getAux(canMeetSLO, ctlrAux));
                }
            }
        }

        static final class CurState {
            double rateMBPS;
            AtomicBoolean canMeetSLO = new AtomicBoolean(true);

            CurState(double rateMBPS) {
                this.rateMBPS = rateMBPS;
            }

            boolean nowCanMeetSLO() {
                return canMeetSLO.get();
            }
        }
    }

    public static final Supplier<String> tablesPerLevelSupplier = Suppliers.memoizeWithExpiration(
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
}
