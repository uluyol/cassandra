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

package org.apache.cassandra.hists;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;

public final class OpLoggers {
    private static Logger logger = Logger.getLogger(OpLoggers.class);

    // these need to be at the top so that they are initiliazed when
    // creating the loggers.
    private final Thread flusher;
    private final ImmutableList<OpLogger> loggers;

    private final OpLogger _flushes;
    private final OpLogger _compactions;
    private final OpLogger _compactionRates;
    private final OpLogger _periodicStats;
    private final OpLogger _writes;

    private static final int WRITE_PERIOD_SECONDS = 15;

    private static final OpLoggers instance = new OpLoggers();

    public OpLoggers() {
        _flushes = new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "flush_time_log.csv"));
        _compactions = new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "compaction_time_log.csv"));
        _compactionRates = new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "compaction_rate_log.csv"));
        _periodicStats = new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "periodic_stats.csv"));
        ImmutableList.Builder<OpLogger> loggersB = ImmutableList.builder();
        loggersB.add(_flushes, _compactions, _compactionRates, _periodicStats);
        if (DatabaseDescriptor.logWriteOps()) {
            _writes = new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "write_ops.csv"));
            loggersB.add(_writes);
        } else {
            _writes = null;
        }

        loggers = loggersB.build();
        flusher = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(WRITE_PERIOD_SECONDS * 1000);
                } catch (InterruptedException e) {}
                for (OpLogger l : loggers) {
                    try {
                        l.flush();
                    } catch (Throwable t) {
                        logger.error("error while flushing", t);
                    }
                }
            }
        });
        flusher.start();
    }

    public static OpLogger flushes() { return instance._flushes; }
    public static OpLogger compactions() { return instance._compactions; }
    public static OpLogger compactionRates() { return instance._compactionRates; }
    public static OpLogger writes() { return instance._writes; }
    static OpLogger periodicStats() { return instance._periodicStats; }

    public static final class RecVal {
        public final long startMicros;
        public final long val;
        public final String aux;

        public RecVal(Instant start, Instant stop, String aux) {
            startMicros = start.getEpochSecond() * 1_000_000L + start.getNano() / 1000L;
            Duration d = Duration.between(start, stop);
            val = d.getSeconds() * 1_000_000L + d.getNano() / 1000L;
            this.aux = aux;
        }

        public RecVal(Instant time, long v, String aux) {
            startMicros = time.getEpochSecond() * 1_000_000L + time.getNano() / 1000L;
            val = v;
            this.aux = aux;
        }
    }

    public static final class OpLogger {
        private static final int LOG_CAPACITY = 1024;

        private final List<RecVal> log;
        private final OutputStream w;

        private final Object hooksWriteLock = new Object();
        private volatile ImmutableList<RecordHook> hooks = ImmutableList.of();

        OpLogger(Path p) {
            ArrayList<RecVal> l = new ArrayList<>();
            l.ensureCapacity(LOG_CAPACITY);
            log = l;
            OutputStream os = null;
            try {
                os = Files.newOutputStream(p);
                os.write("# log file format is below\n# start,duration[,aux]\n".getBytes());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(23);
            }
            w = os;
        }

        public void record(Instant start, Instant stop) { recordRaw(new RecVal(start, stop, null)); }
        public void record(Instant start, Instant stop, String aux) { recordRaw(new RecVal(start, stop, aux)); }

        void recordRaw(RecVal v) {
            synchronized (this) {
                if (log.size() == LOG_CAPACITY) {
                    flush();
                }
                log.add(v);
            }

            ImmutableList<RecordHook> hooks = this.hooks;
            for (RecordHook h : hooks) {
                h.record(v);
            }
        }

        public void recordValue(Instant now, long val) { recordRaw(new RecVal(now, val, null)); }
        public void recordValue(Instant now, long val, String aux) { recordRaw(new RecVal(now, val, aux)); }

        public void flush() {
            try {
                synchronized (this)
                {
                    for (RecVal v : log)
                    {
                        String out = v.startMicros + "," + v.val;
                        if (v.aux == null) {
                            out += "\n";
                        } else {
                            out += "," + v.aux + "\n";
                        }
                        w.write(out.getBytes());
                    }
                    log.clear();
                    w.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(24);
            }
        }

        @FunctionalInterface
        public interface RecordHook {
            public void record(RecVal v);
        }

        public void registerHook(RecordHook h) {
            synchronized (hooksWriteLock) {
                hooks = ImmutableList.<RecordHook>builder().addAll(hooks).add(h).build();
            }
        }
    }

    private static PeriodicStatsCollector _periodicCollector = new PeriodicStatsCollector();

    private static final class PeriodicStatsCollector {
        final Thread t;

        public PeriodicStatsCollector() {
            t = new Thread(() -> {
                while (true) {
                    try {
                        String aux = getAux();
                        OpLoggers.periodicStats().recordValue(Instant.now(NanoClock.instance), 0, aux);
                        Thread.sleep(5000);
                    } catch (Exception e) {}
                }
            });
            t.start();
        }

        private static String getAux() {
            StringBuilder sb = new StringBuilder();
            sb.append("pending=");
            sb.append(CompactionManager.instance.getPendingTasks());
            sb.append(",completedTasks=");
            sb.append(CompactionManager.instance.getCompletedTasks());
            sb.append(",totalCompleted=");
            sb.append(CompactionManager.instance.getTotalCompactionsCompleted());
            sb.append(",totalBytes=");
            sb.append(CompactionManager.instance.getTotalBytesCompacted());
            return sb.toString();
        }
    }
}
