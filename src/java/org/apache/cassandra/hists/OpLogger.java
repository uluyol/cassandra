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

import org.apache.cassandra.config.DatabaseDescriptor;

public final class OpLogger
{
    // these need to be at the top so that they are initiliazed when
    // creating the loggers.
    private static Thread flusher = null;
    private static final List<OpLogger> loggers = Collections.synchronizedList(new ArrayList<>());

    private static final OpLogger _flushes =
        new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "flush_time_log.csv"));
    private static final OpLogger _compactions =
        new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "compaction_time_log.csv"));
    private static final OpLogger _compactionRates =
        new OpLogger(Paths.get(DatabaseDescriptor.getOpLogDir(), "compaction_rate_log.csv"));

    private static final int LOG_CAPACITY = 1024;
    private static final int WRITE_PERIOD_SECONDS = 15;

    private List<RecVal> log = null;
    private OutputStream w = null;

    public OpLogger(Path path) {
        ArrayList<RecVal> l = new ArrayList<>();
        l.ensureCapacity(LOG_CAPACITY);
        log = l;
        try {
            w = Files.newOutputStream(path);
            w.write("# log file format is below\n# start,duration[,aux]\n".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(23);
        }

        addLogger(this);
    }

    private synchronized static void addLogger(OpLogger logger) {
        if (flusher == null) {
            assert loggers != null;
            flusher = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(WRITE_PERIOD_SECONDS * 1000);
                    }
                    catch (InterruptedException $) {}
                    for (OpLogger l : loggers) {
                        l.flush();
                    }
                }
            });
            flusher.start();
        }

        loggers.add(logger);
    }

    public static OpLogger flushes() { return _flushes; }
    public static OpLogger compactions() { return _compactions; }
    public static OpLogger compactionRates() { return _compactionRates; }

    public void record(Instant start, Instant stop) {
        recordRaw(new RecVal(start, stop));
    }

    void recordRaw(RecVal v) {
        synchronized (this) {
            if (log.size() == LOG_CAPACITY) {
                flush();
            }
            log.add(v);
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
                w.flush();
                log.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(24);
        }
    }

    static final class RecVal {
        public final long startMicros;
        public final long val;
        public final String aux;

        public RecVal(Instant start, Instant stop) {
            startMicros = start.getEpochSecond() * 1_000_000L + start.getNano() / 1000L;
            Duration d = Duration.between(start, stop);
            val = d.getSeconds() * 1_000_000L + d.getNano() / 1000L;
            aux = null;
        }

        public RecVal(Instant time, long v, String aux) {
            startMicros = time.getEpochSecond() * 1_000_000L + time.getNano() / 1000L;
            val = v;
            this.aux = aux;
        }
    }
}
