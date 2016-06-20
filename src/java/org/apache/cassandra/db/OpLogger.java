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

package org.apache.cassandra.db;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class OpLogger
{
    // these need to be at the top so that they are initiliazed when
    // creating the loggers.
    private static Thread flusher = null;
    private static List<OpLogger> loggers = Collections.synchronizedList(new ArrayList<>());

    private static OpLogger _flushes = new OpLogger("/cassandra_data/flush_time_log.csv");
    private static OpLogger _compactions = new OpLogger("/cassandra_data/compaction_time_log.csv");

    private static final int LOG_CAPACITY = 1024;
    private static final int WRITE_PERIOD_SECONDS = 15;

    private List<StartStop> log = null;
    private OutputStream w = null;

    public OpLogger(String path) {
        ArrayList<StartStop> l = new ArrayList<>();
        l.ensureCapacity(LOG_CAPACITY);
        log = l;
        try {
            w = Files.newOutputStream(Paths.get(path));
            w.write("# log file format is below\n# start,duration\n".getBytes());
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
                try {
                    Thread.sleep(WRITE_PERIOD_SECONDS * 1000);
                }
                catch (InterruptedException $) {}
                for (OpLogger l : loggers) {
                    l.flush();
                }
            });
        }
    }

    public static OpLogger flushes() { return _flushes; }
    public static OpLogger compactions() { return _compactions; }

    public void record(Instant start, Instant stop) {
        StartStop ss = new StartStop(start, stop);
        synchronized (this) {
            if (log.size() == LOG_CAPACITY) {
                flush();
            }
            log.add(ss);
        }
    }

    public void flush() {
        try {
            synchronized (this)
            {
                for (StartStop ss : log)
                {
                    String out = ss.startMicros() + "," + ss.durationMicros() + "\n";
                    w.write(out.getBytes());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(24);
        }
    }

    private static final class StartStop {
        public final Instant start;
        public final Instant stop;

        public StartStop(Instant start, Instant stop) {
            this.start = start;
            this.stop = stop;
        }

        public long startMicros() {
             return start.getEpochSecond() * 1_000_000 + start.getNano() / 1000;
        }

        public long durationMicros() {
            return Duration.between(start, stop).getNano() / 1000;
        }
    }
}
