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
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.crypto.Data;

import com.google.common.collect.ImmutableList;

import org.apache.log4j.Logger;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageIn;

/*

Hists contain histograms used in our measurements to identify tail latency.

 */
public final class Hists
{
    private static final Logger logger = Logger.getLogger(Hists.class);

    // keep these on top so that they initialize first
    // global list of HistRecorders that should be flushed periodically
    private static List<HistRecorder> recorders = Collections.synchronizedList(new ArrayList<HistRecorder>());
    private static Thread flusher = null; // thread to flush above

    // Hists for reads and writes
    public static final Hists reads = must("reads", Paths.get(DatabaseDescriptor.getHistDir(), "reads"));
    public static final Hists writes = must("writes", Paths.get(DatabaseDescriptor.getHistDir(), "writes"));

    private static final long WRITE_PERIOD_SECONDS = DatabaseDescriptor.getHistWritePeriod();

    public static final Instant epoch = Instant.now(NanoClock.instance);
    public static final AtomicLong flushStart = new AtomicLong(-1);
    public static final AtomicLong flushEnd = new AtomicLong(-1);
    public static final AtomicLong compactionStart = new AtomicLong(-1);
    public static final AtomicLong compactionEnd = new AtomicLong(-1);

    public static long nowMicros() {
        Duration d = Duration.between(epoch, Instant.now(NanoClock.instance));
        return (d.getSeconds() * 1_000_000) + ((long)d.getNano() / 1000);
    }

    public static long toMicros(Instant t) {
        Duration d = Duration.between(epoch, t);
        return (d.getSeconds() * 1_000_000) + ((long)d.getNano() / 1000);
    }

    private static final AtomicBoolean setIfEqLock = new AtomicBoolean(false);

    public static void setIfEq(AtomicLong dest, long destVal, AtomicLong cond, long condVal) {
        while (!setIfEqLock.compareAndSet(false, true)) {}
        if (cond.get() == condVal) {
            dest.set(destVal);
        }
        setIfEqLock.set(false);
    }

    // Per-Hists histograms
    private final HistRecorder overall;
    private final HistRecorder hasFlush;
    private final HistRecorder hasCompaction;

    private Hists(String name, Path destPath) throws IOException {
        Files.createDirectories(destPath);
        overall = HistRecorder.at(name + "/overall", destPath.resolve("overall_hist.log"));
        hasFlush = HistRecorder.at(name + "/hasflush", destPath.resolve("hasflush_hist.log"));
        hasCompaction = HistRecorder.at(name + "/hascompaction", destPath.resolve("hascompaction_hist.log"));

        // add our HistRecorders to a global list so that they
        // are periodically written to disk
        addRecorders(ImmutableList.of(overall, hasFlush, hasCompaction));
    }

    private synchronized static void addRecorders(Collection<HistRecorder> toAdd) {
        if (flusher == null) {
            assert recorders != null;
            flusher = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(WRITE_PERIOD_SECONDS * 1000);
                    } catch (InterruptedException $) {}
                    for (HistRecorder h : recorders) {
                        h.log();
                    }
                }
            });
            flusher.start();
        }

        recorders.addAll(toAdd);
    }

    private static Hists must(String name, Path destPath) {
        try {
            return new Hists(name, destPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void measure(MessageIn.MessageMeta meta, Instant endTime) {
        long tt = Duration.between(meta.getStart(), endTime).toNanos() / 1000;
        overall.recorder.recordValue(tt);

        if (hasOverlap(meta.getStart(), endTime, flushStart, flushEnd)) {
            hasFlush.recorder.recordValue(tt);
        }
        if (hasOverlap(meta.getStart(), endTime, compactionStart, compactionEnd)) {
            hasCompaction.recorder.recordValue(tt);
        }
    }

    private static boolean hasOverlap(Instant msgStart, Instant msgEnd, AtomicLong start, AtomicLong end) {
        long startMicros = start.get();
        long endMicros = end.get();
        return toMicros(msgStart) < endMicros || (endMicros < startMicros && startMicros < toMicros(msgEnd));
    }

    public static boolean overlapCompaction(Instant start, Instant end) { return hasOverlap(start, end, compactionStart, compactionEnd); }
    public static boolean overlapFlush(Instant start, Instant end) { return hasOverlap(start, end, flushStart, flushEnd); }

    private static final class HistRecorder {
        final Recorder recorder; // recorder stores actual latency histogram, is thread-safe
        final HistogramLogWriter writer;
        final OutputStream fw;
        final PrintStream ps;
        final String name;

        private Instant lastLog = Instant.now(NanoClock.instance);

        private Histogram recycleHist = null;

        private HistRecorder(String name, Path destPath) throws IOException {
            this.name = name;
            recorder = new Recorder(3);
            fw = Files.newOutputStream(destPath);
            ps = new PrintStream(fw);
            writer = new HistogramLogWriter(ps);
            writer.outputLogFormatVersion();
            long now = System.currentTimeMillis();
            writer.outputStartTime(now);
            writer.setBaseTime(now);
            writer.outputLegend();
            ps.flush();
            fw.flush();
        }

        public static HistRecorder at(String name, Path destPath) throws IOException { return new HistRecorder(name, destPath); }

        // log writes an interval histogram to disk. It is the caller's responsibility
        // to make sure that log is not called concurrently.
        public void log() {
            Histogram hist = recorder.getIntervalHistogram(recycleHist);
            Instant now = Instant.now(NanoClock.instance);
            Duration diff = Duration.between(lastLog, now);
            lastLog = now;
            logger.info(String.format(
                "%s: last %d seconds: %d recorded p50: %d p95: %d p99: %d us", name, diff.getSeconds(), hist.getTotalCount(),
                hist.getValueAtPercentile(50),
                hist.getValueAtPercentile(95),
                hist.getValueAtPercentile(99)));
            writer.outputIntervalHistogram(hist);
            try {
                ps.flush();
                fw.flush();
            } catch (IOException e) {
                // ignore failed flushes
            }
            recycleHist = hist;
        }
    }
}
