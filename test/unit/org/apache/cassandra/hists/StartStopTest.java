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

import java.time.Instant;

import org.junit.Test;

import junit.framework.Assert;

public class StartStopTest {
    @Test
    public void testDurationMicros() {
        StartEndDuration[] tests =
        {
            StartEndDuration.of(Instant.ofEpochSecond(100), Instant.ofEpochSecond(100, 1000), 1),
            StartEndDuration.of(Instant.ofEpochSecond(100), Instant.ofEpochSecond(100, 1000_000L), 1000),
            StartEndDuration.of(Instant.ofEpochSecond(100), Instant.ofEpochSecond(101), 1000_000L),
            StartEndDuration.of(Instant.ofEpochSecond(352), Instant.ofEpochSecond(358), 6000_000L),
        };
        for (int i = 0; i < tests.length; i++) {
            long got = new OpLogger.RecVal(tests[i].start, tests[i].stop, null).val;
            Assert.assertEquals(String.format("case %d: want %d got %d", i, tests[i].duration, got), tests[i].duration, got);
        }
    }

    private static final class StartEndDuration {
        final Instant start;
        final Instant stop;
        final long duration;

        private StartEndDuration(Instant start, Instant stop, long durationMicros) {
            this.start = start;
            this.stop = stop;
            this.duration = durationMicros;
        }

        public static StartEndDuration of(Instant start, Instant stop, long durationMicros) {
            return new StartEndDuration(start, stop, durationMicros);
        }
    }
}
