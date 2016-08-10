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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * Clock based on System.nanoTime(). Will overflow at unix epoch + 290 years.
 */
public class NanoClock extends Clock {
    private final static long EPOCH_NANOS = System.currentTimeMillis() * 1000_000;
    private final static long NANOS_START = System.nanoTime();

    public final static NanoClock instance = new NanoClock(java.time.ZoneOffset.UTC);

    private ZoneId z;

    private NanoClock(ZoneId z) { this.z = z; }

    @Override
    public ZoneId getZone() { return z; }

    @Override
    public Clock withZone(ZoneId z) { return new NanoClock(z); }

    @Override
    public Instant instant() {
        return nanoToInstant(System.nanoTime());
    }

    public Instant nanoToInstant(long nano) {
        long n = nano - NANOS_START + EPOCH_NANOS;
        return Instant.ofEpochSecond(n / 1000_000_000, n % 1000_000_000);
    }
}
