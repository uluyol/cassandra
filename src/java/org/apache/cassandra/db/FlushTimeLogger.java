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

import java.io.FileWriter;
import java.io.IOException;

public class FlushTimeLogger {
    private static FlushTimeLogger instance = new FlushTimeLogger("/cassandra_data/flush_time_log.csv");

    private FileWriter w = null;

    public FlushTimeLogger(String path) {
        try {
            w = new FileWriter(path);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(23);
        }
    }

    public static FlushTimeLogger getInstance() {
        return instance;
    }

    public void flushStart() {
        long ms = System.currentTimeMillis();
        try {
            synchronized (this) {
                w.write("start," + ms);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void flushStop() {
        long ms = System.currentTimeMillis();
        try {
            synchronized (this) {
                w.write("stop," + ms);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
