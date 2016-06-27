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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import com.google.protobuf.ByteString;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.PausableCache;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.service.CacheService;
import org.apache.jasper.tagplugins.jstl.core.Out;

/**
 * Dumps the contents of the cache once a message
 * is recieved on the named pipe.
 */
public final class CacheDumper
{
    public static final CacheDumper instance = new CacheDumper("/logs/dumpcache.fifo", "/cassandra_data/dumped_cache");
    //public static final CacheDumper instance = new CacheDumper("/tmp/dumpcache.fifo", "/tmp/dumped_cache");

    private Thread t = null;

    private CacheDumper(String inPath, String outPath) {
        InputStream s = null;
        OutputStream out = null;
        try
        {
            Process rmp = Runtime.getRuntime().exec(new String[]{"rm", "-rf", inPath});
            rmp.waitFor();
            Process p = Runtime.getRuntime().exec(new String[]{"mkfifo", inPath});
            int ret = p.waitFor();
            if (ret != 0) {
                throw new RuntimeException("unable to create fifo");
            }
            out = Files.newOutputStream(Paths.get(outPath));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        t = new Thread(new WaitThread(inPath, out));
        t.start();
    }

    private static class WaitThread implements Runnable {
        private final OutputStream out;
        private final String inPath;
        WaitThread(String inPath, OutputStream out) { this.inPath = inPath; this.out = out; }

        public void run() {
            boolean again = true;
            while (again) {
                try {
                    InputStream s = Files.newInputStream(Paths.get(inPath));
                    // wait for message
                    s.read();
                    again = false;
                } catch (IOException $) {
                    again = true;
                }
            }
            BufferedOutputStream bufOut = new BufferedOutputStream(out);
            PausableCache<KeyCacheKey, ?> cache = CacheService.instance.keyCacheCache;
            cache.wlock();
            try {
                Iterator<KeyCacheKey> iter = cache.keyIterator();
                byte lenBuf[] = new byte[4];
                while (iter.hasNext()) {
                    KeyCacheKey k = iter.next();
                    CacheEntryOuterClass.CacheEntry entry = CacheEntryOuterClass.CacheEntry.newBuilder()
                        .setColumnFamily(k.desc.cfname)
                        .setKeyspace(k.desc.ksname)
                        .setKey(ByteString.copyFrom(k.key))
                        .build();
                    entry.writeDelimitedTo(bufOut);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                cache.wunlock();
            }
            try {
                bufOut.flush();
                bufOut.close();
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
