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

package org.apache.cassandra.cache;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A pausable cache is used to dump the contents of the cache once paused.
 */
public final class PausableCache<K, V> implements ICache<K, V>
{
    ICache<K, V> cache;
    ReentrantReadWriteLock mu = new ReentrantReadWriteLock();

    private PausableCache(ICache<K, V> cache) {
        this.cache = cache;
    }

    public static <K, V> PausableCache<K, V> create(ICache<K, V> cache) {
        return new PausableCache(cache);
    }

    public long capacity() {
        return this.cache.capacity();
    }

    public void setCapacity(long cap) {
        this.mu.readLock().lock();
        try {
            this.cache.setCapacity(cap);
        } finally {
            this.mu.readLock().unlock();
        }
    }

    public void put(K k, V v) {
        this.mu.readLock().lock();
        try {
            this.cache.put(k, v);
        } finally {
            this.mu.readLock().unlock();
        }
    }

    public boolean putIfAbsent(K k, V v) {
        this.mu.readLock().lock();
        boolean ret = false;
        try {
            ret = this.cache.putIfAbsent(k, v);
        } finally {
            this.mu.readLock().unlock();
        }
        return ret;
    }

    public boolean replace(K k, V vold, V vnew) {
        this.mu.readLock().lock();
        boolean ret = false;
        try {
            ret = this.cache.replace(k, vold, vnew);
        } finally {
            this.mu.readLock().unlock();
        }
        return ret;
    }

    public V get(K k) {
        return this.cache.get(k);
    }

    public void remove(K k) {
        this.mu.readLock().lock();
        try {
            this.cache.remove(k);
        } finally {
            this.mu.readLock().unlock();
        }
    }

    public int size() { return this.cache.size(); }
    public long weightedSize() { return this.cache.weightedSize(); }

    public void clear() {
        this.mu.readLock().lock();
        try {
            this.cache.clear();
        } finally {
            this.mu.readLock().unlock();
        }
    }

    public Iterator<K> keyIterator() { return this.cache.keyIterator(); }
    public Iterator<K> hotKeyIterator(int n) { return this.cache.hotKeyIterator(n); }
    public boolean containsKey(K k) { return this.cache.containsKey(k); }

    public void wlock() { this.mu.writeLock().lock(); }
    public void wunlock() { this.mu.writeLock().unlock(); }
}
