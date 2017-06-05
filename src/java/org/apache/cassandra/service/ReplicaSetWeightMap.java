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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.compactlb.Coordination;

public final class ReplicaSetWeightMap {

    public final static ReplicaSetWeightMap current = new ReplicaSetWeightMap();

    private final Object weightsWriteLock = new Object();
    private volatile WeightMap weights;
    private volatile WeightMap shadowWeights;

    private ReplicaSetWeightMap() {
        weights = new WeightMap(100);
        shadowWeights = new WeightMap(100);
    }

    public void update(List<Coordination.ReplicaSetWeights> rsWeights) throws UnknownHostException {
        List<InetAddress> addrs = Lists.newArrayListWithExpectedSize(5);
        try {
            synchronized (weightsWriteLock) {
                WeightMap sw = shadowWeights;
                Lock wlock = sw.lock.writeLock();
                wlock.lock();
                try {
                    sw.clear();
                    for (Coordination.ReplicaSetWeights pws : rsWeights) {
                        addrs.clear();
                        for (int i = 0; i < pws.getHostIpsCount(); i++) {
                            addrs.add(InetAddress.getByName(pws.getHostIps(i)));
                        }
                        sw.put(addrs, pws.getWeightsList());
                    }
                } finally {
                    wlock.unlock();
                }
                WeightMap w = weights;
                weights = sw;
                shadowWeights = w;
            }
        } catch (UnknownHostException e) {
            throw e;
        }
    }

    public float[] weightsFor(List<InetAddress> replicas) {
        WeightMap map = weights;
        Lock rlock = map.lock.readLock();
        rlock.lock();
        try {
            return map.get(replicas);
        } finally {
            rlock.unlock();
        }
    }

    static final class WeightMap {
        private final static float maxLoadFactor = 0.6f;
        private PWListList[] buckets;
        private int load;

        final ReadWriteLock lock = new ReentrantReadWriteLock();

        WeightMap(int initialBuckets) {
            buckets = new PWListList[initialBuckets];
            initBuckets(buckets);
            load = 0;
        }

        private void grow() {
            float loadFactor = (float)(load+1) / (float)buckets.length;
            if (loadFactor >= maxLoadFactor) {
                int newSize = Math.max(buckets.length*2, buckets.length+1);
                PWListList[] newBuckets = new PWListList[newSize];
                initBuckets(newBuckets);
                copy(newBuckets, buckets);
                buckets = newBuckets;
            }
        }

        void clear() {
            load = 0;
            for (int i = 0; i < buckets.length; i++) {
                buckets[i].clear();
            }
        }

        // put adds the kv-pair. The addrs ands weights lists are copied so the
        // originals are safe for reuse.
        void put(List<InetAddress> addrs, List<Float> weights) {
            assert addrs.size() == weights.size();
            grow();
            PWList pws = bucketOf(buckets, addrs).add();
            load++;
            for (int i = 0; i < addrs.size(); i++) {
                pws.add(addrs.get(i), weights.get(i));
            }
        }

        // put adds the kv-pair. The addrs ands weights lists are copied so the
        // originals are safe for reuse.
        void put(List<InetAddress> addrs, float[] weights) {
            assert addrs.size() == weights.length;
            grow();
            PWList pws = bucketOf(buckets, addrs).add();
            load++;
            for (int i = 0; i < addrs.size(); i++) {
                pws.add(addrs.get(i), weights[i]);
            }
        }

        float[] get(List<InetAddress> replicas) {
            float[] weights = new float[replicas.size()];
            PWListList bucket = bucketOf(buckets, replicas);
            for (int i = 0; i < bucket.size(); i++) {
                PWList pws = bucket.get(i);
                if (pws.size() != replicas.size()) {
                    continue;
                }
                boolean bad = false;
                for (int j = 0; j < replicas.size(); j++) {
                    float w = pws.lookup(replicas.get(j));
                    if (w < 0) {
                        bad = true;
                        break;
                    }
                    weights[j] = w;
                }
                if (!bad) {
                    return weights;
                }
            }
            return null;
        }

        // actualSize compute the actual number of entries in the map.
        // This is O(n) and is meant for use in tests.
        int actualSize() {
            int n = 0;
            for (PWListList b : buckets) {
                n += b.size();
            }
            return n;
        }

        static PWListList bucketOf(PWListList[] buckets, List<InetAddress> addrs) {
            int pos = (hashOf(addrs) & 0x7FFFFFFF) % buckets.length;
            return buckets[pos];
        }

        static PWListList bucketOf(PWListList[] buckets, PWList addrs) {
            int pos = (hashOf(addrs) & 0x7FFFFFFF) % buckets.length;
            return buckets[pos];
        }

        static int hashOf(List<InetAddress> addrs) {
            Hasher hasher = Hashing.sipHash24().newHasher().putInt(addrs.size());
            int addrHash = 0;
            for (int i = 0; i < addrs.size(); i++) {
                addrHash ^= Arrays.hashCode(addrs.get(i).getAddress());
            }
            return hasher.putInt(addrHash).hash().asInt();
        }

        static int hashOf(PWList l) {
            Hasher hasher = Hashing.sipHash24().newHasher().putInt(l.len);
            int addrHash = 0;
            for (int i = 0; i < l.len; i++) {
                addrHash ^= Arrays.hashCode(l.addrs[i].getAddress());
            }
            return hasher.putInt(addrHash).hash().asInt();
        }

        private static void initBuckets(PWListList[] arr) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = new PWListList();
            }
        }

        private static void copy(PWListList[] dst, PWListList[] src) {
            for (int i = 0; i < src.length; i++) {
                for (int j = 0; j < src[i].size(); j++) {
                    bucketOf(dst, src[i].get(j)).add(src[i].get(j));
                }
            }
        }
    }

    static final class PWListList {
        private PWList[] lists;
        private int len;

        PWListList() {
            lists = new PWList[3];
            len = 0;
        }

        void clear() {
            len = 0;
            for (int i = 0; i < lists.length && lists[i] != null; i++) {
                lists[i].clear();
            }
        }

        private void grow() {
            if (len == lists.length) {
                int newCap = Math.max(lists.length*2, lists.length+1);
                PWList[] newLists = new PWList[newCap];
                for (int i = 0; i < len; i++) {
                    newLists[i] = lists[i];
                }
                lists = newLists;
            }
        }

        void add(PWList l) {
            grow();
            lists[len] = l;
            len++;
        }

        PWList add() {
            grow();
            if (lists[len] == null) {
                lists[len] = new PWList();
            }
            lists[len].clear();
            len++;
            return lists[len-1];
        }

        int size() {
            return len;
        }

        PWList get(int i) {
            assert i < len && i >= 0;
            return lists[i];
        }
    }

    static final class PWList {
        InetAddress[] addrs;
        float[] weights;
        int len;

        PWList() {
            addrs = new InetAddress[5];
            weights = new float[5];
            len = 0;
        }

        void clear() {
            // free addrs, weights are primitives so OK to leave junk
            for (int i = 0; i < addrs.length; i++) {
                addrs[i] = null;
            }
            len = 0;
        }

        private void grow() {
            if (len == addrs.length) {
                int newCap = Math.max(addrs.length*2, addrs.length+1);
                InetAddress[] newAddrs = new InetAddress[newCap];
                float[] newWeights = new float[newCap];
                for (int i = 0; i < len; i++) {
                    newAddrs[i] = addrs[i];
                    newWeights[i] = weights[i];
                }
                addrs = newAddrs;
                weights = newWeights;
            }
        }

        void add(InetAddress addr, float weight) {
            grow();
            addrs[len] = addr;
            weights[len] = weight;
            len++;
        }

        // lookup performs a linear search. Should be the fastest option for small arrays.
        float lookup(InetAddress addr) {
            for (int i = 0; i < len; i++) {
                if (addr.equals(addrs[i])) {
                    return weights[i];
                }
            }
            return -1f;
        }

        InetAddress getAddr(int i) { return addrs[i]; }
        float getWeight(int i) { return weights[i]; }

        int size() { return len; }
    }
}
