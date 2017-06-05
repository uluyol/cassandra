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
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.Pair;

public class ReplicaSetWeightMapTest {

    @BeforeClass
    public static void defineSchema() {
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("ReplicaSetWeightMapTest");
    }

    static Comparator<ImmutableList<InetAddress>> rsComparator = new Comparator<ImmutableList<InetAddress>>() {
        public int compare(ImmutableList<InetAddress> o1, ImmutableList<InetAddress> o2) {
            // sort by size, then elements
            int cmp = Integer.compare(o1.size(), o2.size());
            if (cmp != 0) {
                return cmp;
            }
            for (int i=0; i < o1.size(); i++) {
                cmp = o1.get(i).getHostAddress().compareTo(o2.get(i).getHostAddress());
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    };

    @Test
    public void testCorrectReplicaSets() throws UnknownHostException {
        TokenMetadata tmd = new TokenMetadata();
        TokenGen tg = new TokenGen();
        AddrGen ag = new AddrGen();

        tmd.updateNormalTokens(tg.nextN(5), ag.next());
        tmd.updateNormalTokens(tg.nextN(200), ag.next());
        tmd.updateNormalTokens(tg.nextN(100), ag.next());
        tmd.updateNormalTokens(tg.nextN(50), ag.next());
        tmd.updateNormalTokens(tg.nextN(31), ag.next());
        tmd.updateNormalTokens(tg.nextN(73), ag.next());

        NavigableSet<ImmutableList<InetAddress>> replicaSets = new TreeSet<>(rsComparator);

        for (String ksName : Schema.instance.getKeyspaces()) {
            Keyspace ks = Schema.instance.getKeyspaceInstance(ksName);
            Multimap<Range<Token>, InetAddress> rangeAddrs = ks.getReplicationStrategy().getRangeAddresses(tmd);
            for (Range<Token> key : rangeAddrs.keySet()) {
                List<InetAddress> p = Lists.newArrayList();
                for (InetAddress a : rangeAddrs.get(key)) {
                    p.add(a);
                }
                Collections.sort(p, Ordering.usingToString());
                replicaSets.add(ImmutableList.copyOf(p));
            }
        }

        ReplicaSetWeightMap.WeightMap wmap = new ReplicaSetWeightMap.WeightMap(100);
        WeightGen wgen = new WeightGen();
        for (ImmutableList<InetAddress> p : replicaSets) {
            wmap.put(p, wgen.make(p));
        }

        Random rand = new Random(9999);
        TokenGen queryGen = new TokenGen(99977);
        ImmutableList<String> keyspaces = ImmutableList.copyOf(Schema.instance.getKeyspaces());
        for (int i = 0; i < 5000; i++) {
            Keyspace ks = Schema.instance.getKeyspaceInstance(keyspaces.get(rand.nextInt(keyspaces.size())));
            List<InetAddress> endpoints = ks.getReplicationStrategy().calculateNaturalEndpoints(queryGen.next(), tmd);
            float[] weights = wmap.get(endpoints);
            Assert.assertNotNull("missing weights", weights);
        }
    }

    private static final class TokenGen {
        private final Random rand;

        TokenGen() { rand = new Random(0); }

        TokenGen(long seed) { rand = new Random(seed); }

        public Token next() { return new Murmur3Partitioner.LongToken(rand.nextLong()); }

        public Collection<Token> nextN(int n) {
            List<Token> tokens = Lists.newArrayList();
            for (int i = 0; i < n; i++) {
                tokens.add(next());
            }
            return tokens;
        }
    }

    private static final class WeightGen {
        private final Random rand = new Random(0);

        public float[] make(List<InetAddress> addrs) {
            float[] weights = new float[addrs.size()];
            float sumWeight = 0;
            for (int i = 0; i < weights.length; i++) {
                float w = rand.nextFloat();
                weights[i] = w;
                sumWeight += w;
            }
            for (int i = 0; i < weights.length; i++) {
                weights[i] /= sumWeight;
            }
            return weights;
        }
    }

    @Test
    public void testHashOfDiffOrder() throws UnknownHostException {
        ReplicaSetWeightMap.PWList pl1 = new ReplicaSetWeightMap.PWList();
        pl1.add(InetAddress.getByName("10.0.0.123"), 0.1f);
        pl1.add(InetAddress.getByName("10.0.0.124"), 0.5f);
        pl1.add(InetAddress.getByName("10.0.2.124"), 0.4f);
        ReplicaSetWeightMap.PWList pl2 = new ReplicaSetWeightMap.PWList();
        pl2.add(InetAddress.getByName("10.0.0.123"), 0.1f);
        pl2.add(InetAddress.getByName("10.0.2.124"), 0.4f);
        pl2.add(InetAddress.getByName("10.0.0.124"), 0.5f);
        pl2.add(InetAddress.getByName("10.0.9.9"), 0.5f);
        pl2.add(InetAddress.getByName("10.0.9.10"), 0.5f);
        pl2.add(InetAddress.getByName("10.0.9.92"), 0.5f);
        pl2.clear();
        pl2.add(InetAddress.getByName("10.0.0.123"), 0.1f);
        pl2.add(InetAddress.getByName("10.0.2.124"), 0.4f);
        pl2.add(InetAddress.getByName("10.0.0.124"), 0.5f);
        List<InetAddress> al1 = ImmutableList.of(InetAddress.getByName("10.0.0.124"),
                                                 InetAddress.getByName("10.0.2.124"),
                                                 InetAddress.getByName("10.0.0.123"));
        List<InetAddress> al2 = ImmutableList.of(InetAddress.getByName("10.0.2.124"),
                                                 InetAddress.getByName("10.0.0.123"),
                                                 InetAddress.getByName("10.0.0.124"));
        assertHashesEqual(pl1, pl2, al2, al1);
        pl2.add(InetAddress.getByName("10.0.0.12"), 0f);
        Assert.assertFalse("got same hash codes for different lists",
                           ReplicaSetWeightMap.WeightMap.hashOf(pl2) == ReplicaSetWeightMap.WeightMap.hashOf(al1));
    }

    @Test
    public void testPWListList() throws UnknownHostException {
        List<InetAddress> a1 = ImmutableList.of(InetAddress.getByName("10.0.0.1"),
                                                InetAddress.getByName("10.0.0.2"),
                                                InetAddress.getByName("192.168.1.3"));
        List<Float> w1 = ImmutableList.of(0.3f, 0.2f, 0.5f);
        Pair<List<InetAddress>, List<Float>> p1 = Pair.create(a1, w1);
        List<InetAddress> a2 = ImmutableList.of(InetAddress.getByName("100.10.10.1"),
                                                InetAddress.getByName("212.22.21.12"),
                                                InetAddress.getByName("11.11.11.11"),
                                                InetAddress.getByName("google.com"),
                                                InetAddress.getByName("facebook.com"),
                                                InetAddress.getByName("apache.org"),
                                                InetAddress.getByName("twitter.com"),
                                                InetAddress.getByName("microsoft.com"),
                                                InetAddress.getByName("apple.com"),
                                                InetAddress.getByName("kernel.org"),
                                                InetAddress.getByName("gnu.org"),
                                                InetAddress.getByName("umich.edu"));
        List<Float> w2 = ImmutableList.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.1f, 0.05f, 0.05f, 0.05f, 0.05f);
        assert a2.size() == w2.size();
        Pair<List<InetAddress>, List<Float>> p2 = Pair.create(a2, w2);

        List<List<Pair<List<InetAddress>, List<Float>>>> tests = ImmutableList.of(
            ImmutableList.of(p1, p2, p2, p1, p2, p2, p1),
            ImmutableList.of(p2, p2, p2),
            ImmutableList.of(),
            ImmutableList.of(p1, p1));
        for (int i = 0; i < tests.size(); i++) {
            assertPWListListEq(makeListPWList(tests.get(i)), makePWListList(tests.get(i)));
        }
    }

    @Test
    public void testWeightMap() throws UnknownHostException {
        AddrGen g = new AddrGen();
        List<Pair<List<InetAddress>, List<Float>>> rsWeights = ImmutableList.of(
            pwPairFrom(g, ImmutableList.of(0.5f, 0.4f, 0.1f)),
            pwPairFrom(g, ImmutableList.of(0.7f, 0.3f)),
            pwPairFrom(g, ImmutableList.of(0.1f, 0.1f, 0.1f, 0.7f)),
            pwPairFrom(g, ImmutableList.of(0.05f, 0.3f, 0.3f, 0.3f, 0.35f)),
            pwPairFrom(g, ImmutableList.of(0.1f, 0.1f, 0.1f, 0.1f, 0.5f, 0.1f)),
            pwPairFrom(g, ImmutableList.of(0.9f, 0.1f)),
            pwPairFrom(g, ImmutableList.of(1f)),
            pwPairFrom(g, ImmutableList.of()),
            pwPairFrom(g, ImmutableList.of(0.4f, 0.4f, 0.2f)),
            pwPairFrom(g, ImmutableList.of(0.6f, 0.2f, 0.2f)));
        ReplicaSetWeightMap.WeightMap wmap = new ReplicaSetWeightMap.WeightMap(3);
        for (int i=0; i < rsWeights.size(); i++) {
            Assert.assertEquals(wmap.actualSize(), i);
            wmap.put(rsWeights.get(i).left, rsWeights.get(i).right);
            assertSameWeights(rsWeights.get(i).right, wmap.get(rsWeights.get(i).left));
        }

        for (int i=0; i < rsWeights.size(); i++) {
            assertSameWeights(rsWeights.get(i).right, wmap.get(rsWeights.get(i).left));
        }
    }

    private static void assertSameWeights(List<Float> want, float[] have) {
        Assert.assertNotNull("got null weights", have);
        Assert.assertEquals("incorrect number of weights", want.size(), have.length);
        for (int i=0; i < have.length; i++) {
            Assert.assertEquals("differing weight", (float)want.get(i), have[i], 0);
        }
    }

    private static Pair<List<InetAddress>, List<Float>> pwPairFrom(AddrGen g, List<Float> weights)
    throws UnknownHostException {
        List<InetAddress> addrs = Lists.newArrayList();
        for (int i = 0; i < weights.size(); i++) {
            addrs.add(g.next());
        }
        return Pair.create(addrs, weights);
    }

    private static final class AddrGen {
        private int n = 0;
        private static final String base = "10.0.";

        public InetAddress next() throws UnknownHostException {
            n++;
            assert n <= 0xFFFF;
            int top = n & 0xFF00 >> 8;
            int bot = n & 0xFF;
            String name = base + top + "." + bot;
            return InetAddress.getByName(name);
        }
    }

    private static void assertPWListListEq(List<ReplicaSetWeightMap.PWList> lpwl, ReplicaSetWeightMap.PWListList pwll) {
        Assert.assertSame(lpwl.size(), pwll.size());
        for (int i = 0; i < lpwl.size(); i++) {
            assertPWListEq(lpwl.get(i), pwll.get(i));
        }
    }

    private static List<ReplicaSetWeightMap.PWList> makeListPWList(List<Pair<List<InetAddress>, List<Float>>> data) {
        List<ReplicaSetWeightMap.PWList> l = Lists.newArrayList();
        for (Pair<List<InetAddress>, List<Float>> p : data) {
            l.add(populate(new ReplicaSetWeightMap.PWList(), p.left, p.right));
        }
        return l;
    }

    private static ReplicaSetWeightMap.PWListList makePWListList(List<Pair<List<InetAddress>, List<Float>>> data) {
        ReplicaSetWeightMap.PWListList l = new ReplicaSetWeightMap.PWListList();
        int toAdd = new Random(data.size()).nextInt(300);
        for (int i = 0; i < toAdd; i++) {
            l.add();
        }
        l.clear();
        for (Pair<List<InetAddress>, List<Float>> p : data) {
            populate(l.add(), p.left, p.right);
        }
        return l;
    }

    private static ReplicaSetWeightMap.PWList populate(ReplicaSetWeightMap.PWList pws,
                                                       List<InetAddress> addrs,
                                                       List<Float> weights) {
        assert addrs.size() == weights.size();
        for (int i = 0; i < addrs.size(); i++) {
            pws.add(addrs.get(i), weights.get(i));
        }
        return pws;
    }

    private static void assertPWListEq(ReplicaSetWeightMap.PWList p1, ReplicaSetWeightMap.PWList p2) {
        Assert.assertSame(p1.size(), p2.size());
        for (int i = 0; i < p1.size(); i++) {
            Assert.assertTrue(p1.getWeight(i) == p2.getWeight(i));
            Assert.assertSame(p1.getAddr(i), p2.getAddr(i));
        }
    }

    private static void assertHashesEqual(Object... args) {
        assert args.length > 1;
        List<List<InetAddress>> addrLists = Lists.newArrayList();
        List<ReplicaSetWeightMap.PWList> pwLists = Lists.newArrayList();

        for (Object a : args) {
            if (a instanceof ReplicaSetWeightMap.PWList) {
                pwLists.add((ReplicaSetWeightMap.PWList)a);
            } else if (a instanceof List) {
                addrLists.add((List<InetAddress>)a);
            } else {
                throw new InvalidParameterException();
            }
        }

        List<Integer> hashCodes = Lists.newArrayList();
        for (List<InetAddress> al : addrLists) {
            hashCodes.add(ReplicaSetWeightMap.WeightMap.hashOf(al));
        }
        for (ReplicaSetWeightMap.PWList pwl : pwLists) {
            hashCodes.add(ReplicaSetWeightMap.WeightMap.hashOf(pwl));
        }
        for (int i = 1; i < hashCodes.size(); i++) {
            Assert.assertTrue(String.format("mismatching hash codes for %d: %d %d", i, hashCodes.get(0), hashCodes.get(i)), hashCodes.get(0).equals(hashCodes.get(i)));
        }
    }
}
