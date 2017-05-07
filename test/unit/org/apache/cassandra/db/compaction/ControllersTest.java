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

package org.apache.cassandra.db.compaction;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class ControllersTest {
    @Test
    public void testAIMDStable() {
        Controller ctlr = Controllers.newAIMD(1, 0.5, 100, 3, 0,
                                              2, 20, 10);
        compareToInsts(ctlr, ImmutableList.of(
            Inst.record(200), Inst.check(5),
            Inst.record(50), Inst.check(6),
            Inst.record(200), Inst.check(5),
            Inst.record(50), Inst.check(6),
            Inst.record(99), Inst.check(7),
            Inst.record(100), Inst.check(7),
            Inst.record(11), Inst.check(8), Inst.checkAIMDFails(0),
            Inst.record(101), Inst.check(7),
            Inst.record(102), Inst.check(6), Inst.checkAIMDFails(2),
            Inst.record(100), Inst.check(6), Inst.checkAIMDFails(1),
            Inst.record(101), Inst.check(5), Inst.checkAIMDFails(2),
            Inst.record(990), Inst.check(2.5), Inst.checkAIMDFails(3)));
    }

    @Test
    public void testAIMDBoundary() {
        Controller ctlr = Controllers.newAIMD(2, 0.5, 100, 0, 0,
                                              3, 27, 10);
        compareToInsts(ctlr, ImmutableList.of(
            Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0),
            Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0),
            Inst.check(27),
            Inst.record(666), Inst.record(666), Inst.record(666), Inst.record(666), Inst.record(666), Inst.record(666),
            Inst.record(666), Inst.record(666), Inst.record(666), Inst.record(666), Inst.record(666), Inst.record(666),
            Inst.check(3),
            Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0),
            Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0),
            Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0),
            Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0), Inst.record(0),
            Inst.check(27)));
    }

    @Test
    public void testAIMDRepeatedSuccEqOrFail() {
        Controller ctlr = Controllers.newAIMD(1, 0.5, 100, 3, 0.1,
                                              2, 20, 10);
        compareToInsts(ctlr, ImmutableList.of(
            Inst.record(100), Inst.check(10),
            Inst.record(110), Inst.check(10),
            Inst.record(100), Inst.check(10),
            Inst.record(110), Inst.check(10),
            Inst.record(100), Inst.check(10),
            Inst.record(100), Inst.check(10),
            Inst.record(90), Inst.check(10),
            Inst.record(111), Inst.check(9),
            Inst.record(120), Inst.check(8),
            Inst.record(111), Inst.check(4)));

        ctlr = Controllers.newAIMD(1, 0.5, 100, 3, 0,
                                   2, 20, 10);
        compareToInsts(ctlr, ImmutableList.of(
            Inst.record(101), Inst.check(5),
            Inst.record(101), Inst.check(2.5),
            Inst.record(101), Inst.check(2),
            Inst.record(101), Inst.check(2),
            Inst.record(101), Inst.check(2),
            Inst.record(101), Inst.check(2),
            Inst.record(101), Inst.check(2),
            Inst.record(99), Inst.check(3),
            Inst.record(99), Inst.check(4),
            Inst.record(101), Inst.check(3)));
    }

    @Test
    public void testBangBang() {
        Controller ctlr = Controllers.newBangBang(885, 35, 77, 0);

        compareToInsts(ctlr, ImmutableList.of(
            Inst.record(100), Inst.check(0),
            Inst.record(885), Inst.check(0),
            Inst.record(885+77), Inst.check(0),
            Inst.record(885+78), Inst.check(Double.MAX_VALUE),
            Inst.record(885+77), Inst.check(Double.MAX_VALUE),
            Inst.record(885), Inst.check(Double.MAX_VALUE),
            Inst.record(885-35), Inst.check(Double.MAX_VALUE),
            Inst.record(885-36), Inst.check(0)));
    }

    @Test
    public void testProportional() {
        Controller ctlr = Controllers.newProportional(10, 0.5, 2, 20, 0, 4, 20);

        compareToInsts(ctlr, ImmutableList.of(
            Inst.record(10), Inst.check(20),
            Inst.record(12), Inst.check(20),
            Inst.record(8), Inst.check(19),
            Inst.record(8), Inst.check(18),
            Inst.record(6), Inst.check(16),
            Inst.record(16), Inst.check(19),
            Inst.record(2), Inst.check(15),
            Inst.record(0), Inst.check(11),
            Inst.record(11), Inst.check(11.5)));

        ctlr = Controllers.newProportional(10, 0.5, 2, 20, 2, 100, 20);

        compareToInsts(ctlr, ImmutableList.of(
            Inst.record(10), Inst.check(18),
            Inst.record(12), Inst.check(20),
            Inst.record(8), Inst.check(18),
            Inst.record(8), Inst.check(16),
            Inst.record(6), Inst.check(14),
            Inst.record(16), Inst.check(17),
            Inst.record(0), Inst.check(12)));
    }

    private void compareToInsts(Controller ctlr, ImmutableList<Inst> insts) {
        for (int i = 0; i < insts.size(); i++) {
            Inst inst = insts.get(i);
            if (inst.kind == Inst.Kind.RECORD) {
                ctlr.record(ctlr.getInput(), inst.output);
            } else if (inst.kind == Inst.Kind.CHECK_INPUT) {
                double input = ctlr.getInput();
                Assert.assertEquals("inst " + i + ": want same input: aux: " + ctlr.getAux(), inst.input, input, 0.001);
            } else if (inst.kind == Inst.Kind.CHECK_FAILS) {
                Controllers.AIMD aimd = (Controllers.AIMD)ctlr;
                Assert.assertEquals("inst " + i + ": want same fails: aux: " + ctlr.getAux(), inst.fails, aimd.getFails());
            } else {
                throw new RuntimeException("invalid instruction");
            }
        }
    }

    private static class Inst {
        public final Kind kind;
        public final double input;
        public final double output;
        public final int fails;

        private Inst(Kind k, double i, double o, int f) { kind = k; input = i; output = o; fails = f; }

        public static Inst record(double obs) { return new Inst(Kind.RECORD, 0, obs, 0); }
        public static Inst check(double in) { return new Inst(Kind.CHECK_INPUT, in, 0, 0); }
        public static Inst checkAIMDFails(int fails) { return new Inst(Kind.CHECK_FAILS, 0, 0, fails); }

        public static enum Kind { RECORD, CHECK_INPUT, CHECK_FAILS }
    }
}
