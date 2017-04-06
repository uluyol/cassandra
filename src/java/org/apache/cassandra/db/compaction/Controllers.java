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

import java.util.Arrays;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.apache.log4j.Logger;

public final class Controllers
{
    public static Percentile newPercentile(Controller c, double pct, int winSize, double highFudgeFactor) {
        return new Percentile(c, pct, winSize, highFudgeFactor);
    }

    public static AIMD newAIMD(double stepSize, double remainFrac, double refOut, double minInput, double maxInput, double initInput) {
        return new AIMD(stepSize, remainFrac, refOut, minInput, maxInput, initInput);
    }

    /*
    Percentile computes the percentile based on a sliding window
    of observations before passing a Record() call to the
    underlying simenv.Controller.

    WindowSize must be sufficiently large to compute the requested
    percentile accurately.
    */
    public static final class Percentile implements Controller {
        private static final Logger logger = Logger.getLogger(Percentile.class);

        private final Controller actual;
        private double percentile;
        private final int windowSize;
        private final double highFudge;

        private double[] winBuf;
        private int winLen;
        private int winPos;

        private double[] scratch;
        private int numHigh;
        private boolean prevEarlyBadFire;

        Percentile(Controller c, double pct, int winSize, double highFudgeFactor) {
            actual = c;
            percentile = pct;
            windowSize = winSize;
            highFudge = highFudgeFactor;
            numHigh = 0;
            prevEarlyBadFire = false;

            winInit();
        }

        @Override
        public double getInput() { return actual.getInput(); }
        @Override
        public void setReference(double refOut) {
            numHigh = 0;
            actual.setReference(refOut);
        }
        @Override
        public double getReference() { return actual.getReference(); }

        public void setPercentile(double pct) { percentile = pct; }

        private final void winInit() {
            if (winBuf == null) {
                winBuf = new double[windowSize];
                winLen = 0;
                winPos = 0;
            }
        }

        private final boolean winIsFull() { return winLen == winBuf.length; }

        private final void winPush(double v) {
            if (winIsFull()) {
                winBuf[winPos] = v;
                winPos = (winPos + 1) % winBuf.length;
                return;
            }
            winBuf[winPos] = v;
            assert winLen == winPos;
            winLen++;
            winPos = (winPos + 1) % winBuf.length;
        }

        private final void winClear() {
            winInit();
            winLen = 0;
            winPos = 0;
        }

        @Override
        public void record(double input, double output) {
            winInit();
            winPush(output);
            if (output > actual.getReference()) {
                numHigh++;
                if (((double)numHigh)/windowSize >= highFudge*(1-percentile)) {
                    logger.debug(String.format("got %d/%d >= %f: ref: %f out: %f",
                                              numHigh, windowSize, 1-percentile,
                                              actual.getReference(), output));
                    prevEarlyBadFire = true;
                    actual.record(input, output);
                    winClear();
                    numHigh = 0;
                }
            }
            if (!winIsFull()) {
                return;
            }

            if (scratch == null || scratch.length != winBuf.length) {
                scratch = new double[winBuf.length];
            }

            double[] vals = scratch;
            for (int i = 0; i < winBuf.length; i++) {
                vals[i] = winBuf[i];
            }

            prevEarlyBadFire = false;
            Arrays.sort(vals);

            // Following percentile calculation was adapted from
            // http://stackoverflow.com/a/2753343/4873134
            double k = (double)(vals.length-1) * percentile;
            double l = Math.floor(k);
            double r = Math.ceil(k);
            if (l == r) {
                actual.record(input, vals[(int)k]);
                return;
            }
            double d0 = vals[(int)l] * (r - k);
            double d1 = vals[(int)r] * (k - l);
            actual.record(input, d0+d1);
            winClear();
            numHigh = 0;
        }

        @Override
        public String getAux() {
            String aux = "ctrlWinSize=" + winLen + ",prevEarlyBadFire=" + prevEarlyBadFire;
            String actualAux = actual.getAux();
            if (actualAux != null && !actualAux.isEmpty()) {
                aux += ',' + actualAux;
            }
            return aux;
        }
    }

    /*
    AIMD is an additive increase, multiplicative decrease controller

    If the current output is below the reference, the input is increased by StepSize.
    If it exceeds the reference, it is multiplied by RemainFrac.
    Otherwise it is left constant.
    */
    public static final class AIMD implements Controller {
        private static final Logger logger = Logger.getLogger(AIMD.class);

        private final double stepSize; // Additive increase factor 0 < StepSize
        private final double remainFrac; // Multiplicative factor 0 < RemainFrac < 1

        private double refOut, curOut;
        private final double minInput, maxInput;
        private double curInput;

        AIMD(double stepSize, double remainFrac, double refOut, double minInput, double maxInput, double initInput) {
            this.stepSize = stepSize;
            this.remainFrac = remainFrac;
            this.curOut = refOut; // ensures correct value for first getInput()
            this.refOut = refOut;
            this.minInput = minInput;
            this.maxInput = maxInput;
            this.curInput = initInput;
        }

        @Override
        public void setReference(double v) { refOut = v; }
        @Override
        public double getReference() { return refOut; }
        @Override
        public void record(double input, double output) { curOut = output; curInput = input; }

        @Override
        public double getInput() {
            double input = curInput;

            if (curOut < refOut) {
                input = curInput + stepSize;
            } else if (curOut > refOut) {
                input = curInput * remainFrac;
            }

            if (input > maxInput) {
                return maxInput;
            }
            if (input < minInput) {
                return minInput;
            }
            return input;
        }

        @Override
        public String getAux() { return "ctrlAIMDOut=" + curOut + ",ctrlAIMDRef=" + refOut; }
    }

    /*
    UpdatingRegression estimates the output based on a linear model of previous observations,
    and sets the input to maximize the output without exceeding the reference.
     */
    /*
    public static final class UpdatingRegression implements Controller {
        private double refOut;
        private double curInput;

        private final double minInput, maxInput;
        private final ObservationRing obs;
        private final ImmutableList<Supplier<Double>> xSupp;
        private final Supplier<Double> ySupp;

        UpdatingRegression(double minInput, double maxInput, double refOut, double initInput, int winSize, ImmutableList<Supplier<Double>> xSuppliers, Supplier<Double> ySupplier) {
            this.minInput = minInput;
            this.maxInput = maxInput;
            this.curInput = initInput;
            this.refOut = refOut;

            obs = new ObservationRing(xSuppliers.size(), winSize);
            xSupp = xSuppliers;
            ySupp = ySupplier;
        }

        public void observeAndComputeInput() {
            obs.observe(xSupp, ySupp);

            placeHolderSoThisIsEventuallycompleted()
        }

        @Override
        public void setReference(double val) {
            refOut = val;
            observeAndComputeInput();
        }

        @Override
        public double getReference() { return refOut; }

        @Override
        public void record(double input, double output) {
            // think about triggering an update based on ouput
            // input is useless
        }

        @Override
        public double getInput() { return curInput; }

        @Override
        public String getAux() { return ""; }

        @FunctionalInterface
        private static interface ObservationConsumer {
            public void consume(double[][] x, double[] y);
        }

        private static final class ObservationRing {
            private final double[][] x;
            private final double[] y;
            private int pos;
            private int len;

            ObservationRing(int nobs, int winSize) {
                assert winSize > 0;
                x = new double[winSize][];
                for (int i = 0; i < winSize; i++) {
                    x[i] = new double[nobs];
                }
                y = new double[winSize];
                pos = 0;
                len = 0;
            }

            public void observe(ImmutableList<Supplier<Double>> xSupp, Supplier<Double> ySupp) {
                assert xSupp.size() == x[0].length;
                y[pos] = ySupp.get();
                for (int i = 0; i < x[0].length; i++) {
                    x[pos][i] = xSupp.get(i).get();
                }
                pos++;
                if (len < x.length) {
                    len++;
                } else {
                    pos %= x.length;
                }
            }

            public void consume(ObservationConsumer c) { c.consume(x, y); }

            public boolean isFull() { return len == x.length; }
        }
    }
    */
}
