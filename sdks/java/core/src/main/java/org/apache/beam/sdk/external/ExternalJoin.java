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
package org.apache.beam.sdk.external;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

@Experimental
public class ExternalJoin<K, InputT, JoinT> extends
    PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, KV<InputT, JoinT>>>> {

  private ExternalStateFactory<K, JoinT> stateFactory;
  private ExternalStoreFactory<K, JoinT> storeFactory;
  private int batchGetCount;

  private ExternalJoin(
      ExternalStateFactory<K, JoinT> stateFactory,
      ExternalStoreFactory<K, JoinT> storeFactory,
      int batchGetCount) {
    this.stateFactory = stateFactory;
    this.storeFactory = storeFactory;
    this.batchGetCount = batchGetCount;
  }

  public static <K, InputT, JoinT> ExternalJoin<K, InputT, JoinT>
  by(ExternalStateFactory<K, JoinT> stateFactory,
     ExternalStoreFactory<K, JoinT> storeFactory,
     int batchGetCount) {
    return new ExternalJoin<>(stateFactory, storeFactory, batchGetCount);
  }

  @Override
  public PCollection<KV<K, KV<InputT, JoinT>>> expand(PCollection<KV<K, InputT>> input) {
    return input.apply(ParDo.of(new DoFn<KV<K, InputT>, KV<K, KV<InputT, JoinT>>>() {

      private transient List<WindowedValue<KV<K, InputT>>> elements;
      private transient Set<K> keys;
      private transient ExternalState<K, JoinT> externalState;
      @Setup
      public void setup() {
        elements = new ArrayList<>();
        keys = new HashSet<>();
        externalState = stateFactory.createExternalState(storeFactory.createExternalStore());
        externalState.setup();
      }

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) {
        elements.add(WindowedValue.of(c.element(), c.timestamp(), window, PaneInfo.NO_FIRING));
        keys.add(c.element().getKey());
        if (keys.size() >= batchGetCount) {
          join(new ProcessOutput(c));
        }
      }

      private void join(Output output) {
        Map<K, JoinT> dimValues = externalState.get(keys);
        for (WindowedValue<KV<K, InputT>> element : elements) {
          KV<K, InputT> input = element.getValue();
          JoinT join = dimValues.get(input.getKey());
          output.output(WindowedValue.of(
              KV.of(input.getKey(), KV.of(input.getValue(), join)),
              element.getTimestamp(),
              element.getWindows(),
              element.getPane()));
        }
        elements.clear();
        keys.clear();
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c) throws Exception {
        join(new FinishBundleOutput(c));
      }

      @Teardown
      public void teardown() {
        externalState.teardown();
      }

      @Override
      public Duration getAllowedTimestampSkew() {
        return Duration.millis(Long.MAX_VALUE);
      }

      abstract class Output {
        abstract void output(WindowedValue<KV<K, KV<InputT, JoinT>>> value);
      }

      class ProcessOutput extends Output {
        private ProcessContext c;
        ProcessOutput(ProcessContext c) {
          this.c = c;
        }
        @Override
        void output(WindowedValue<KV<K, KV<InputT, JoinT>>> value) {
          c.outputWithTimestamp(value.getValue(), value.getTimestamp());
        }
      }

      class FinishBundleOutput extends Output {
        private FinishBundleContext c;
        FinishBundleOutput(FinishBundleContext c) {
          this.c = c;
        }
        @Override
        void output(WindowedValue<KV<K, KV<InputT, JoinT>>> value) {
          c.output(value.getValue(), value.getTimestamp(),
              Iterables.getOnlyElement(value.getWindows()));
        }
      }
    }));
  }
}
