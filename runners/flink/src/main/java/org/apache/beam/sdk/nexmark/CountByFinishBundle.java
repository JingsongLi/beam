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
package org.apache.beam.sdk.nexmark;

import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;

/**
 * CountByFinishBundle.
 */
public class CountByFinishBundle extends DoFn<KV<Long, Event>, KV<Long, KV<Long, Event>>> {
  @StateId("count")
  private final StateSpec<ValueState<Long>> ignore =
      StateSpecs.value(VarLongCoder.of());
  private Map<Long, KV<Long, Event>> cache = new HashMap<>();
  @ProcessElement
  public void processElement(ProcessContext c,
                             @StateId("count") ValueState<Long> state) {
    long key = c.element().getValue().newAuction.seller;
    Long count = MoreObjects.firstNonNull(state.read(), 0L);
    Long update = count + 1;
    state.write(update);
    cache.put(key, KV.of(update, c.element().getValue()));
  }
  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    for (Map.Entry<Long, KV<Long, Event>> entry : cache.entrySet()) {
      c.output(KV.of(entry.getKey(), entry.getValue()),
          GlobalWindow.TIMESTAMP_MAX_VALUE, GlobalWindow.INSTANCE);
    }
    cache.clear();
  }
}
