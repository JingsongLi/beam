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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

/**
 * CountByTimer.
 */
public class CountByTimer extends DoFn<KV<Long, Event>, KV<Long, KV<Long, Event>>> {
  @StateId("count")
  private final StateSpec<ValueState<KV<Long, Event>>> ignore =
      StateSpecs.value(KvCoder.of(VarLongCoder.of(), Event.CODER));
  @StateId("key")
  private final StateSpec<ValueState<Long>> ignore2 =
      StateSpecs.value(VarLongCoder.of());
  @TimerId("timer")
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
  @ProcessElement
  public void processElement(ProcessContext c,
                             @StateId("count") ValueState<KV<Long, Event>> state,
                             @StateId("key") ValueState<Long> key,
                             @TimerId("timer") Timer timer) {
    timer.align(Duration.millis(500)).setRelative();
    key.write(c.element().getKey());
    Long count = MoreObjects.firstNonNull(state.read(), KV.of(0L, null)).getKey();
    Long update = count + 1;
    state.write(KV.of(update, c.element().getValue()));
  }
  @OnTimer("timer")
  public void onTimer(OnTimerContext c,
                      @StateId("count") ValueState<KV<Long, Event>> state,
                      @StateId("key") ValueState<Long> key) {
    c.output(KV.of(key.read(), state.read()));
  }
}
