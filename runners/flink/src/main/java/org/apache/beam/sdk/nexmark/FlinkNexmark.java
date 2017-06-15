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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.joda.time.Duration;

/**
 * For test, count by seller, and output the latest event.
 */
public class FlinkNexmark implements Serializable {

  public static void main(String[] args) throws InterruptedException, IOException {

    FlinkNexmarkOptions options =
        PipelineOptionsFactory.fromArgs(args).as(FlinkNexmarkOptions.class);
    options.setStreaming(true);
    // let bundleByTime
    options.setMaxBundleSize(Long.MAX_VALUE);
    options.setMaxBundleTime(500L);
    options.setNumEvents(Long.MAX_VALUE);
    AbstractStateBackend backend =
        options.getUseRocks() ?
            new RocksDBStateBackend(options.getCpPath()) : new FsStateBackend(options.getCpPath());
    options.setStateBackend(backend);

    if (options.getParallelism() == 1) {
      options.setParallelism(10);
    }
    if (options.getCheckpointingInterval() == -1) {
      options.setCheckpointingInterval(Duration.standardMinutes(2).getMillis());
    }
    options.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    NexmarkConfiguration configuration =
        Iterables.getOnlyElement(options.getSuite().getConfigurations(options));

    Pipeline p = Pipeline.create(options);

    DoFn<KV<Long, Event>, KV<Long, KV<Long, Event>>> countDoFn =
        options.getUseBundle() ? new CountByFinishBundle() : new CountByTimer();
    p
        .apply("NexmarkReadUnbounded", NexmarkUtils.streamEventsSource(configuration))
        .apply(ParDo.of(new DoFn<Event, KV<Long, Event>>() {
          List<KV<Long, Event>> cache = new ArrayList<>();
          @ProcessElement
          public void processElement(ProcessContext c) {
            Event event = c.element();
            if (event.newAuction != null) {
              long seller = event.newAuction.seller;
              cache.add(KV.of(seller, event));
            }
          }
          @FinishBundle
          public void finishBundle(FinishBundleContext c) {
            for (KV<Long, Event> entry : cache) {
              c.output(KV.of(entry.getKey(), entry.getValue()),
                  GlobalWindow.TIMESTAMP_MAX_VALUE, GlobalWindow.INSTANCE);
            }
            cache.clear();
          }
        }))
        .apply(ParDo.of(countDoFn))
        .apply("PrintParDo", ParDo.of(new DoFn<KV<Long, KV<Long, Event>>, Void>() {
          // For force hash to show metrics in UI
          @StateId("ignore")
          private final StateSpec<ValueState<Long>> ignore =
              StateSpecs.value(VarLongCoder.of());
          @ProcessElement
          public void process(ProcessContext c, @StateId("ignore") ValueState<Long> state) {
          }
        }));

    p.run().waitUntilFinish();
    Thread.sleep(Duration.standardDays(1).getMillis());
  }

  interface FlinkNexmarkOptions extends NexmarkOptions, FlinkPipelineOptions {
    @Default.Boolean(true)
    boolean getUseRocks();
    void setUseRocks(boolean useRocks);

    @Default.Boolean(true)
    boolean getUseBundle();
    void setUseBundle(boolean useBundle);

    String getCpPath();
    void setCpPath(String cpPath);
  }

}
