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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
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

  private Duration expireAfterWrite;
  private long maximumSize;
  private int batchGetCount;
  private ExternalKvStoreFactory<K, JoinT> storeFactory;

  private ExternalJoin(
      Duration expireAfterWrite,
      long maximumSize,
      int batchGetCount,
      ExternalKvStoreFactory<K, JoinT> storeFactory) {
    this.expireAfterWrite = expireAfterWrite;
    this.maximumSize = maximumSize;
    this.batchGetCount = batchGetCount;
    this.storeFactory = storeFactory;
  }

  public static <K, InputT, JoinT> ExternalJoin<K, InputT, JoinT>
  by(Duration expireAfterWrite,
     long maximumSize,
     int batchGetCount,
     ExternalKvStoreFactory<K, JoinT> storeFactory) {
    return new ExternalJoin<>(expireAfterWrite, maximumSize, batchGetCount, storeFactory);
  }

  @Override
  public PCollection<KV<K, KV<InputT, JoinT>>> expand(PCollection<KV<K, InputT>> input) {
    return input.apply(ParDo.of(new ExternalJoinDoFn()));
  }

  private class ExternalJoinDoFn extends DoFn<KV<K, InputT>, KV<K, KV<InputT, JoinT>>> {

    private transient List<WindowedValue<KV<K, InputT>>> elements;
    private transient Set<K> keys;
    private transient ExternalKvStore<K, JoinT> kvState;
    private transient LoadingCache<K, JoinT> cache;

    @Setup
    public void setup() {
      elements = new ArrayList<>();
      keys = new HashSet<>();
      cache = CacheBuilder.newBuilder()
          .expireAfterWrite(expireAfterWrite.getMillis(), TimeUnit.MILLISECONDS)
          .maximumSize(maximumSize)
          .build(new ExternalLoader());
      kvState = storeFactory.createExternalKvStore();
      kvState.setup();
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
      Map<K, JoinT> dimValues;
      try {
        dimValues = cache.getAll(keys);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
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
      kvState.teardown();
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

    class ExternalLoader extends CacheLoader<K, JoinT> {

      @Override
      public JoinT load(@Nullable K k) throws Exception {
        return kvState.get(k);
      }

      @Override
      public Map<K, JoinT> loadAll(Iterable<? extends K> keys) throws Exception {
        Map<K, JoinT> result = new HashMap<>();
        Iterable<JoinT> values = kvState.get(keys);
        Iterator<? extends K> keyIterator = keys.iterator();
        Iterator<JoinT> valueIterator = values.iterator();
        while (keyIterator.hasNext()) {
          result.put(keyIterator.next(), valueIterator.next());
        }
        return result;
      }
    }
  }

}
