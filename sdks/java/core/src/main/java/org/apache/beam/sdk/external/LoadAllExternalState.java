package org.apache.beam.sdk.external;

import com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;

public class LoadAllExternalState<K, V> implements ExternalState<K, V> {

  private final ExternalStore<K, V> store;
  private Duration expireAfterWrite;

  private transient ImmutableMap<K, V> allElements;
  private transient Long lastLoadTime;

  private LoadAllExternalState(ExternalStore<K, V> store, Duration expireAfterWrite) {
    this.store = store;
    this.expireAfterWrite = expireAfterWrite;
  }

  @Override
  public void setup() {
    store.setup();
    scan();
    lastLoadTime = System.currentTimeMillis();
  }

  private void checkReload() {

    // check if not need reload
    if (expireAfterWrite.getMillis() != Long.MAX_VALUE) {
      long now = System.currentTimeMillis();

      if (now - lastLoadTime > expireAfterWrite.getMillis()) {
        scan();
        lastLoadTime = now;
      }
    }
  }

  private void scan() {
    allElements = ImmutableMap.<K, V>builder().putAll(store.scan()).build();
  }

  @Override
  public V get(K k) {
    checkReload();
    return allElements.get(k);
  }

  @Override
  public ImmutableMap<K, V> get(Iterable<? extends K> ks) {
    checkReload();
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    for (K k : ks) {
      V v = allElements.get(k);
      if (v != null) {
        builder.put(k, v);
      }
    }
    return builder.build();
  }

  @Override
  public void teardown() {
    store.teardown();
  }

  public static class Factory<K, V> implements ExternalStateFactory<K, V> {

    private Duration expireAfterWrite;

    public Factory(Duration expireAfterWrite) {
      this.expireAfterWrite = expireAfterWrite;
    }

    @Override
    public ExternalState<K, V> createExternalState(ExternalStore<K, V> externalStore) {
      return new LoadAllExternalState<>(externalStore, expireAfterWrite);
    }
  }
}
