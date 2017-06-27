package org.apache.beam.sdk.external;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheExternalState<K, V> implements ExternalState<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(CacheExternalState.class);

  private final Duration expireAfterWrite;
  private final long maximumSize;
  private final ExternalStore<K, V> store;
  private transient LoadingCache<K, V> cache;

  private CacheExternalState(
      ExternalStore<K, V> store, Duration expireAfterWrite,
      long maximumSize) {
    this.store = store;
    this.expireAfterWrite = expireAfterWrite;
    this.maximumSize = maximumSize;
  }

  @Override
  public void setup() {
    store.setup();
    initCache();
  }

  private void initCache() {
    LOG.info("I am Guava cache! The expireAfterWrite is: "
        + expireAfterWrite + " , the maximumSize: " + maximumSize);
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
        .expireAfterWrite(expireAfterWrite.getMillis(), TimeUnit.MILLISECONDS)
        .maximumSize(maximumSize);
    cache = builder.build(new LruCacheLoader());
  }

  @Override
  public V get(K k) {
    try {
      return cache.get(k);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ImmutableMap<K, V> get(Iterable<? extends K> ks) {
    try {
      return cache.getAll(ks);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown() {
    store.teardown();
  }

  private class LruCacheLoader extends CacheLoader<K, V> {

    @Override
    public V load(@Nullable K k) throws Exception {
      return store.get(k);
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
      Map<K, V> result = new HashMap<>();
      Iterable<V> values = store.get(keys);
      Iterator<? extends K> keyIterator = keys.iterator();
      Iterator<V> valueIterator = values.iterator();
      while (keyIterator.hasNext()) {
        result.put(keyIterator.next(), valueIterator.next());
      }
      return result;
    }

  }

  public static class Factory<K, V> implements ExternalStateFactory<K, V> {

    private Duration expireAfterWrite;
    private long maximumSize;

    public Factory(Duration expireAfterWrite, long maximumSize) {
      this.expireAfterWrite = expireAfterWrite;
      this.maximumSize = maximumSize;
    }

    @Override
    public ExternalState<K, V> createExternalState(ExternalStore<K, V> externalStore) {
      return new CacheExternalState<>(externalStore, expireAfterWrite, maximumSize);
    }
  }

}
