package org.apache.beam.sdk.external;

import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
public interface ExternalStore<K, V> {

  void setup();

  V get(K k);

  Iterable<V> get(Iterable<? extends K> keys);

  Iterable<Map.Entry<K, V>> scan();

  void teardown();

}
