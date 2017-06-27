package org.apache.beam.sdk.external;

import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * Simple and Abstract.
 */
@Experimental
public interface ExternalState<K, V> {

  void setup();

  V get(K k);

  Map<K, V> get(Iterable<? extends K> keys);

  void teardown();

}
