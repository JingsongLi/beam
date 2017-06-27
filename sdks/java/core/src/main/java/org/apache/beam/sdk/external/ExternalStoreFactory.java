package org.apache.beam.sdk.external;

import java.io.Serializable;

public interface ExternalStoreFactory<K, V> extends Serializable {
  ExternalStore<K, V> createExternalStore();
}
