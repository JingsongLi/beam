package org.apache.beam.sdk.external;

import java.io.Serializable;

public interface ExternalKvStoreFactory<K, V> extends Serializable {
  ExternalKvStore<K, V> createExternalKvStore();
}
