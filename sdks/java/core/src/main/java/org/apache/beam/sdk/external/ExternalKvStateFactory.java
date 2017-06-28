package org.apache.beam.sdk.external;

import java.io.Serializable;

public interface ExternalKvStateFactory<K, V> extends Serializable {
  ExternalKvState<K, V> createExternalKvState();
}
