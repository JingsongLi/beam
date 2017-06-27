package org.apache.beam.sdk.external;

import java.io.Serializable;

public interface ExternalStateFactory<K, V> extends Serializable {
  ExternalState<K, V> createExternalState(ExternalStore<K, V> externalStore);
}
