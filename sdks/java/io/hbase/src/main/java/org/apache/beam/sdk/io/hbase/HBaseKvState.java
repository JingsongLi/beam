package org.apache.beam.sdk.io.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.external.ExternalKvState;
import org.apache.beam.sdk.external.ExternalKvStateFactory;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseKvState<K, V> implements ExternalKvState<K, V> {

  private SerializableConfiguration serializableConfiguration;
  private Coder<K> keyCoder;
  private SerializableFunction<Result, V> resultFn;
  private String tableId;
  private Set<String> familys;

  private transient Connection connection;
  private transient Table table;

  private HBaseKvState(
      Coder<K> keyCoder, SerializableFunction<Result, V> resultFn,
      String tableId, Set<String> familys, SerializableConfiguration conf) {
    this.keyCoder = keyCoder;
    this.resultFn = resultFn;
    this.tableId = tableId;
    this.familys = familys;
    this.serializableConfiguration = conf;
  }

  @Override
  public void setup() {
    try {
      Configuration configuration = serializableConfiguration.get();
      connection = ConnectionFactory.createConnection(configuration);
      TableName tableName = TableName.valueOf(tableId);
      table = connection.getTable(tableName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public V get(K k) {
    try {
      Result result = table.get(newGet(k));
      return resultFn.apply(result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<V> get(Iterable<? extends K> keys) {
    List<Get> gets = new ArrayList<>();
    for (K k : keys) {
      gets.add(newGet(k));
    }
    try {
      Result[] results = table.get(gets);
      List<V> result = new ArrayList<>();
      for (Result r : results) {
        result.add(resultFn.apply(r));
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<Map.Entry<K, V>> scan() {

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    for (String family : familys) {
      scan.addFamily(Bytes.toBytes(family));
    }

    try (ResultScanner scanner = table.getScanner(scan)){
      Map<K, V> result = new HashMap<>();
      for (Result r : scanner) {
        K k = CoderUtils.decodeFromByteArray(keyCoder, r.getRow());
        result.put(k, resultFn.apply(r));
      }
      return result.entrySet();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Get newGet(K k) {
    try {
      Get get = new Get(CoderUtils.encodeToByteArray(keyCoder, k));
      get.setCacheBlocks(false);
      for (String family : familys) {
        get.addFamily(Bytes.toBytes(family));
      }
      return get;
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
  }

  public static class Factory<K, V> implements ExternalKvStateFactory<K, V> {

    private SerializableConfiguration conf;
    private Coder<K> keyCoder;
    private SerializableFunction<Result, V> resultFn;
    private String tableId;
    private Set<String> familys;

    public static Factory<String, Map<String, String>>
    ofString(String tableId, SerializableConfiguration conf) {
      return new Factory<>(
          StringUtf8Coder.of(), new SerializableFunction<Result, Map<String, String>>() {
        @Override
        public Map<String, String> apply(Result input) {
          Map<String, String> result = new HashMap<>();
          List<Cell> cells = input.listCells();
          if (cells == null) {
            return result;
          }
          for (Cell cell : cells) {
            String subKey = Bytes.toString(cell.getQualifierArray());
            String value = Bytes.toString(cell.getValueArray());
            result.put(subKey, value);
          }
          return result;
        }
      }, tableId, new HashSet<String>(), conf);
    }

    public static <K, V> Factory<K, V> of(
        Coder<K> keyCoder, SerializableFunction<Result, V> resultFn,
        String tableId, SerializableConfiguration conf) {
      return new Factory<>(
          keyCoder, resultFn, tableId, new HashSet<String>(), conf);
    }

    private Factory(
        Coder<K> keyCoder, SerializableFunction<Result, V> resultFn,
        String tableId, Set<String> familys, SerializableConfiguration conf) {
      this.keyCoder = keyCoder;
      this.resultFn = resultFn;
      this.tableId = tableId;
      this.familys = familys;
      this.conf = conf;
    }

    public Factory<K, V> addFamily(String family) {
      familys.add(family);
      return this;
    }

    @Override
    public ExternalKvState<K, V> createExternalKvState() {
      return new HBaseKvState<>(keyCoder, resultFn, tableId, familys, conf);
    }
  }

}
