package org.apache.beam.sdk.external;

import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuralByteArray;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.CoderUtils;

public class ExternalRow {

  public static final Coder<StructuralByteArray> BYTE_ARRAY_CODER = new StructuralByteArrayCoder();
  public static final Coder<ExternalRow> CODER = new ExternalRowCoder();

  private Map<StructuralByteArray, StructuralByteArray> columns;

  public ExternalRow(Map<StructuralByteArray, StructuralByteArray> columns) {
    this.columns = columns;
  }

  public String byString(String column) throws CoderException {
    return get(column, StringUtf8Coder.of(), StringUtf8Coder.of());
  }

  public <K, V> V get(K column, Coder<K> kCoder, Coder<V> vCoder) throws CoderException {
    return CoderUtils.decodeFromByteArray(vCoder,
        columns.get(new StructuralByteArray(
            CoderUtils.encodeToByteArray(kCoder, column))).getValue());
  }

  public Map<StructuralByteArray, StructuralByteArray> getColumns() {
    return columns;
  }

  public static class StructuralByteArrayCoder extends StructuredCoder<StructuralByteArray> {

    @Override
    public void encode(StructuralByteArray value, OutputStream outStream) throws CoderException, IOException {
      ByteArrayCoder.of().encode(value.getValue(), outStream);
    }

    @Override
    public StructuralByteArray decode(InputStream inStream) throws CoderException, IOException {
      return new StructuralByteArray(ByteArrayCoder.of().decode(inStream));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }

  public static class ExternalRowCoder extends StructuredCoder<ExternalRow> {

    static final Coder<Map<StructuralByteArray, StructuralByteArray>> MAP_CODER
        = MapCoder.of(BYTE_ARRAY_CODER, BYTE_ARRAY_CODER);

    @Override
    public void encode(ExternalRow value, OutputStream outStream) throws CoderException, IOException {
      MAP_CODER.encode(value.getColumns(), outStream);
    }

    @Override
    public ExternalRow decode(InputStream inStream) throws CoderException, IOException {
      return new ExternalRow(MAP_CODER.decode(inStream));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }
}
