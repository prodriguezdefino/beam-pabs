/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Mutation;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteSchemaTransformProvider;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;

/**
 * Cloud Bigtable benchmark in a Java pipeline: {@code bt_to_gcs} and {@code gcs_to_bt}.
 *
 * <p>Readers ({@code --reader}, {@code bt_to_gcs}):
 *
 * <ul>
 *   <li>{@code java-native}: {@code BigtableIO.read()} in the Java pipeline environment.
 *   <li>{@code java-schema}: {@code BigtableReadSchemaTransformProvider} in the Java pipeline
 *       environment.
 *   <li>{@code rust}: the Rust {@code BigtableReadProvider} through the Rust expansion service.
 * </ul>
 *
 * <p>Writers ({@code --writer}, {@code gcs_to_bt}):
 *
 * <ul>
 *   <li>{@code java-native}: {@code BigtableIO.write()} in the Java pipeline environment.
 *   <li>{@code java-schema}: {@code BigtableWriteSchemaTransformProvider} in the Java pipeline
 *       environment.
 *   <li>{@code rust}: the Rust {@code BigtableWriteProvider} through the Rust expansion service.
 * </ul>
 */
public class BigtableBenchmark {
  static final String RUST_READ_URN = "beam:schematransform:org.apache.beam:rust:bigtable_read:v1";
  static final String RUST_WRITE_URN =
      "beam:schematransform:org.apache.beam:rust:bigtable_write:v1";
  static final String SCHEMA_TRANSFORM_URN = "beam:expansion:payload:schematransform:v1";

  static final Schema BATCHED_MUTATIONS_SCHEMA =
      Schema.builder()
          .addByteArrayField("key")
          .addArrayField(
              "mutations",
              Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.BYTES))
          .build();

  /** Benchmark flags. Dataflow flags come from {@link DataflowPipelineOptions}. */
  public interface Options extends DataflowPipelineOptions {
    @Description("Reader (bt_to_gcs): java-native, java-schema or rust.")
    String getReader();

    void setReader(String value);

    @Description("Writer (gcs_to_bt): java-native, java-schema or rust.")
    String getWriter();

    void setWriter(String value);

    @Description("Shape: bt_to_gcs or gcs_to_bt.")
    @Default.String("bt_to_gcs")
    String getShape();

    void setShape(String value);

    @Description("Google Cloud project ID containing the Bigtable instance.")
    String getBigtableProject();

    void setBigtableProject(String value);

    @Description("Cloud Bigtable instance ID.")
    String getBigtableInstance();

    void setBigtableInstance(String value);

    @Description("Cloud Bigtable table ID.")
    String getBigtableTable();

    void setBigtableTable(String value);

    @Description("Optional Bigtable app profile ID (supported by java-native and rust).")
    @Default.String("")
    String getAppProfile();

    void setAppProfile(String value);

    @Description("Emit one element per column qualifier (true) or per Bigtable row (false).")
    @Default.Boolean(false)
    boolean getFlatten();

    void setFlatten(boolean value);

    @Description("Cloud Storage input file pattern for gcs_to_bt.")
    String getInput();

    void setInput(String value);

    @Description("Cloud Storage output prefix for bt_to_gcs.")
    String getOutput();

    void setOutput(String value);

    @Description("Address of the Rust expansion service.")
    @Default.String("localhost:8097")
    String getExpansionService();

    void setExpansionService(String value);
  }

  /** Encodes a {@code SchemaTransformPayload}, as Python's {@code SchemaAwareExternalTransform}. */
  static byte[] schemaTransformPayload(String urn, Schema configSchema, Row config)
      throws IOException {
    ByteArrayOutputStream row = new ByteArrayOutputStream();
    RowCoder.of(configSchema).encode(config, row);
    return SchemaTransformPayload.newBuilder()
        .setIdentifier(urn)
        .setConfigurationSchema(SchemaTranslation.schemaToProto(configSchema, true))
        .setConfigurationRow(ByteString.copyFrom(row.toByteArray()))
        .build()
        .toByteArray();
  }

  static PCollection<Row> readJavaNative(Pipeline p, Options options) {
    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstance())
            .withTableId(options.getBigtableTable());
    if (!options.getAppProfile().isEmpty()) {
      read = read.withAppProfileId(options.getAppProfile());
    }
    boolean flatten = options.getFlatten();
    Schema outputSchema =
        flatten
            ? BigtableReadSchemaTransformProvider.FLATTENED_ROW_SCHEMA
            : BigtableReadSchemaTransformProvider.ROW_SCHEMA;
    return p.apply("ReadBigtableRows", read)
        .apply("ConvertToBeamRows", ParDo.of(new ToBeamRowFn(flatten)))
        .setRowSchema(outputSchema);
  }

  /**
   * Converts each {@link com.google.bigtable.v2.Row} into Beam {@link Row}s of {@link
   * BigtableReadSchemaTransformProvider#FLATTENED_ROW_SCHEMA} or {@link
   * BigtableReadSchemaTransformProvider#ROW_SCHEMA}, matching {@link
   * BigtableReadSchemaTransformProvider} and the Rust connector.
   */
  static class ToBeamRowFn extends DoFn<com.google.bigtable.v2.Row, Row> {
    private final boolean flatten;

    ToBeamRowFn(boolean flatten) {
      this.flatten = flatten;
    }

    private static List<Row> convertCells(List<Cell> bigtableCells) {
      List<Row> beamCells = new ArrayList<>(bigtableCells.size());
      for (Cell cell : bigtableCells) {
        Row cellRow =
            Row.withSchema(BigtableReadSchemaTransformProvider.CELL_SCHEMA)
                .addValues(cell.getValue().toByteArray(), cell.getTimestampMicros())
                .build();
        beamCells.add(cellRow);
      }
      return beamCells;
    }

    @ProcessElement
    public void process(@Element com.google.bigtable.v2.Row bigtableRow, OutputReceiver<Row> out) {
      byte[] key = bigtableRow.getKey().toByteArray();
      if (flatten) {
        for (Family fam : bigtableRow.getFamiliesList()) {
          String familyName = fam.getName();
          for (Column col : fam.getColumnsList()) {
            List<Row> cells = convertCells(col.getCellsList());
            Row flattenedRow =
                Row.withSchema(BigtableReadSchemaTransformProvider.FLATTENED_ROW_SCHEMA)
                    .addValues(key, familyName, col.getQualifier().toByteArray(), cells)
                    .build();
            out.output(flattenedRow);
          }
        }
      } else {
        Map<String, Map<String, List<Row>>> families =
            new LinkedHashMap<>(bigtableRow.getFamiliesCount());
        for (Family fam : bigtableRow.getFamiliesList()) {
          Map<String, List<Row>> columns = new LinkedHashMap<>(fam.getColumnsCount());
          for (Column col : fam.getColumnsList()) {
            columns.put(col.getQualifier().toStringUtf8(), convertCells(col.getCellsList()));
          }
          families.put(fam.getName(), columns);
        }
        Row beamRow =
            Row.withSchema(BigtableReadSchemaTransformProvider.ROW_SCHEMA)
                .addValues(key, families)
                .build();
        out.output(beamRow);
      }
    }
  }

  static PCollection<Row> readJavaSchema(Pipeline p, Options options) {
    if (!options.getAppProfile().isEmpty()) {
      throw new IllegalArgumentException(
          "--appProfile is not supported by java-schema: use java-native or rust");
    }
    BigtableReadSchemaTransformProvider provider = new BigtableReadSchemaTransformProvider();
    Row config =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("project_id", options.getBigtableProject())
            .withFieldValue("instance_id", options.getBigtableInstance())
            .withFieldValue("table_id", options.getBigtableTable())
            .withFieldValue("flatten", options.getFlatten())
            .build();
    return PCollectionRowTuple.empty(p).apply("Read", provider.from(config)).get("output");
  }

  static PCollection<Row> readRust(Pipeline p, Options options) throws IOException {
    Schema.Builder schema =
        Schema.builder()
            .addStringField("project_id")
            .addStringField("instance_id")
            .addStringField("table_id")
            .addBooleanField("flatten");
    List<Object> values = new ArrayList<>();
    values.add(options.getBigtableProject());
    values.add(options.getBigtableInstance());
    values.add(options.getBigtableTable());
    values.add(options.getFlatten());
    if (!options.getAppProfile().isEmpty()) {
      schema.addStringField("app_profile_id");
      values.add(options.getAppProfile());
    }
    Schema configSchema = schema.build();
    Row config = Row.withSchema(configSchema).addValues(values).build();
    byte[] payload = schemaTransformPayload(RUST_READ_URN, configSchema, config);
    PCollectionTuple outputs =
        p.apply(
            "Read",
            External.<PBegin, Row>of(SCHEMA_TRANSFORM_URN, payload, options.getExpansionService())
                .withMultiOutputs());
    @SuppressWarnings("unchecked")
    PCollection<Row> rows = (PCollection<Row>) outputs.get(new TupleTag<Row>("output"));
    return rows;
  }

  static String decodeUtf8(byte[] bytes) {
    return bytes == null ? "" : new String(bytes, StandardCharsets.UTF_8);
  }

  /** Counts {@code e2e/rows} and formats each Beam {@link Row} as text. */
  static class TouchRowFn extends DoFn<Row, String> {
    private final boolean flatten;
    private final Counter rows = Metrics.counter("e2e", "rows");

    TouchRowFn(boolean flatten) {
      this.flatten = flatten;
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<String> out) {
      rows.inc();
      String key = decodeUtf8(row.getBytes("key"));
      if (flatten) {
        String family = row.getString("family_name");
        String qualifier = decodeUtf8(row.getBytes("column_qualifier"));
        Collection<Row> cells = row.getArray("cells");
        String value = "";
        if (cells != null && !cells.isEmpty()) {
          Row first = cells.iterator().next();
          value = decodeUtf8(first.getBytes("value"));
        }
        out.output(key + "\t" + family + ":" + qualifier + "\t" + value);
      } else {
        StringBuilder sb = new StringBuilder(key.length() + 128);
        sb.append(key);
        Map<String, Map<String, Collection<Row>>> families = row.getMap("column_families");
        if (families != null) {
          for (Map.Entry<String, Map<String, Collection<Row>>> fam : families.entrySet()) {
            String familyName = fam.getKey();
            Map<String, Collection<Row>> cols = fam.getValue();
            if (cols != null) {
              for (Map.Entry<String, Collection<Row>> col : cols.entrySet()) {
                Collection<Row> cells = col.getValue();
                String value = "";
                if (cells != null && !cells.isEmpty()) {
                  Row first = cells.iterator().next();
                  value = decodeUtf8(first.getBytes("value"));
                }
                sb.append('\t')
                    .append(familyName)
                    .append(':')
                    .append(col.getKey())
                    .append('=')
                    .append(value);
              }
            }
          }
        }
        out.output(sb.toString());
      }
    }
  }

  static final class ParsedCell {
    final String family;
    final String qualifier;
    final String value;

    ParsedCell(String family, String qualifier, String value) {
      this.family = family;
      this.qualifier = qualifier;
      this.value = value;
    }
  }

  static List<ParsedCell> parseCells(String[] parts) {
    List<ParsedCell> cells = new ArrayList<>(parts.length - 1);
    if (parts.length == 3 && parts[1].indexOf('=') < 0) {
      int colon = parts[1].indexOf(':');
      if (colon > 0) {
        cells.add(
            new ParsedCell(
                parts[1].substring(0, colon), parts[1].substring(colon + 1), parts[2]));
      }
      return cells;
    }
    for (int i = 1; i < parts.length; i++) {
      int eq = parts[i].indexOf('=');
      if (eq <= 0) {
        continue;
      }
      String col = parts[i].substring(0, eq);
      String val = parts[i].substring(eq + 1);
      int colon = col.indexOf(':');
      if (colon <= 0) {
        continue;
      }
      cells.add(new ParsedCell(col.substring(0, colon), col.substring(colon + 1), val));
    }
    return cells;
  }

  /** Parses text lines into native Bigtable {@link Mutation}s and counts {@code e2e/rows}. */
  static class ToNativeMutationsFn
      extends DoFn<String, KV<com.google.protobuf.ByteString, Iterable<Mutation>>> {
    private final Counter rows = Metrics.counter("e2e", "rows");

    @ProcessElement
    public void process(
        @Element String line,
        OutputReceiver<KV<com.google.protobuf.ByteString, Iterable<Mutation>>> out) {
      if (line == null || line.isEmpty()) {
        return;
      }
      String[] parts = line.split("\t", -1);
      if (parts.length < 2 || parts[0].isEmpty()) {
        return;
      }
      List<ParsedCell> parsed = parseCells(parts);
      if (parsed.isEmpty()) {
        return;
      }
      List<Mutation> mutations = new ArrayList<>(parsed.size());
      for (ParsedCell cell : parsed) {
        mutations.add(
            Mutation.newBuilder()
                .setSetCell(
                    Mutation.SetCell.newBuilder()
                        .setFamilyName(cell.family)
                        .setColumnQualifier(
                            com.google.protobuf.ByteString.copyFromUtf8(cell.qualifier))
                        .setTimestampMicros(-1L)
                        .setValue(com.google.protobuf.ByteString.copyFromUtf8(cell.value)))
                .build());
      }
      rows.inc();
      out.output(KV.of(com.google.protobuf.ByteString.copyFromUtf8(parts[0]), mutations));
    }
  }

  /** Parses text lines into {@link #BATCHED_MUTATIONS_SCHEMA} rows and counts {@code e2e/rows}. */
  static class ToMutationRowFn extends DoFn<String, Row> {
    private static final byte[] SET_CELL_BYTES = "SetCell".getBytes(StandardCharsets.UTF_8);
    private final Counter rows = Metrics.counter("e2e", "rows");

    @ProcessElement
    public void process(@Element String line, OutputReceiver<Row> out) {
      if (line == null || line.isEmpty()) {
        return;
      }
      String[] parts = line.split("\t", -1);
      if (parts.length < 2 || parts[0].isEmpty()) {
        return;
      }
      List<ParsedCell> parsed = parseCells(parts);
      if (parsed.isEmpty()) {
        return;
      }
      List<Map<String, byte[]>> mutations = new ArrayList<>(parsed.size());
      for (ParsedCell cell : parsed) {
        Map<String, byte[]> map = new LinkedHashMap<>(4);
        map.put("type", SET_CELL_BYTES);
        map.put("family_name", cell.family.getBytes(StandardCharsets.UTF_8));
        map.put("column_qualifier", cell.qualifier.getBytes(StandardCharsets.UTF_8));
        map.put("value", cell.value.getBytes(StandardCharsets.UTF_8));
        mutations.add(map);
      }
      rows.inc();
      out.output(
          Row.withSchema(BATCHED_MUTATIONS_SCHEMA)
              .addValues(parts[0].getBytes(StandardCharsets.UTF_8), mutations)
              .build());
    }
  }

  static void writeJavaNative(PCollection<String> lines, Options options) {
    BigtableIO.Write write =
        BigtableIO.write()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstance())
            .withTableId(options.getBigtableTable());
    if (!options.getAppProfile().isEmpty()) {
      write = write.withAppProfileId(options.getAppProfile());
    }
    lines.apply("Touch", ParDo.of(new ToNativeMutationsFn())).apply("Write", write);
  }

  static void writeJavaSchema(PCollection<String> lines, Options options) {
    if (!options.getAppProfile().isEmpty()) {
      throw new IllegalArgumentException(
          "--appProfile is not supported by java-schema: use java-native or rust");
    }
    PCollection<Row> rows =
        lines
            .apply("Touch", ParDo.of(new ToMutationRowFn()))
            .setRowSchema(BATCHED_MUTATIONS_SCHEMA);
    BigtableWriteSchemaTransformProvider provider = new BigtableWriteSchemaTransformProvider();
    Row config =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("project_id", options.getBigtableProject())
            .withFieldValue("instance_id", options.getBigtableInstance())
            .withFieldValue("table_id", options.getBigtableTable())
            .build();
    PCollectionRowTuple.of("input", rows).apply("Write", provider.from(config));
  }

  static void writeRust(PCollection<String> lines, Options options) throws IOException {
    PCollection<Row> rows =
        lines
            .apply("Touch", ParDo.of(new ToMutationRowFn()))
            .setRowSchema(BATCHED_MUTATIONS_SCHEMA);
    Schema.Builder schema =
        Schema.builder()
            .addStringField("project_id")
            .addStringField("instance_id")
            .addStringField("table_id");
    List<Object> values = new ArrayList<>();
    values.add(options.getBigtableProject());
    values.add(options.getBigtableInstance());
    values.add(options.getBigtableTable());
    if (!options.getAppProfile().isEmpty()) {
      schema.addStringField("app_profile_id");
      values.add(options.getAppProfile());
    }
    Schema configSchema = schema.build();
    Row config = Row.withSchema(configSchema).addValues(values).build();
    byte[] payload = schemaTransformPayload(RUST_WRITE_URN, configSchema, config);
    PCollectionTuple.of(new TupleTag<Row>("input"), rows)
        .apply(
            "Write",
            External.<PCollectionTuple, Row>of(
                    SCHEMA_TRANSFORM_URN, payload, options.getExpansionService())
                .withMultiOutputs());
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    if ("bt_to_gcs".equals(options.getShape())) {
      if (options.getOutput() == null || options.getOutput().isEmpty()) {
        throw new IllegalArgumentException("bt_to_gcs needs --output");
      }
      boolean flatten = options.getFlatten();
      PCollection<Row> rows;
      switch (String.valueOf(options.getReader())) {
        case "java-native":
          rows = readJavaNative(p, options);
          break;
        case "java-schema":
          rows = readJavaSchema(p, options);
          break;
        case "rust":
          rows = readRust(p, options);
          break;
        default:
          throw new IllegalArgumentException("--reader must be java-native, java-schema or rust");
      }
      rows.apply("Touch", ParDo.of(new TouchRowFn(flatten)))
          .apply("Write", TextIO.write().to(options.getOutput()));
    } else if ("gcs_to_bt".equals(options.getShape())) {
      if (options.getInput() == null || options.getInput().isEmpty()) {
        throw new IllegalArgumentException("gcs_to_bt needs --input");
      }
      PCollection<String> lines = p.apply("Read", TextIO.read().from(options.getInput()));
      switch (String.valueOf(options.getWriter())) {
        case "java-native":
          writeJavaNative(lines, options);
          break;
        case "java-schema":
          writeJavaSchema(lines, options);
          break;
        case "rust":
          writeRust(lines, options);
          break;
        default:
          throw new IllegalArgumentException("--writer must be java-native, java-schema or rust");
      }
    } else {
      throw new IllegalArgumentException("--shape must be bt_to_gcs or gcs_to_bt");
    }

    PipelineResult result = p.run();
    if (result instanceof DataflowPipelineJob) {
      System.out.println("JOB_ID=" + ((DataflowPipelineJob) result).getJobId());
      return;
    }
    PipelineResult.State state = result.waitUntilFinish();
    if (state != PipelineResult.State.DONE) {
      throw new IllegalStateException("The job finished in state " + state);
    }
  }
}
