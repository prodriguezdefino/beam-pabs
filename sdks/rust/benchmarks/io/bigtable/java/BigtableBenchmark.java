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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;

/**
 * Cloud Bigtable benchmark in a Java pipeline: the {@code bt_to_gcs} shape.
 *
 * <p>Readers ({@code --reader}):
 *
 * <ul>
 *   <li>{@code java-native}: {@code BigtableIO.read()} in the Java pipeline environment.
 *   <li>{@code java-schema}: {@code BigtableReadSchemaTransformProvider} in the Java pipeline
 *       environment, which converts {@code com.google.bigtable.v2.Row} into Beam {@code Row}s.
 *   <li>{@code rust}: the Rust {@code BigtableReadProvider} through the Rust expansion service.
 * </ul>
 *
 * <p>Shape ({@code --shape}):
 *
 * <ul>
 *   <li>{@code bt_to_gcs}: reads {@code --bigtableTable} with {@code --reader}, counts each row in
 *       {@code e2e/rows} and formats it as a tab-separated line in a Java step, and writes the
 *       lines to {@code --output} in Cloud Storage with {@code TextIO.write()}.
 * </ul>
 */
public class BigtableBenchmark {
  static final String RUST_READ_URN = "beam:schematransform:org.apache.beam:rust:bigtable_read:v1";
  static final String SCHEMA_TRANSFORM_URN = "beam:expansion:payload:schematransform:v1";

  /** Benchmark flags. The Dataflow flags come from {@link DataflowPipelineOptions}. */
  public interface Options extends DataflowPipelineOptions {
    @Description("Reader: java-native (BigtableIO), java-schema (SchemaTransform) or rust.")
    String getReader();

    void setReader(String value);

    @Description("Shape: bt_to_gcs.")
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

    @Description("Cloud Storage output prefix for bt_to_gcs.")
    String getOutput();

    void setOutput(String value);

    @Description("Address of the Rust expansion service.")
    @Default.String("localhost:8097")
    String getExpansionService();

    void setExpansionService(String value);

    @Description("Fail the job unless e2e/rows equals this count.")
    Long getExpectedRows();

    void setExpectedRows(Long value);
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
            new java.util.LinkedHashMap<>(bigtableRow.getFamiliesCount());
        for (Family fam : bigtableRow.getFamiliesList()) {
          Map<String, List<Row>> columns = new java.util.LinkedHashMap<>(fam.getColumnsCount());
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

  static long committedRows(PipelineResult result) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("e2e", "rows"))
                    .build());
    long total = 0;
    for (MetricResult<Long> counter : metrics.getCounters()) {
      Long committed = counter.getCommitted();
      if (committed != null) {
        total += committed;
      }
    }
    return total;
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    if (!"bt_to_gcs".equals(options.getShape())) {
      throw new IllegalArgumentException("--shape must be bt_to_gcs");
    }
    if (options.getOutput() == null || options.getOutput().isEmpty()) {
      throw new IllegalArgumentException("bt_to_gcs needs --output");
    }

    Pipeline p = Pipeline.create(options);
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

    PipelineResult result = p.run();
    PipelineResult.State state = result.waitUntilFinish();
    if (state != PipelineResult.State.DONE) {
      throw new IllegalStateException("The job finished in state " + state);
    }
    if (options.getExpectedRows() != null) {
      long committed = committedRows(result);
      if (committed != options.getExpectedRows()) {
        throw new IllegalStateException(
            "The job read " + committed + " rows, expected " + options.getExpectedRows());
      }
    }
  }
}
