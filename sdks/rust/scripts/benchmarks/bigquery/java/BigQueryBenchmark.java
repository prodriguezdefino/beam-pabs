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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;

/**
 * BigQuery benchmark in a Java pipeline: one reader and, for {@code copy}, one writer.
 *
 * <p>Readers ({@code --reader}):
 *
 * <ul>
 *   <li>{@code java-native}: {@code BigQueryIO} with {@code DIRECT_READ}. {@code --nativeOutput}
 *       selects {@code TableRow}s or Beam rows, and {@code --nativeFormat} selects Avro or Arrow
 *       read sessions. {@code copy} always reads Beam rows, because the writers take Beam rows.
 *   <li>{@code java-managed}: the Java provider through Managed I/O. It reads {@code TableRow}s
 *       with the same source and converts them to Beam rows.
 *   <li>{@code rust}: the Rust provider through the Rust expansion service.
 * </ul>
 *
 * <p>Writers ({@code --writer}, {@code copy} only):
 *
 * <ul>
 *   <li>{@code java-native}: {@code BigQueryIO} with the Storage Write API ({@code
 *       STORAGE_WRITE_API}, or {@code STORAGE_API_AT_LEAST_ONCE} with {@code --semantics
 *       at_least_once}), in the pipeline environment.
 *   <li>{@code java-managed}: the Java provider through Managed I/O. For a bounded input it
 *       writes with file loads, not the Storage Write API, and ignores {@code --semantics}.
 *   <li>{@code rust}: the Rust provider through the Rust expansion service.
 * </ul>
 *
 * <p>{@code java-native} runs in the pipeline environment, so its rows go to the next step in the
 * same process. Dataflow upgrades a Managed transform before the job starts and runs it in an
 * environment of its own, and the Rust transforms run in the Rust environment. Those rows cross
 * the Fn API data plane to and from the pipeline environment.
 *
 * <p>Shapes:
 *
 * <ul>
 *   <li>{@code count} counts the rows. The job does almost nothing but read.
 *   <li>{@code per_key} counts the rows for each value of {@code --key} and then counts the keys.
 *   <li>{@code copy} reads the table, counts each row in the {@code e2e/rows} metric in a Java
 *       step, and writes the rows to {@code --destination}. The Java step runs in the Java
 *       worker, so the rows of a Rust reader come to Java and go back to a Rust writer.
 * </ul>
 *
 * <p>{@code ../python/bigquery_benchmark.py} runs the same shapes in a Python pipeline. {@code
 * ./gradlew :sdks:rust:bigQueryBenchmark -Ppipeline=java} runs this class.
 */
public class BigQueryBenchmark {
  static final String RUST_READ_URN =
      "beam:schematransform:org.apache.beam:rust:bigquery_storage_read:v1";
  static final String RUST_WRITE_URN =
      "beam:schematransform:org.apache.beam:rust:bigquery_storage_write:v1";
  static final String SCHEMA_TRANSFORM_URN = "beam:expansion:payload:schematransform:v1";

  /** Benchmark flags. The Dataflow flags come from {@link DataflowPipelineOptions}. */
  public interface Options extends DataflowPipelineOptions {
    @Description("Reader: java-native (BigQueryIO), java-managed (Managed I/O) or rust.")
    String getReader();

    void setReader(String value);

    @Description("Writer of copy: java-native (BigQueryIO), java-managed (Managed I/O) or rust.")
    String getWriter();

    void setWriter(String value);

    @Description("Output of the java-native reader: tablerow or row (Beam rows).")
    @Default.String("tablerow")
    String getNativeOutput();

    void setNativeOutput(String value);

    @Description("Read session format of the java-native reader: avro or arrow.")
    @Default.String("avro")
    String getNativeFormat();

    void setNativeFormat(String value);

    @Description("Shape: count, per_key or copy.")
    @Default.String("count")
    String getShape();

    void setShape(String value);

    @Description("Table to read, as project:dataset.table or project.dataset.table.")
    @Default.String("bigquery-public-data:samples.wikipedia")
    String getTable();

    void setTable(String value);

    @Description("Comma-separated columns to read. Empty reads all columns.")
    @Default.String("")
    String getSelectedFields();

    void setSelectedFields(String value);

    @Description("Row filter of the read session.")
    @Default.String("")
    String getRowRestriction();

    void setRowRestriction(String value);

    @Description("Column to count by in the per_key shape.")
    @Default.String("language")
    String getKey();

    void setKey(String value);

    @Description("Table that copy creates, as project.dataset.table.")
    String getDestination();

    void setDestination(String value);

    @Description("Guarantee of the copy writer: exactly_once or at_least_once.")
    @Default.String("exactly_once")
    String getSemantics();

    void setSemantics(String value);

    @Description("Address of the Rust expansion service.")
    @Default.String("localhost:8097")
    String getExpansionService();

    void setExpansionService(String value);

    @Description("Arrow buffer compression of the Rust reader: NONE, LZ4_FRAME or ZSTD.")
    @Default.String("")
    String getCompression();

    void setCompression(String value);

    @Description("Fail the job unless it reads this many rows (count shape).")
    Long getExpectedRows();

    void setExpectedRows(Long value);

    @Description(
        "Copy only: reshuffle the touched rows, so the writer runs in a stage of its own instead"
            + " of fused with the reader and Touch.")
    @Default.Boolean(false)
    boolean getReshuffle();

    void setReshuffle(boolean value);
  }

  static List<String> selectedFields(Options options) {
    return Arrays.stream(options.getSelectedFields().split(","))
        .filter(field -> !field.isEmpty())
        .collect(Collectors.toList());
  }

  /** Sets the table, method, format and filters that every {@code java-native} read shares. */
  static <T> BigQueryIO.TypedRead<T> directRead(BigQueryIO.TypedRead<T> read, Options options) {
    String format = options.getNativeFormat();
    if (!"avro".equals(format) && !"arrow".equals(format)) {
      throw new IllegalArgumentException("--nativeFormat must be avro or arrow");
    }
    BigQueryIO.TypedRead<T> direct =
        read.from(options.getTable())
            .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
            .withFormat("arrow".equals(format) ? DataFormat.ARROW : DataFormat.AVRO);
    List<String> fields = selectedFields(options);
    if (!fields.isEmpty()) {
      direct = direct.withSelectedFields(fields);
    }
    if (!options.getRowRestriction().isEmpty()) {
      direct = direct.withRowRestriction(options.getRowRestriction());
    }
    return direct;
  }

  static PCollection<TableRow> readNativeTableRows(Pipeline p, Options options) {
    return p.apply("Read", directRead(BigQueryIO.readTableRows(), options));
  }

  /**
   * Reads Beam rows with {@code BigQueryIO.read}, which converts each record straight to a row.
   * The row coder needs the schema before the pipeline runs, so it comes from the table metadata.
   */
  static PCollection<Row> readNativeRows(Pipeline p, Options options)
      throws IOException, InterruptedException {
    TableReference ref = BigQueryHelpers.parseTableSpec(options.getTable());
    List<String> fields = selectedFields(options);
    Table table;
    try (BigQueryServices.DatasetService datasets =
        new BigQueryServicesImpl().getDatasetService(options.as(BigQueryOptions.class))) {
      table = fields.isEmpty() ? datasets.getTable(ref) : datasets.getTable(ref, fields);
    } catch (IOException | InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Cannot close the BigQuery dataset service", e);
    }
    if (table == null) {
      throw new IllegalArgumentException("Table not found: " + options.getTable());
    }
    Schema schema = BigQueryUtils.fromTableSchema(table.getSchema());
    return p.apply(
        "Read",
        directRead(BigQueryIO.read(new ToBeamRow(schema)).withCoder(RowCoder.of(schema)), options));
  }

  /** Converts one record of a read session to a Beam row of a schema that is known in advance. */
  static class ToBeamRow implements SerializableFunction<SchemaAndRecord, Row> {
    private final Schema schema;

    ToBeamRow(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Row apply(SchemaAndRecord input) {
      return BigQueryUtils.toBeamRow(
          input.getRecord(), schema, BigQueryUtils.ConversionOptions.builder().build());
    }
  }

  /**
   * Reads through Managed I/O, in the form of the Dataflow Managed I/O guide. Apply {@code
   * Managed.read} to the pipeline. Applied to {@code PCollectionRowTuple.empty(p)}, the Dataflow
   * upgrade fails with "Error processing pipeline." and the job does not start (Beam 2.76.0).
   */
  static PCollection<Row> readJavaManaged(Pipeline p, Options options) {
    Map<String, Object> config = new HashMap<>();
    config.put("table", options.getTable());
    List<String> fields = selectedFields(options);
    if (!fields.isEmpty()) {
      config.put("fields", fields);
    }
    if (!options.getRowRestriction().isEmpty()) {
      config.put("row_restriction", options.getRowRestriction());
    }
    return p.apply(Managed.read(Managed.BIGQUERY).withConfig(config)).getSinglePCollection();
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

  /**
   * Applies the Rust read provider. The expansion service maps the config fields by name, so the
   * config row holds only the fields that the benchmark sets.
   */
  static PCollection<Row> readRust(Pipeline p, Options options) throws IOException {
    Schema.Builder schema = Schema.builder();
    List<Object> values = new ArrayList<>();
    schema.addStringField("table_spec");
    values.add(options.getTable());
    schema.addStringField("billing_project");
    values.add(options.getProject());
    List<String> fields = selectedFields(options);
    if (!fields.isEmpty()) {
      schema.addArrayField("selected_fields", Schema.FieldType.STRING);
      values.add(fields);
    }
    if (!options.getRowRestriction().isEmpty()) {
      schema.addStringField("row_restriction");
      values.add(options.getRowRestriction());
    }
    if (!options.getCompression().isEmpty()) {
      schema.addStringField("compression");
      values.add(options.getCompression());
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

  static void writeJavaNative(PCollection<Row> rows, String table, boolean atLeastOnce) {
    rows.apply(
        "Write",
        BigQueryIO.<Row>write()
            .to(table)
            .useBeamSchema()
            .withMethod(
                atLeastOnce
                    ? BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE
                    : BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
  }

  static void writeJavaManaged(PCollection<Row> rows, String table) {
    Map<String, Object> config = new HashMap<>();
    config.put("table", table);
    PCollectionRowTuple.of("input", rows)
        .apply("Write", Managed.write(Managed.BIGQUERY).withConfig(config));
  }

  /**
   * Applies the Rust write provider. The provider takes its input under the tag {@code input},
   * and has no output without {@code error_handling}.
   */
  static void writeRust(
      PCollection<Row> rows, String table, boolean atLeastOnce, String expansionService)
      throws IOException {
    Schema configSchema =
        Schema.builder()
            .addStringField("table")
            .addBooleanField("use_at_least_once_semantics")
            .build();
    Row config = Row.withSchema(configSchema).addValues(table, atLeastOnce).build();
    byte[] payload = schemaTransformPayload(RUST_WRITE_URN, configSchema, config);
    PCollectionTuple.of(new TupleTag<Row>("input"), rows)
        .apply(
            "Write",
            External.<PCollectionTuple, Row>of(SCHEMA_TRANSFORM_URN, payload, expansionService)
                .withMultiOutputs());
  }

  /** Counts each row and emits it. */
  static class TouchFn extends DoFn<Row, Row> {
    private final Counter rows = Metrics.counter("e2e", "rows");

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> out) {
      rows.inc();
      out.output(row);
    }
  }

  /** Applies a read-only shape to the rows of any reader. {@code field} reads one column. */
  static <T> void applyReadShape(
      PCollection<T> rows, Options options, SerializableFunction<T, Object> field) {
    if ("count".equals(options.getShape())) {
      PCollection<Long> total = rows.apply("CountRows", Count.globally());
      if (options.getExpectedRows() != null) {
        PAssert.thatSingleton("CheckRows", total).isEqualTo(options.getExpectedRows());
      }
    } else {
      rows.apply(
              "Key",
              MapElements.into(TypeDescriptors.strings())
                  .via((T row) -> String.valueOf(field.apply(row))))
          .apply("CountPerKey", Count.perElement())
          .apply("CountKeys", Count.globally());
    }
  }

  /** Reads Beam rows with the selected reader, for {@code copy}. */
  static PCollection<Row> readRows(Pipeline p, Options options)
      throws IOException, InterruptedException {
    switch (String.valueOf(options.getReader())) {
      case "java-native":
        return readNativeRows(p, options);
      case "java-managed":
        return readJavaManaged(p, options);
      case "rust":
        return readRust(p, options);
      default:
        throw new IllegalArgumentException("--reader must be java-native, java-managed or rust");
    }
  }

  static void copy(Pipeline p, Options options) throws IOException, InterruptedException {
    if (options.getDestination() == null || options.getDestination().isEmpty()) {
      throw new IllegalArgumentException("copy needs --destination");
    }
    if (!"exactly_once".equals(options.getSemantics())
        && !"at_least_once".equals(options.getSemantics())) {
      throw new IllegalArgumentException("--semantics must be exactly_once or at_least_once");
    }
    if (!selectedFields(options).isEmpty()) {
      throw new IllegalArgumentException("copy reads whole rows: no --selectedFields");
    }
    boolean atLeastOnce = "at_least_once".equals(options.getSemantics());
    PCollection<Row> rows = readRows(p, options);
    PCollection<Row> touched =
        rows.apply("Touch", ParDo.of(new TouchFn())).setRowSchema(rows.getSchema());
    if (options.getReshuffle()) {
      touched =
          touched.apply("Reshuffle", Reshuffle.viaRandomKey()).setRowSchema(rows.getSchema());
    }
    String destination = options.getDestination();
    switch (String.valueOf(options.getWriter())) {
      case "java-native":
        writeJavaNative(touched, destination, atLeastOnce);
        break;
      case "java-managed":
        writeJavaManaged(touched, destination);
        break;
      case "rust":
        writeRust(touched, destination, atLeastOnce, options.getExpansionService());
        break;
      default:
        throw new IllegalArgumentException(
            "copy needs --writer: java-native, java-managed or rust");
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    String shape = options.getShape();
    String key = options.getKey();
    List<String> fields = selectedFields(options);
    if (!"count".equals(shape) && !"per_key".equals(shape) && !"copy".equals(shape)) {
      throw new IllegalArgumentException("--shape must be count, per_key or copy");
    }
    if (!"copy".equals(shape) && options.getWriter() != null) {
      throw new IllegalArgumentException("--writer applies to copy only, not to " + shape);
    }
    if ("per_key".equals(shape) && !fields.isEmpty() && !fields.contains(key)) {
      throw new IllegalArgumentException("--selectedFields must hold --key");
    }

    Pipeline p = Pipeline.create(options);
    if ("copy".equals(shape)) {
      copy(p, options);
    } else {
      switch (String.valueOf(options.getReader())) {
        case "java-native":
          if ("tablerow".equals(options.getNativeOutput())) {
            applyReadShape(readNativeTableRows(p, options), options, (TableRow row) -> row.get(key));
          } else if ("row".equals(options.getNativeOutput())) {
            applyReadShape(readNativeRows(p, options), options, (Row row) -> row.getValue(key));
          } else {
            throw new IllegalArgumentException("--nativeOutput must be tablerow or row");
          }
          break;
        case "java-managed":
          applyReadShape(readJavaManaged(p, options), options, (Row row) -> row.getValue(key));
          break;
        case "rust":
          applyReadShape(readRust(p, options), options, (Row row) -> row.getValue(key));
          break;
        default:
          throw new IllegalArgumentException("--reader must be java-native, java-managed or rust");
      }
    }

    PipelineResult.State state = p.run().waitUntilFinish();
    if (state != PipelineResult.State.DONE) {
      throw new IllegalStateException("The job finished in state " + state);
    }
  }
}
