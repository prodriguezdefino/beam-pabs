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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg benchmark in a Java pipeline: the streaming {@code ingest} shape with {@code KafkaIO} and
 * {@code IcebergIO} ({@code --writer=java-native}), or with the Rust Kafka read and Rust Iceberg
 * write providers through the Rust expansion service ({@code --writer=rust}). The {@code count}
 * shape reads {@code --table} with {@code IcebergIO} ({@code --reader=java-native}) or with the
 * Rust Iceberg read provider ({@code --reader=rust}), and counts the rows.
 *
 * <p>The pipeline reads the DeviceMetric JSON records of {@code --topic}, counts each record in
 * {@code e2e/rows} and {@code e2e/latency_ms} (the same step as the Kafka benchmark), parses it as
 * the Python reference job does, and appends the rows to {@code --table} of the BigLake REST
 * catalog. The table is partitioned by the hour of {@code timestamp_ms}. {@code IcebergIO} takes no
 * table properties, so the catalog sets them with its {@code table-default.} properties; the Rust
 * provider sets {@code table_properties} directly.
 *
 * <p>{@code ./gradlew :sdks:rust:icebergBenchmark -Ppipeline=java -Pwriters=java-native,rust} runs
 * this class; {@code benchmarks/tools/job_wait.py} waits for and drains the submitted job.
 */
public class IcebergBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergBenchmark.class);

  static final String BIGLAKE = "https://biglake.googleapis.com/iceberg/v1/restcatalog";
  static final String RUST_KAFKA_READ_URN =
      "beam:schematransform:org.apache.beam:rust:kafka_read:v1";
  static final String RUST_ICEBERG_WRITE_URN =
      "beam:schematransform:org.apache.beam:rust:iceberg_write:v1";
  static final String RUST_ICEBERG_READ_URN =
      "beam:schematransform:org.apache.beam:rust:iceberg_read:v1";
  static final String SCHEMA_TRANSFORM_URN = "beam:expansion:payload:schematransform:v1";

  static final Schema DEVICE_METRIC =
      Schema.builder()
          .addInt64Field("id")
          .addDateTimeField("timestamp_ms")
          .addStringField("device_id")
          .addDoubleField("payload")
          .addStringField("location")
          .build();

  /**
   * Upper bounds (ms) of the latency buckets. Keep in sync with {@code LATENCY_BOUNDS_MS} of the
   * Kafka benchmark and of {@code io_report.py}.
   */
  static final long[] LATENCY_BOUNDS_MS = {
    50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000
  };

  public interface Options extends DataflowPipelineOptions {
    @Description("Writer: java-native (IcebergIO) or rust (Rust expansion service).")
    @Default.String("java-native")
    String getWriter();

    void setWriter(String value);

    @Description("Reader of the count shape: java-native (IcebergIO) or rust.")
    @Default.String("")
    String getReader();

    void setReader(String value);

    @Description("Shape: ingest or count.")
    @Default.String("ingest")
    String getShape();

    void setShape(String value);

    @Description("Kafka bootstrap servers (host:port).")
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Description("The topic of the DeviceMetric JSON records.")
    String getTopic();

    void setTopic(String value);

    @Description("Where a consumer group with no committed offset starts: earliest or latest.")
    @Default.String("latest")
    String getAutoOffsetReset();

    void setAutoOffsetReset(String value);

    @Description("Kafka consumer group.id. Default: a new group for each job.")
    @Default.String("")
    String getGroupId();

    void setGroupId(String value);

    @Description("The REST catalog endpoint.")
    @Default.String(BIGLAKE)
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("The warehouse of the catalog, for example bl://projects/<p>/catalogs/<c>.")
    String getWarehouse();

    void setWarehouse(String value);

    @Description("The table, as namespace.name.")
    String getTable();

    void setTable(String value);

    @Description("The project that BigLake bills, as the x-goog-user-project header.")
    String getUserProject();

    void setUserProject(String value);

    @Description("The region of the catalog, as the gcp_location catalog property.")
    String getGcpLocation();

    void setGcpLocation(String value);

    @Description("How often the write commits a snapshot, in seconds.")
    @Default.Integer(60)
    int getTriggeringFrequencySeconds();

    void setTriggeringFrequencySeconds(int value);

    @Description("Number of write shards for --writer=rust. Unset or <= 0 uses the default.")
    Integer getNumShards();

    void setNumShards(Integer value);

    @Description("Hash-prefixed data file paths (write.object-storage.enabled).")
    @Default.Boolean(false)
    boolean getObjectStorage();

    void setObjectStorage(boolean value);

    @Description("Address of the Rust expansion service.")
    @Default.String("localhost:8097")
    String getExpansionService();

    void setExpansionService(String value);
  }

  static Map<String, Object> managedKafkaAuth() {
    Map<String, Object> props = new HashMap<>();
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "OAUTHBEARER");
    props.put(
        "sasl.login.callback.handler.class",
        "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler");
    props.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
    return props;
  }

  /** Counts {@code e2e/rows} and records {@code e2e/latency_ms} from the record timestamp. */
  static class TouchFn extends DoFn<byte[], byte[]> {
    private final Counter rows = Metrics.counter("e2e", "rows");
    private final Distribution latencyMs = Metrics.distribution("e2e", "latency_ms");
    private final Counter[] latencyBuckets = latencyBuckets();

    static Counter[] latencyBuckets() {
      Counter[] buckets = new Counter[LATENCY_BOUNDS_MS.length + 1];
      for (int i = 0; i < LATENCY_BOUNDS_MS.length; i++) {
        buckets[i] = Metrics.counter("e2e", "latency_le_" + LATENCY_BOUNDS_MS[i]);
      }
      buckets[LATENCY_BOUNDS_MS.length] = Metrics.counter("e2e", "latency_le_inf");
      return buckets;
    }

    static int bucket(long latency) {
      int i = 0;
      while (i < LATENCY_BOUNDS_MS.length && latency > LATENCY_BOUNDS_MS[i]) {
        i++;
      }
      return i;
    }

    @ProcessElement
    public void process(
        @Element byte[] element, @Timestamp Instant timestamp, OutputReceiver<byte[]> out) {
      rows.inc();
      long tsMs = timestamp.getMillis();
      long nowMs = System.currentTimeMillis();
      if (tsMs > 0 && nowMs >= tsMs) {
        long latency = nowMs - tsMs;
        latencyMs.update(latency);
        latencyBuckets[bucket(latency)].inc();
      }
      out.output(element);
    }
  }

  /**
   * Parses a DeviceMetric JSON record, as {@code ParseAndValidateMessage} of the Python reference
   * job. A record that does not parse is dropped and counted in {@code e2e/parse_errors}.
   */
  static class ParseFn extends DoFn<byte[], Row> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Counter parseErrors = Metrics.counter("e2e", "parse_errors");

    @ProcessElement
    public void process(@Element byte[] value, OutputReceiver<Row> out) {
      try {
        JsonNode node = MAPPER.readTree(value);
        JsonNode location = node.get("location");
        out.output(
            Row.withSchema(DEVICE_METRIC)
                .addValues(
                    node.get("id").asLong(),
                    new Instant(node.get("timestamp").asLong()),
                    node.get("device_id").asText(),
                    node.get("payload").asDouble(),
                    location == null || location.isNull() ? "us-central1" : location.asText())
                .build());
      } catch (IOException | RuntimeException e) {
        parseErrors.inc();
      }
    }
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

  /** Extracts the {@code payload} bytes of a {@code RAW} Kafka row, keeping its timestamp. */
  static class ExtractPayloadFn extends DoFn<Row, byte[]> {
    @ProcessElement
    public void process(@Element Row row, OutputReceiver<byte[]> out) {
      byte[] bytes = row.getBytes("payload");
      if (bytes == null) {
        throw new IllegalArgumentException("Null payload bytes in RAW Kafka row");
      }
      out.output(bytes);
    }
  }

  static PCollection<byte[]> readValues(Pipeline p, Options options) {
    Map<String, Object> consumerConfig = managedKafkaAuth();
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getAutoOffsetReset());
    consumerConfig.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        options.getGroupId().isEmpty()
            ? "iceberg-bench-java-" + System.currentTimeMillis()
            : options.getGroupId());
    // The element timestamp is the record CreateTime, as in the Rust read.
    return p.apply(
            "Read",
            KafkaIO.readBytes()
                .withTopic(options.getTopic())
                .withBootstrapServers(options.getBootstrapServers())
                .withConsumerConfigUpdates(consumerConfig)
                .withCreateTime(Duration.standardMinutes(1))
                .commitOffsetsInFinalize()
                .withoutMetadata())
        .apply("Values", Values.create());
  }

  static PCollection<byte[]> readRustValues(Pipeline p, Options options) throws IOException {
    String groupId =
        options.getGroupId().isEmpty()
            ? "iceberg-bench-java-rust-" + System.currentTimeMillis()
            : options.getGroupId();
    Map<String, String> consumerUpdates = new HashMap<>();
    consumerUpdates.put("group.id", groupId);

    Schema configSchema =
        Schema.builder()
            .addStringField("bootstrap_servers")
            .addStringField("topic")
            .addStringField("format")
            .addStringField("auto_offset_reset_config")
            .addBooleanField("with_gcp_adc")
            .addMapField(
                "consumer_config_updates", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();
    Row config =
        Row.withSchema(configSchema)
            .addValues(
                options.getBootstrapServers(),
                options.getTopic(),
                "RAW",
                options.getAutoOffsetReset(),
                true,
                consumerUpdates)
            .build();
    byte[] payload = schemaTransformPayload(RUST_KAFKA_READ_URN, configSchema, config);
    PCollectionTuple outputs =
        p.apply(
            "Read",
            External.<PBegin, Row>of(SCHEMA_TRANSFORM_URN, payload, options.getExpansionService())
                .withMultiOutputs());
    @SuppressWarnings("unchecked")
    PCollection<Row> rawRows = (PCollection<Row>) outputs.get(new TupleTag<Row>("output"));
    return rawRows
        .apply("PayloadBytes", ParDo.of(new ExtractPayloadFn()))
        .setCoder(ByteArrayCoder.of());
  }

  static Map<String, String> catalogProperties(Options options) {
    Map<String, String> props = new HashMap<>();
    props.put("type", "rest");
    props.put("uri", options.getCatalogUri());
    props.put("warehouse", options.getWarehouse());
    props.put("rest.auth.type", "google");
    props.put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO");
    props.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
    props.put("rest-metrics-reporting-enabled", "true");
    if (options.getUserProject() != null && !options.getUserProject().isEmpty()) {
      props.put("header.x-goog-user-project", options.getUserProject());
    }
    if (options.getGcpLocation() != null && !options.getGcpLocation().isEmpty()) {
      props.put("gcp_location", options.getGcpLocation());
    }
    return props;
  }

  static Map<String, String> tableProperties(Options options) {
    Map<String, String> props = new HashMap<>();
    props.put("gcp.biglake.table-management.enabled", "true");
    props.put("gcp.biglake.bigquery-dml.enabled", "true");
    props.put("write.parquet.compression-codec", "zstd");
    if (options.getObjectStorage()) {
      props.put("write.object-storage.enabled", "true");
    }
    return props;
  }

  /** The catalog of the Python reference job, with the table properties as table defaults. */
  static IcebergCatalogConfig catalog(Options options) {
    Map<String, String> props = catalogProperties(options);
    for (Map.Entry<String, String> entry : tableProperties(options).entrySet()) {
      props.put("table-default." + entry.getKey(), entry.getValue());
    }
    return IcebergCatalogConfig.builder()
        .setCatalogName("biglake")
        .setCatalogProperties(props)
        .build();
  }

  static void writeRust(PCollection<Row> rows, Options options) throws IOException {
    Schema.Builder schema =
        Schema.builder()
            .addStringField("table")
            .addStringField("catalog_name")
            .addMapField("catalog_properties", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .addMapField("table_properties", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .addArrayField("partition_fields", Schema.FieldType.STRING)
            .addInt32Field("triggering_frequency_seconds");
    List<Object> values = new ArrayList<>();
    values.add(options.getTable());
    values.add("biglake");
    values.add(catalogProperties(options));
    values.add(tableProperties(options));
    values.add(Arrays.asList("hour(timestamp_ms)"));
    values.add(options.getTriggeringFrequencySeconds());
    if (options.getNumShards() != null && options.getNumShards() > 0) {
      schema.addInt32Field("num_shards");
      values.add(options.getNumShards());
    }
    Schema configSchema = schema.build();
    Row config = Row.withSchema(configSchema).addValues(values).build();
    byte[] payload = schemaTransformPayload(RUST_ICEBERG_WRITE_URN, configSchema, config);
    PCollectionTuple.of(new TupleTag<Row>("input"), rows)
        .apply(
            "Write",
            External.<PCollectionTuple, Row>of(
                    SCHEMA_TRANSFORM_URN, payload, options.getExpansionService())
                .withMultiOutputs());
  }

  static PCollection<Row> readRustRows(Pipeline p, Options options) throws IOException {
    Schema configSchema =
        Schema.builder()
            .addStringField("table")
            .addStringField("catalog_name")
            .addMapField("catalog_properties", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();
    Row config =
        Row.withSchema(configSchema)
            .addValues(options.getTable(), "biglake", catalogProperties(options))
            .build();
    byte[] payload = schemaTransformPayload(RUST_ICEBERG_READ_URN, configSchema, config);
    PCollectionTuple outputs =
        p.apply(
            "Read",
            External.<PBegin, Row>of(SCHEMA_TRANSFORM_URN, payload, options.getExpansionService())
                .withMultiOutputs());
    @SuppressWarnings("unchecked")
    PCollection<Row> rows = (PCollection<Row>) outputs.get(new TupleTag<Row>("output"));
    return rows;
  }

  /** Reads {@code --table} with the selected reader and counts the rows. */
  static void count(Pipeline p, Options options) throws IOException {
    PCollection<Row> rows =
        "rust".equals(options.getReader())
            ? readRustRows(p, options)
            : p.apply(
                "Read",
                IcebergIO.readRows(catalog(options)).from(TableIdentifier.parse(options.getTable())));
    rows.apply("CountRows", Count.globally());
  }

  static void run(Pipeline p) {
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

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    if ("count".equals(options.getShape())) {
      if (!"java-native".equals(options.getReader()) && !"rust".equals(options.getReader())) {
        throw new IllegalArgumentException("Use --shape=count --reader=java-native or --reader=rust");
      }
      if (options.getWarehouse() == null || options.getTable() == null) {
        throw new IllegalArgumentException("--warehouse and --table are required");
      }
      Pipeline p = Pipeline.create(options);
      count(p, options);
      run(p);
      return;
    }
    if (!"ingest".equals(options.getShape())
        || (!"java-native".equals(options.getWriter()) && !"rust".equals(options.getWriter()))) {
      throw new IllegalArgumentException(
          "Use --shape=ingest --writer=java-native or --writer=rust, or --shape=count");
    }
    for (String required :
        new String[] {
          options.getBootstrapServers(), options.getTopic(), options.getWarehouse(),
          options.getTable()
        }) {
      if (required == null || required.isEmpty()) {
        throw new IllegalArgumentException(
            "--bootstrapServers, --topic, --warehouse and --table are required");
      }
    }
    options.setStreaming(true);

    Pipeline p = Pipeline.create(options);
    PCollection<byte[]> rawBytes =
        "rust".equals(options.getWriter()) ? readRustValues(p, options) : readValues(p, options);
    PCollection<Row> parsed =
        rawBytes
            .apply("Touch", ParDo.of(new TouchFn()))
            .apply("Parse", ParDo.of(new ParseFn()))
            .setRowSchema(DEVICE_METRIC);
    if ("rust".equals(options.getWriter())) {
      writeRust(parsed, options);
    } else {
      parsed.apply(
          "Write",
          IcebergIO.writeRows(catalog(options))
              .to(TableIdentifier.parse(options.getTable()))
              .withPartitionFields(Arrays.asList("hour(timestamp_ms)"))
              .withTriggeringFrequency(
                  Duration.standardSeconds(options.getTriggeringFrequencySeconds())));
    }
    run(p);
  }
}
