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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg benchmark in a Java pipeline: the streaming {@code ingest} shape with {@code KafkaIO}
 * and {@code IcebergIO} ({@code --writer=java-native}).
 *
 * <p>The pipeline reads the DeviceMetric JSON records of {@code --topic}, counts each record in
 * {@code e2e/rows} and {@code e2e/latency_ms} (the same step as the Kafka benchmark), parses it as
 * the Python reference job does, and appends the rows to {@code --table} of the BigLake REST
 * catalog. The table is partitioned by the hour of {@code timestamp_ms}. {@code IcebergIO} takes
 * no table properties, so the catalog sets them with its {@code table-default.} properties.
 *
 * <p>{@code ./gradlew :sdks:rust:icebergBenchmark -Ppipeline=java -Pwriter=java-native} runs this
 * class; {@code benchmarks/tools/job_wait.py} waits for and drains the submitted job.
 */
public class IcebergBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergBenchmark.class);

  static final String BIGLAKE = "https://biglake.googleapis.com/iceberg/v1/restcatalog";

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
    @Description("Writer: java-native (IcebergIO).")
    @Default.String("java-native")
    String getWriter();

    void setWriter(String value);

    @Description("Shape: ingest.")
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

    @Description("Hash-prefixed data file paths (write.object-storage.enabled).")
    @Default.Boolean(false)
    boolean getObjectStorage();

    void setObjectStorage(boolean value);
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

  /** The catalog of the Python reference job, with the table properties as table defaults. */
  static IcebergCatalogConfig catalog(Options options) {
    Map<String, String> props = new HashMap<>();
    props.put("type", "rest");
    props.put("uri", options.getCatalogUri());
    props.put("warehouse", options.getWarehouse());
    props.put("rest.auth.type", "google");
    props.put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO");
    props.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
    props.put("rest-metrics-reporting-enabled", "true");
    if (options.getUserProject() != null) {
      props.put("header.x-goog-user-project", options.getUserProject());
    }
    if (options.getGcpLocation() != null) {
      props.put("gcp_location", options.getGcpLocation());
    }
    props.put("table-default.gcp.biglake.table-management.enabled", "true");
    props.put("table-default.gcp.biglake.bigquery-dml.enabled", "true");
    props.put("table-default.write.parquet.compression-codec", "zstd");
    if (options.getObjectStorage()) {
      props.put("table-default.write.object-storage.enabled", "true");
    }
    return IcebergCatalogConfig.builder()
        .setCatalogName("biglake")
        .setCatalogProperties(props)
        .build();
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    if (!"ingest".equals(options.getShape()) || !"java-native".equals(options.getWriter())) {
      throw new IllegalArgumentException("Use --shape=ingest --writer=java-native");
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
    readValues(p, options)
        .apply("Touch", ParDo.of(new TouchFn()))
        .apply("Parse", ParDo.of(new ParseFn()))
        .setRowSchema(DEVICE_METRIC)
        .apply(
            "Write",
            IcebergIO.writeRows(catalog(options))
                .to(TableIdentifier.parse(options.getTable()))
                .withPartitionFields(Arrays.asList("hour(timestamp_ms)"))
                .withTriggeringFrequency(
                    Duration.standardSeconds(options.getTriggeringFrequencySeconds())));
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
