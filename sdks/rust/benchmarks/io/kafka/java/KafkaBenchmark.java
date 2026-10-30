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
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
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
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka benchmark in a Java pipeline: a batch {@code load} shape and a streaming {@code copy}
 * shape.
 *
 * <p>Readers ({@code --reader}, {@code copy} only):
 *
 * <ul>
 *   <li>{@code java-native}: {@code KafkaIO.readBytes()} in the pipeline environment, followed by
 *       a {@code ParDo} that converts each record payload to a Beam row.
 *   <li>{@code java-managed}: the Java provider through Managed I/O.
 *   <li>{@code rust}: the Rust provider through the Rust expansion service.
 * </ul>
 *
 * <p>Writers ({@code --writer}):
 *
 * <ul>
 *   <li>{@code java-native}: a {@code ParDo} that converts each Beam row to {@code KV<byte[],
 *       byte[]>}, followed by {@code KafkaIO.write()} in the pipeline environment.
 *   <li>{@code java-managed}: the Java provider through Managed I/O.
 *   <li>{@code rust}: the Rust provider through the Rust expansion service.
 * </ul>
 *
 * <p>Shapes:
 *
 * <ul>
 *   <li>{@code load} reads {@code --table} from BigQuery with {@code BigQueryIO} ({@code
 *       DIRECT_READ}), counts each row in {@code e2e/rows}, and writes the rows to {@code --topic}
 *       with {@code --writer}. The BigQuery read runs in the Java worker, so the Rust worker runs
 *       only the Kafka write.
 *   <li>{@code copy} streams from {@code --topic} with {@code --reader}, passes each row through a
 *       Java step that counts {@code e2e/rows} and records {@code e2e/latency_ms} (wall clock
 *       minus the element timestamp, which is the Kafka record timestamp), and writes the rows to
 *       {@code --destinationTopic} with {@code --writer}. The Java step keeps a Rust reader from
 *       fusing with a Rust writer, so the rows cross the Fn API data plane to Java and back. The
 *       step also counts each latency in one {@code e2e/latency_le_<ms>} bucket, for
 *       percentiles.
 *   <li>{@code produce} writes {@code --rate} records per second to {@code --topic}, from a cycle
 *       of the JSON records in {@code --samplePath}, with {@code KafkaIO}. It gives every copy
 *       variant the same offered load, from a job that is not under test.
 * </ul>
 *
 * <p>Formats ({@code --format}): {@code JSON} decodes each record into a row of the Wikipedia
 * schema and encodes it again; {@code RAW} wraps the record bytes in a row of one {@code BYTES}
 * field; {@code BYTES} (copy with {@code java-native} to {@code java-native} only) passes the
 * {@code byte[]} of {@code KafkaIO.readBytes()} through a {@code byte[]} step to {@code
 * KafkaIO.write().values()}, so no step builds a row. It is the counterpart of {@code ReadBytes}
 * and {@code WriteBytes} in the Rust pipeline.
 *
 * <p>{@code ../python/kafka_benchmark.py} runs the same shapes in a Python pipeline. {@code
 * ./gradlew :sdks:rust:kafkaBenchmark -Ppipeline=java} runs this class.
 */
public class KafkaBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBenchmark.class);

  static final String RUST_READ_URN = "beam:schematransform:org.apache.beam:rust:kafka_read:v1";
  static final String RUST_WRITE_URN = "beam:schematransform:org.apache.beam:rust:kafka_write:v1";
  static final String SCHEMA_TRANSFORM_URN = "beam:expansion:payload:schematransform:v1";

  static final Schema RAW_BYTES_SCHEMA =
      Schema.builder().addField("payload", Schema.FieldType.BYTES).build();

  static final Schema WIKIPEDIA_SCHEMA =
      Schema.builder()
          .addStringField("title")
          .addNullableInt64Field("id")
          .addStringField("language")
          .addInt64Field("wp_namespace")
          .addNullableBooleanField("is_redirect")
          .addNullableInt64Field("revision_id")
          .addNullableStringField("contributor_ip")
          .addNullableInt64Field("contributor_id")
          .addNullableStringField("contributor_username")
          .addInt64Field("timestamp")
          .addNullableBooleanField("is_minor")
          .addNullableBooleanField("is_bot")
          .addNullableInt64Field("reversion_id")
          .addNullableStringField("comment")
          .addInt64Field("num_characters")
          .build();

  /**
   * The JSON Schema of {@code bigquery-public-data:samples.wikipedia}. All 15 properties are in
   * {@code required} in column order, and the 10 nullable columns set {@code "nullable": true}, so
   * both {@code JsonUtils} in Java and {@code schema_from_json_schema} in Rust build {@link
   * #WIKIPEDIA_SCHEMA}.
   */
  static final String WIKIPEDIA_JSON_SCHEMA =
      "{\"type\":\"object\",\"properties\":{"
          + "\"title\":{\"type\":\"string\"},"
          + "\"id\":{\"type\":\"integer\",\"nullable\":true},"
          + "\"language\":{\"type\":\"string\"},"
          + "\"wp_namespace\":{\"type\":\"integer\"},"
          + "\"is_redirect\":{\"type\":\"boolean\",\"nullable\":true},"
          + "\"revision_id\":{\"type\":\"integer\",\"nullable\":true},"
          + "\"contributor_ip\":{\"type\":\"string\",\"nullable\":true},"
          + "\"contributor_id\":{\"type\":\"integer\",\"nullable\":true},"
          + "\"contributor_username\":{\"type\":\"string\",\"nullable\":true},"
          + "\"timestamp\":{\"type\":\"integer\"},"
          + "\"is_minor\":{\"type\":\"boolean\",\"nullable\":true},"
          + "\"is_bot\":{\"type\":\"boolean\",\"nullable\":true},"
          + "\"reversion_id\":{\"type\":\"integer\",\"nullable\":true},"
          + "\"comment\":{\"type\":\"string\",\"nullable\":true},"
          + "\"num_characters\":{\"type\":\"integer\"}"
          + "},\"required\":["
          + "\"title\",\"id\",\"language\",\"wp_namespace\",\"is_redirect\","
          + "\"revision_id\",\"contributor_ip\",\"contributor_id\",\"contributor_username\","
          + "\"timestamp\",\"is_minor\",\"is_bot\",\"reversion_id\",\"comment\",\"num_characters\""
          + "]}";

  /** Benchmark flags. The Dataflow flags come from {@link DataflowPipelineOptions}. */
  public interface Options extends DataflowPipelineOptions {
    @Description("Reader of copy: java-native (KafkaIO), java-managed (Managed I/O) or rust.")
    String getReader();

    void setReader(String value);

    @Description("Writer: java-native (KafkaIO), java-managed (Managed I/O) or rust.")
    String getWriter();

    void setWriter(String value);

    @Description("Shape: load or copy.")
    @Default.String("load")
    String getShape();

    void setShape(String value);

    @Description(
        "Record format: JSON, RAW or BYTES. BYTES (copy, java-native reader and writer only)"
            + " passes byte[] from KafkaIO to KafkaIO with no row.")
    @Default.String("JSON")
    String getFormat();

    void setFormat(String value);

    @Description("Kafka bootstrap servers (host:port).")
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Description("Topic to write in load, or to read in copy.")
    String getTopic();

    void setTopic(String value);

    @Description("Topic that copy writes to.")
    String getDestinationTopic();

    void setDestinationTopic(String value);

    @Description("BigQuery table that load reads, as project:dataset.table.")
    @Default.String("bigquery-public-data:samples.wikipedia")
    String getTable();

    void setTable(String value);

    @Description("Row filter of the BigQuery read session in load.")
    @Default.String("")
    String getRowRestriction();

    void setRowRestriction(String value);

    @Description("Where a consumer group with no committed offset starts: earliest or latest.")
    @Default.String("earliest")
    String getAutoOffsetReset();

    void setAutoOffsetReset(String value);

    @Description("Kafka consumer group.id for copy.")
    @Default.String("")
    String getGroupId();

    void setGroupId(String value);

    @Description(
        "For copy with the java-native reader, the partition count of -topic. When it is set, the"
            + " read does not ask the broker for the partitions at submission. Runner v1 needs it"
            + " when the submitting machine cannot reach the broker.")
    @Default.Integer(0)
    int getPartitions();

    void setPartitions(int value);

    @Description("Address of the Rust expansion service.")
    @Default.String("localhost:8097")
    String getExpansionService();

    void setExpansionService(String value);

    @Description("For copy, drain the job once e2e/rows reaches at least this count.")
    Long getExpectedRows();

    void setExpectedRows(Long value);

    @Description("For copy, drain the job after this many seconds.")
    Long getDrainAfterSeconds();

    void setDrainAfterSeconds(Long value);

    @Description("For copy, print the job ID and exit once the job is submitted.")
    @Default.Boolean(false)
    boolean getDetach();

    void setDetach(boolean value);

    @Description("For produce, the records per second that the job writes to --topic.")
    @Default.Long(10_000L)
    long getRate();

    void setRate(long value);

    @Description(
        "For produce, a file of JSON records (one per line) that the job writes in a cycle.")
    String getSamplePath();

    void setSamplePath(String value);

    @Description(
        "For copy with the Rust reader, the longest time one read call keeps a partition, in "
            + "seconds. 0 keeps the default of the connector.")
    @Default.Long(0L)
    long getMaxReadTimePerCallSeconds();

    void setMaxReadTimePerCallSeconds(long value);

    @Description(
        "For copy with the Rust reader, the longest time one read call waits for the next "
            + "record, in milliseconds. 0 keeps the default of the connector.")
    @Default.Long(0L)
    long getMaxIdlePerCallMs();

    void setMaxIdlePerCallMs(long value);

    @Description("More properties of the Kafka consumer, as comma-separated key=value pairs.")
    @Default.String("")
    String getConsumerProperties();

    void setConsumerProperties(String value);

    @Description("More properties of the Kafka producer, as comma-separated key=value pairs.")
    @Default.String("")
    String getProducerProperties();

    void setProducerProperties(String value);
  }

  static Map<String, String> managedKafkaAuth() {
    Map<String, String> props = new HashMap<>();
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

  /** Parses comma-separated {@code key=value} pairs, splitting each pair at the first {@code =}. */
  static Map<String, String> parseProperties(String raw) {
    Map<String, String> props = new HashMap<>();
    if (raw == null || raw.isEmpty()) {
      return props;
    }
    for (String pair : raw.split(",")) {
      int eq = pair.indexOf('=');
      if (eq <= 0) {
        throw new IllegalArgumentException("'" + pair + "' is not a key=value pair");
      }
      props.put(pair.substring(0, eq), pair.substring(eq + 1));
    }
    return props;
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

  /** Reads Beam rows from BigQuery for {@code load}. */
  static PCollection<Row> readBigQueryRows(Pipeline p, Options options)
      throws IOException, InterruptedException {
    TableReference ref = BigQueryHelpers.parseTableSpec(options.getTable());
    Table table;
    try (BigQueryServices.DatasetService datasets =
        new BigQueryServicesImpl().getDatasetService(options.as(BigQueryOptions.class))) {
      table = datasets.getTable(ref);
    } catch (IOException | InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Cannot close the BigQuery dataset service", e);
    }
    if (table == null) {
      throw new IllegalArgumentException("Table not found: " + options.getTable());
    }
    Schema schema = BigQueryUtils.fromTableSchema(table.getSchema());
    BigQueryIO.TypedRead<Row> read =
        BigQueryIO.read(new ToBeamRow(schema))
            .withCoder(RowCoder.of(schema))
            .from(options.getTable())
            .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
            .withFormat(DataFormat.AVRO);
    if (!options.getRowRestriction().isEmpty()) {
      read = read.withRowRestriction(options.getRowRestriction());
    }
    return p.apply("Read", read).setRowSchema(schema);
  }

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

  /** Converts a byte payload from {@code KafkaIO.readBytes()} to a Beam row. */
  static class BytesToRowFn extends DoFn<byte[], Row> {
    private final SerializableFunction<byte[], Row> mapper;

    BytesToRowFn(SerializableFunction<byte[], Row> mapper) {
      this.mapper = mapper;
    }

    @ProcessElement
    public void process(@Element byte[] bytes, OutputReceiver<Row> out) {
      out.output(mapper.apply(bytes));
    }
  }

  /** Converts a Beam row to a {@code KV<byte[], byte[]>} for {@code KafkaIO.write()}. */
  static class RowToKvBytesFn extends DoFn<Row, KV<byte[], byte[]>> {
    private final SerializableFunction<Row, byte[]> mapper;

    RowToKvBytesFn(SerializableFunction<Row, byte[]> mapper) {
      this.mapper = mapper;
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<KV<byte[], byte[]>> out) {
      out.output(KV.of(null, mapper.apply(row)));
    }
  }

  static class RawBytesToRow implements SerializableFunction<byte[], Row> {
    @Override
    public Row apply(byte[] input) {
      return Row.withSchema(RAW_BYTES_SCHEMA).addValue(input).build();
    }
  }

  static class RowToRawBytes implements SerializableFunction<Row, byte[]> {
    private final String fieldName;

    RowToRawBytes(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public byte[] apply(Row input) {
      byte[] bytes = input.getBytes(fieldName);
      if (bytes == null) {
        throw new IllegalArgumentException("Null bytes in RAW row");
      }
      return bytes;
    }
  }

  static PCollection<Row> readJavaNative(Pipeline p, Options options) {
    Schema schema;
    SerializableFunction<byte[], Row> mapper;
    if ("RAW".equals(options.getFormat())) {
      schema = RAW_BYTES_SCHEMA;
      mapper = new RawBytesToRow();
    } else {
      schema = WIKIPEDIA_SCHEMA;
      mapper = JsonUtils.getJsonBytesToRowFunction(schema);
    }
    return readJavaNativeValues(p, options)
        .apply("DecodeRows", ParDo.of(new BytesToRowFn(mapper)))
        .setRowSchema(schema);
  }

  /** The record values of {@code --topic}, as {@code byte[]}, from {@code KafkaIO.readBytes()}. */
  static PCollection<byte[]> readJavaNativeValues(Pipeline p, Options options) {
    Map<String, Object> consumerConfig = new HashMap<>(managedKafkaAuth());
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getAutoOffsetReset());
    String groupId =
        options.getGroupId().isEmpty()
            ? "kafka-bench-java-native-" + System.currentTimeMillis()
            : options.getGroupId();
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerConfig.putAll(parseProperties(options.getConsumerProperties()));

    // The element timestamp is the record CreateTime, as in the Rust read, so e2e/latency_ms has
    // the same reference for both readers. The default policy uses the processing time. The read
    // commits the offsets once the runner has processed the records, as the Rust read does.
    KafkaIO.Read<byte[], byte[]> read =
        options.getPartitions() > 0
            ? KafkaIO.readBytes().withTopicPartitions(topicPartitions(options))
            : KafkaIO.readBytes().withTopic(options.getTopic());
    return p.apply(
            "ReadKafkaBytes",
            read.withBootstrapServers(options.getBootstrapServers())
                .withConsumerConfigUpdates(consumerConfig)
                .withCreateTime(Duration.standardMinutes(1))
                .commitOffsetsInFinalize()
                .withoutMetadata())
        .apply("Values", Values.create());
  }

  static List<TopicPartition> topicPartitions(Options options) {
    return IntStream.range(0, options.getPartitions())
        .mapToObj(partition -> new TopicPartition(options.getTopic(), partition))
        .collect(Collectors.toList());
  }

  static PCollection<Row> readJavaManaged(Pipeline p, Options options) {
    Map<String, String> consumerConfig = managedKafkaAuth();
    if (!options.getGroupId().isEmpty()) {
      consumerConfig.put("group.id", options.getGroupId());
    }
    consumerConfig.putAll(parseProperties(options.getConsumerProperties()));
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap_servers", options.getBootstrapServers());
    config.put("topic", options.getTopic());
    config.put("format", options.getFormat());
    config.put("auto_offset_reset_config", options.getAutoOffsetReset());
    config.put("consumer_config_updates", consumerConfig);
    if ("JSON".equals(options.getFormat())) {
      config.put("schema", WIKIPEDIA_JSON_SCHEMA);
    }
    return p.apply("Read", Managed.read(Managed.KAFKA).withConfig(config)).getSinglePCollection();
  }

  static PCollection<Row> readRust(Pipeline p, Options options) throws IOException {
    Schema.Builder schema =
        Schema.builder()
            .addStringField("bootstrap_servers")
            .addStringField("topic")
            .addStringField("format")
            .addStringField("auto_offset_reset_config")
            .addBooleanField("with_gcp_adc");
    List<Object> values = new ArrayList<>();
    values.add(options.getBootstrapServers());
    values.add(options.getTopic());
    values.add(options.getFormat());
    values.add(options.getAutoOffsetReset());
    values.add(true);
    if ("JSON".equals(options.getFormat())) {
      schema.addStringField("schema");
      values.add(WIKIPEDIA_JSON_SCHEMA);
    }
    Map<String, String> props = new HashMap<>();
    if (!options.getGroupId().isEmpty()) {
      props.put("group.id", options.getGroupId());
    }
    props.putAll(parseProperties(options.getConsumerProperties()));
    if (!props.isEmpty()) {
      schema.addMapField(
          "consumer_config_updates", Schema.FieldType.STRING, Schema.FieldType.STRING);
      values.add(props);
    }
    if (options.getMaxReadTimePerCallSeconds() > 0) {
      schema.addInt64Field("max_read_time_per_call_seconds");
      values.add(options.getMaxReadTimePerCallSeconds());
    }
    if (options.getMaxIdlePerCallMs() > 0) {
      schema.addInt64Field("max_idle_per_call_ms");
      values.add(options.getMaxIdlePerCallMs());
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

  static void writeJavaNative(PCollection<Row> rows, Options options, String topic) {
    Schema inputSchema = rows.getSchema();
    SerializableFunction<Row, byte[]> mapper =
        "RAW".equals(options.getFormat())
            ? new RowToRawBytes(inputSchema.getField(0).getName())
            : JsonUtils.getRowToJsonBytesFunction(inputSchema);
    Map<String, Object> producerConfig = new HashMap<>(managedKafkaAuth());
    producerConfig.putAll(parseProperties(options.getProducerProperties()));
    rows.apply("EncodeRows", ParDo.of(new RowToKvBytesFn(mapper)))
        .setCoder(KvCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()))
        .apply(
            "Write",
            KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopic(topic)
                .withProducerConfigUpdates(producerConfig)
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class));
  }

  /**
   * Writes each {@code byte[]} as the value of one record to {@code topic}, with no key, with
   * {@code KafkaIO.write().values()}. No step builds a row.
   */
  static void writeJavaNativeValues(PCollection<byte[]> values, Options options, String topic) {
    Map<String, Object> producerConfig = new HashMap<>(managedKafkaAuth());
    producerConfig.putAll(parseProperties(options.getProducerProperties()));
    values.apply(
        "Write",
        KafkaIO.<byte[], byte[]>write()
            .withBootstrapServers(options.getBootstrapServers())
            .withTopic(topic)
            .withProducerConfigUpdates(producerConfig)
            .withValueSerializer(ByteArraySerializer.class)
            .values());
  }

  static void writeJavaManaged(PCollection<Row> rows, Options options, String topic) {
    Map<String, String> producerConfig = managedKafkaAuth();
    producerConfig.putAll(parseProperties(options.getProducerProperties()));
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap_servers", options.getBootstrapServers());
    config.put("topic", topic);
    config.put("format", options.getFormat());
    config.put("producer_config_updates", producerConfig);
    PCollectionRowTuple.of("input", rows)
        .apply("Write", Managed.write(Managed.KAFKA).withConfig(config));
  }

  static void writeRust(PCollection<Row> rows, Options options, String topic) throws IOException {
    Schema.Builder schema =
        Schema.builder()
            .addStringField("bootstrap_servers")
            .addStringField("topic")
            .addStringField("format")
            .addBooleanField("with_gcp_adc");
    List<Object> values = new ArrayList<>();
    values.add(options.getBootstrapServers());
    values.add(topic);
    values.add(options.getFormat());
    values.add(true);
    Map<String, String> props = parseProperties(options.getProducerProperties());
    if (!props.isEmpty()) {
      schema.addMapField(
          "producer_config_updates", Schema.FieldType.STRING, Schema.FieldType.STRING);
      values.add(props);
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

  static void writeRows(PCollection<Row> rows, Options options, String topic) throws IOException {
    switch (String.valueOf(options.getWriter())) {
      case "java-native":
        writeJavaNative(rows, options, topic);
        break;
      case "java-managed":
        writeJavaManaged(rows, options, topic);
        break;
      case "rust":
        writeRust(rows, options, topic);
        break;
      default:
        throw new IllegalArgumentException(
            "--writer must be java-native, java-managed or rust");
    }
  }

  /** Counts each row in {@code load}, and converts to {@code RAW} when {@code rawMapper} is set. */
  static class TouchLoadFn extends DoFn<Row, Row> {
    private final Counter rows = Metrics.counter("e2e", "rows");
    private final SerializableFunction<Row, byte[]> rawMapper;

    TouchLoadFn(SerializableFunction<Row, byte[]> rawMapper) {
      this.rawMapper = rawMapper;
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> out) {
      rows.inc();
      if (rawMapper != null) {
        out.output(Row.withSchema(RAW_BYTES_SCHEMA).addValue(rawMapper.apply(row)).build());
      } else {
        out.output(row);
      }
    }
  }

  /**
   * Upper bounds (ms) of the latency buckets. {@code e2e/latency_le_<bound>} counts the rows with
   * a latency above the previous bound and at most this bound; {@code e2e/latency_le_inf} counts
   * the rest. Keep in sync with {@code LATENCY_BOUNDS_MS} in {@code kafka_benchmark.py} and in
   * {@code io_report.py}.
   */
  static final long[] LATENCY_BOUNDS_MS = {
    50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000
  };

  /**
   * Counts {@code e2e/rows} and records {@code e2e/latency_ms} from the record timestamp, as a
   * distribution and as bucket counters. It emits each element (a row, or the record bytes with
   * {@code BYTES}) with no change.
   */
  static class TouchCopyFn<T> extends DoFn<T, T> {
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
    public void process(@Element T element, @Timestamp Instant timestamp, OutputReceiver<T> out) {
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
   * Maps each sequence index of {@code produce} to a record of the sample file, in a cycle. The
   * record has no key and no timestamp, so the Kafka producer sets {@code CreateTime} when it
   * sends the record.
   */
  static class SampleRecordFn extends DoFn<Long, KV<byte[], byte[]>> {
    private final String samplePath;
    private final Counter rows = Metrics.counter("produce", "rows");
    private transient List<byte[]> records;

    SampleRecordFn(String samplePath) {
      this.samplePath = samplePath;
    }

    @Setup
    public void setup() throws IOException {
      List<byte[]> lines = new ArrayList<>();
      try (BufferedReader reader =
          new BufferedReader(
              Channels.newReader(
                  FileSystems.open(FileSystems.matchSingleFileSpec(samplePath).resourceId()),
                  StandardCharsets.UTF_8.name()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (!line.isEmpty()) {
            lines.add(line.getBytes(StandardCharsets.UTF_8));
          }
        }
      }
      if (lines.isEmpty()) {
        throw new IOException("No records in " + samplePath);
      }
      records = lines;
    }

    @ProcessElement
    public void process(@Element Long index, OutputReceiver<KV<byte[], byte[]>> out) {
      rows.inc();
      out.output(KV.of(null, records.get((int) (index % records.size()))));
    }
  }

  /** Writes {@code --rate} records per second from {@code --samplePath} to {@code --topic}. */
  static void produce(Pipeline p, Options options) {
    if (options.getSamplePath() == null || options.getSamplePath().isEmpty()) {
      throw new IllegalArgumentException("produce needs --samplePath");
    }
    if (options.getRate() <= 0) {
      throw new IllegalArgumentException("produce needs a --rate larger than zero");
    }
    p.apply(
            "Generate",
            GenerateSequence.from(0).withRate(options.getRate(), Duration.standardSeconds(1)))
        .apply("SampleRecords", ParDo.of(new SampleRecordFn(options.getSamplePath())))
        .setCoder(KvCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()))
        .apply(
            "Write",
            KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopic(options.getTopic())
                .withProducerConfigUpdates(new HashMap<>(managedKafkaAuth()))
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class));
  }

  static long committedRows(PipelineResult result) {
    try {
      MetricQueryResults metrics =
          result
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(MetricNameFilter.named("e2e", "rows"))
                      .build());
      long total = 0;
      for (MetricResult<Long> counter : metrics.getCounters()) {
        Long attempted = counter.getAttempted();
        if (attempted != null) {
          total += attempted;
        }
      }
      return total;
    } catch (Exception e) {
      LOG.warn("Could not query e2e/rows metric", e);
      return 0;
    }
  }

  static void drainDataflowJob(String project, String region, String jobId)
      throws IOException, InterruptedException {
    Process proc =
        new ProcessBuilder(
                "gcloud",
                "dataflow",
                "jobs",
                "drain",
                jobId,
                "--project=" + project,
                "--region=" + region)
            .inheritIO()
            .start();
    int code = proc.waitFor();
    if (code != 0) {
      throw new IOException("gcloud dataflow jobs drain exited with " + code);
    }
  }

  static void waitForStreamingJob(PipelineResult result, Options options)
      throws IOException, InterruptedException {
    DataflowPipelineJob job = (DataflowPipelineJob) result;
    String jobId = job.getJobId();
    System.out.println("JOB_ID=" + jobId);
    System.out.flush();
    if (options.getDetach()) {
      return;
    }
    Long expectedRows = options.getExpectedRows();
    Long drainAfterSeconds = options.getDrainAfterSeconds();
    long startMs = System.currentTimeMillis();
    boolean drained = false;
    while (true) {
      PipelineResult.State state = job.getState();
      long rows = committedRows(job);
      long elapsedSeconds = (System.currentTimeMillis() - startMs) / 1000;
      LOG.info("Job {}: state={} e2e/rows={} elapsed={}s", jobId, state, rows, elapsedSeconds);
      if (state.isTerminal()) {
        if (state != PipelineResult.State.DONE && !"DRAINED".equals(state.name())) {
          throw new IllegalStateException("Job " + jobId + " finished in state " + state);
        }
        if (expectedRows != null && rows < expectedRows) {
          throw new IllegalStateException(
              "Job " + jobId + " finished with " + rows + " rows, expected " + expectedRows);
        }
        return;
      }
      boolean shouldDrain =
          (expectedRows != null && rows >= expectedRows)
              || (drainAfterSeconds != null && elapsedSeconds >= drainAfterSeconds);
      if (shouldDrain && !drained && state == PipelineResult.State.RUNNING) {
        LOG.info("Draining job {} at e2e/rows={} ({}s)", jobId, rows, elapsedSeconds);
        drainDataflowJob(options.getProject(), options.getRegion(), jobId);
        drained = true;
      }
      Thread.sleep(15_000);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    String shape = options.getShape();
    String format = options.getFormat();
    if (!"load".equals(shape) && !"copy".equals(shape) && !"produce".equals(shape)) {
      throw new IllegalArgumentException("--shape must be load, copy or produce");
    }
    if (!"JSON".equals(format) && !"RAW".equals(format) && !"BYTES".equals(format)) {
      throw new IllegalArgumentException("--format must be JSON, RAW or BYTES");
    }
    if ("BYTES".equals(format)
        && (!"copy".equals(shape)
            || !"java-native".equals(options.getReader())
            || !"java-native".equals(options.getWriter()))) {
      throw new IllegalArgumentException(
          "--format=BYTES runs only the copy shape with --reader=java-native and"
              + " --writer=java-native: Managed I/O and the Rust expansion service exchange rows");
    }
    if (options.getBootstrapServers() == null || options.getBootstrapServers().isEmpty()) {
      throw new IllegalArgumentException("--bootstrapServers is required");
    }
    if (options.getTopic() == null || options.getTopic().isEmpty()) {
      throw new IllegalArgumentException("--topic is required");
    }
    if (!"load".equals(shape)) {
      options.setStreaming(true);
    }

    Pipeline p = Pipeline.create(options);
    if ("produce".equals(shape)) {
      produce(p, options);
      waitForStreamingJob(p.run(), options);
      return;
    }
    if ("load".equals(shape)) {
      if (options.getReader() != null) {
        throw new IllegalArgumentException("--reader applies to copy only, not to load");
      }
      PCollection<Row> bqRows = readBigQueryRows(p, options);
      boolean raw = "RAW".equals(format);
      SerializableFunction<Row, byte[]> rawMapper =
          raw ? JsonUtils.getRowToJsonBytesFunction(bqRows.getSchema()) : null;
      Schema outputSchema = raw ? RAW_BYTES_SCHEMA : bqRows.getSchema();
      PCollection<Row> touched =
          bqRows
              .apply("Touch", ParDo.of(new TouchLoadFn(rawMapper)))
              .setRowSchema(outputSchema);
      writeRows(touched, options, options.getTopic());
      PipelineResult result = p.run();
      PipelineResult.State state = result.waitUntilFinish();
      if (state != PipelineResult.State.DONE) {
        throw new IllegalStateException("The job finished in state " + state);
      }
      if (options.getExpectedRows() != null) {
        long rows = committedRows(result);
        if (rows != options.getExpectedRows()) {
          throw new IllegalStateException(
              "The job wrote " + rows + " rows, expected " + options.getExpectedRows());
        }
      }
    } else if ("BYTES".equals(format)) {
      if (options.getDestinationTopic() == null || options.getDestinationTopic().isEmpty()) {
        throw new IllegalArgumentException("copy needs --destinationTopic");
      }
      // KafkaIO bytes in, KafkaIO bytes out, and a byte[] step between them: no step builds a
      // row, decodes JSON or encodes JSON.
      PCollection<byte[]> touched =
          readJavaNativeValues(p, options)
              .apply("Touch", ParDo.of(new TouchCopyFn<byte[]>()))
              .setCoder(ByteArrayCoder.of());
      writeJavaNativeValues(touched, options, options.getDestinationTopic());
      PipelineResult result = p.run();
      waitForStreamingJob(result, options);
    } else {
      if (options.getDestinationTopic() == null || options.getDestinationTopic().isEmpty()) {
        throw new IllegalArgumentException("copy needs --destinationTopic");
      }
      PCollection<Row> rows;
      switch (String.valueOf(options.getReader())) {
        case "java-native":
          rows = readJavaNative(p, options);
          break;
        case "java-managed":
          rows = readJavaManaged(p, options);
          break;
        case "rust":
          rows = readRust(p, options);
          break;
        default:
          throw new IllegalArgumentException(
              "copy needs --reader: java-native, java-managed or rust");
      }
      PCollection<Row> touched =
          rows.apply("Touch", ParDo.of(new TouchCopyFn<Row>())).setRowSchema(rows.getSchema());
      writeRows(touched, options, options.getDestinationTopic());
      PipelineResult result = p.run();
      waitForStreamingJob(result, options);
    }
  }
}
