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
package org.apache.beam.gradle.rust

import groovy.transform.CompileStatic
import org.gradle.api.GradleException
import org.gradle.api.Project

/** Iceberg benchmark on Dataflow ({@code icebergBenchmark}). */
@CompileStatic
class RustIcebergBenchmark {

  static void register(RustIoBenchmarkSupport support) {
    Project project = support.build.project

    support.xlang.registerDataflowExpansionImageTasks([
      buildTask       : 'buildIcebergBenchmarkExpansionServiceDataflow',
      imageTask       : 'icebergBenchmarkExpansionServiceDataflowImage',
      bin             : 'beam-iceberg-benchmark-expansion-service',
      imageName       : 'iceberg_benchmark_expansion_service',
      root            : support.ioTasks.connectorDataflowRoot,
      buildDescription: 'Build the linux/amd64 Kafka and Iceberg expansion service binary of icebergBenchmark for Dataflow',
      imageDescription: 'Build and push the linux/amd64 beam_rust_iceberg_benchmark_expansion_service image for Dataflow ([-PdataflowRepositoryRoot=<registry>])',
    ])

    support.registerDataflowBenchmark(new RustIoBenchmarkSupport.BenchmarkSpec(
        task: 'icebergBenchmark',
        compileTask: 'compileIcebergJavaBenchmark',
        dir: 'iceberg',
        label: 'iceberg',
        jobPrefix: 'iceberg',
        pythonScript: 'iceberg_benchmark.py',
        mainClass: 'IcebergBenchmark',
        counter: 'rows',
        counterShapes: ['ingest'],
        io: { String shape ->
          [
            namespace: (Object) 'e2e',
            rows     : (Object) 'rows',
            streaming: (Object) true,
            latency  : (Object) 'latency_ms',
            buckets  : (Object) 'latency_le_',
          ]
        },
        rustPackage: 'apache-beam-benchmark-iceberg',
        rustShapes: ['ingest'],
        expansionImageTask: 'icebergBenchmarkExpansionServiceDataflowImage',
        expansionImageName: 'iceberg_benchmark_expansion_service',
        defaultShape: 'ingest',
        shapes: ['ingest'],
        readShapes: [],
        writeShapes: ['ingest'],
        writers: [
          python: ['java', 'rust'],
          java  : ['java-native', 'rust'],
          rust  : ['rust'],
        ],
        hasProducer: { String shape -> support.kafkaProducerRequested() },
        producerDefaults: [rate: '100000', loadSeconds: '1800', idleAfterLoadSeconds: '270'],
        producer: { Map<String, Object> job ->
          String bootstrapServers = support.benchProp('bootstrapServers')
          String topic = (project.findProperty('topic') ?: 'device-metrics').toString()
          return support.kafkaProducerVariant(job, (File) job['kafkaJavaArgs'], [
            bootstrapServers: (Object) bootstrapServers,
            topic           : topic,
            format          : 'JSON',
            sampleKey       : 'icebergSamplePath',
          ])
        },
        description: 'Stream DeviceMetric records from Kafka to a BigLake Iceberg table on Dataflow ' +
            '([-Ppipeline=rust|java|python] -Pwriter=rust (Rust) or java-native|rust (Java) or java|rust ' +
            '(Python) [-PbootstrapServers=<host:port>] [-Ptopic=device-metrics] ' +
            '[-Pcatalog=<BigLake catalog>] [-Pnamespace=iceberg_bench] [-PtriggeringFrequency=60] ' +
            '[-PnumShards=<n>] [-PobjectStorage] [-PautoOffsetReset=earliest|latest] [-PgroupId=<id>] ' +
            '-PkafkaCluster=<cluster> (or example.benchmark.kafkaCluster) ' +
            '[-PsamplePath=<gs://...> [-Prate=100000] [-PloadSeconds=1800] [-PidleAfterLoadSeconds=270] ' +
            '[-PproducerExtraArgs="<producer flags>"] [-PsharedProducer=<id> [-PsharedProducerTasks=2]]] ' +
            '[-PexpectedRows=<n>] [-PdrainAfterSeconds=<s>] [-PkillWorkerAfterSeconds=<s>] ' +
            '[-PkeepTable] ' +
            '[-PnumWorkers=4] [-PmaxNumWorkers=<n>] [-PworkerMachineType=n2-standard-4] ' +
            '[-PextraArgs="<pipeline flags>"])',
        configure: { RustIoBenchmarkSupport.RunContext run ->
          boolean dryRun = support.benchmarks.isSet('dryRun')
          String bootstrapServers = support.benchProp('bootstrapServers') ?: (dryRun ? '<bootstrapServers>' : null)
          String catalog = (project.findProperty('catalog') ?: support.benchProp('icebergCatalog'))?.toString()
              ?: (dryRun ? '<catalog>' : null)
          if (!bootstrapServers || !catalog) {
            throw new GradleException('Set -PbootstrapServers and -Pcatalog (or ' +
                'example.benchmark.bootstrapServers and example.benchmark.icebergCatalog in ' +
                'sdks/rust/.local/sdk.properties).')
          }
          String namespace = (project.findProperty('namespace') ?: 'iceberg_bench').toString()
          String table = "${namespace}.ingest_{job_name_underscore}"
          String autoOffsetReset = (project.findProperty('autoOffsetReset') ?: 'earliest').toString()
          if (!['earliest', 'latest'].contains(autoOffsetReset)) {
            throw new GradleException("Unknown autoOffsetReset '${autoOffsetReset}': use earliest or latest.")
          }
          String frequency = (project.findProperty('triggeringFrequency') ?: '60').toString()
          String topic = (project.findProperty('topic') ?: 'device-metrics').toString()
          String kafkaCluster = support.benchProp('kafkaCluster') ?: (dryRun ? '<kafkaCluster>' : null)
          if (!kafkaCluster) {
            throw new GradleException('Set -PkafkaCluster or example.benchmark.kafkaCluster in ' +
                'sdks/rust/.local/sdk.properties: the Managed Kafka cluster of -PbootstrapServers.')
          }
          List<String> args = [
            "${run.flag('bootstrap_servers')}=${bootstrapServers}".toString(),
            "--topic=${topic}".toString(),
            "${run.flag('auto_offset_reset')}=${autoOffsetReset}".toString(),
            "--warehouse=bl://projects/${run.gcpProject}/catalogs/${catalog}".toString(),
            "--table=${table}".toString(),
            "${run.flag('user_project')}=${run.gcpProject}".toString(),
            "${run.flag('gcp_location')}=${run.region}".toString(),
            "${run.flag('triggering_frequency_seconds')}=${frequency}".toString(),
          ] + support.benchArgs(run, [groupId: 'group_id', numShards: 'num_shards'])
          if (support.benchmarks.isSet('objectStorage')) {
            args.add(run.flag('object_storage'))
          }
          return [
            catalog   : (Object) catalog,
            gcpProject: run.gcpProject,
            region    : run.region,
            table     : table,
            kafkaTopic: "${kafkaCluster}/${topic}".toString(),
            labels    : [frequency: "${frequency}s".toString(), offset: autoOffsetReset],
            args      : args,
          ]
        },
        after: { Map<String, Object> spec ->
          List<String> args = [
            "${support.benchVenv.absolutePath}/bin/python".toString(),
            "${support.build.rustDir.absolutePath}/benchmarks/tools/check_iceberg_table.py".toString(),
            "--table=${spec['table']}".toString(),
            "--catalog=${spec['catalog']}".toString(),
            "--project=${spec['gcpProject']}".toString(),
            "--region=${spec['region']}".toString(),
            "--kafka_topic=${spec['kafkaTopic']}".toString(),
            '--distinct_column=id,timestamp_ms',
          ]
          if (!support.benchmarks.isSet('keepTable')) {
            args.add('--drop')
          }
          return args
        }))
  }
}
