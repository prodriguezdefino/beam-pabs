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

/** Kafka benchmark on Dataflow ({@code kafkaBenchmark}). */
@CompileStatic
class RustKafkaBenchmark {

  static void register(RustIoBenchmarkSupport support) {
    Project project = support.build.project
    support.registerDataflowBenchmark(new RustIoBenchmarkSupport.BenchmarkSpec(
        task: 'kafkaBenchmark',
        compileTask: 'compileKafkaJavaBenchmark',
        dir: 'kafka',
        label: 'kafka',
        jobPrefix: 'kafka',
        pythonScript: 'kafka_benchmark.py',
        mainClass: 'KafkaBenchmark',
        counter: 'rows',
        counterShapes: ['load', 'copy', 'produce'],
        io: { String shape ->
          Map<String, Object> out = [
            namespace: (Object) (shape == 'produce' ? 'produce' : 'e2e'),
            rows     : (Object) 'rows',
            streaming: (Object) (shape != 'load'),
          ]
          if (shape == 'copy') {
            out['latency'] = 'latency_ms'
            out['buckets'] = 'latency_le_'
          }
          return out
        },
        rustPackage: 'apache-beam-benchmark-kafka',
        rustShapes: ['copy'],
        expansionImageTask: 'kafkaExpansionServiceDataflowImage',
        expansionImageName: 'kafka_expansion_service',
        defaultShape: 'load',
        shapes: ['load', 'copy', 'produce'],
        readShapes: ['copy'],
        writeShapes: ['load', 'copy'],
        readers: [
          python: ['java', 'rust'],
          java  : ['java-native', 'java-managed', 'rust'],
          rust  : ['rust'],
        ],
        writers: [
          python: ['java', 'rust'],
          java  : ['java-native', 'java-managed', 'rust'],
          rust  : ['rust'],
        ],
        hasProducer: { String shape -> shape == 'copy' && support.kafkaProducerRequested() },
        producer: { Map<String, Object> job ->
          String bootstrapServers = support.benchProp('bootstrapServers')
          String topic = project.findProperty('topic')?.toString()
          String format = (project.findProperty('format') ?: 'JSON').toString()
          return support.kafkaProducerVariant(job, (File) job['kafkaJavaArgs'], [
            bootstrapServers: (Object) bootstrapServers,
            topic           : topic,
            format          : format == 'BYTES' ? 'RAW' : format,
            sampleKey       : 'kafkaSamplePath',
          ])
        },
        description: 'Write a BigQuery table to Kafka (-Pshape=load), stream a Kafka topic to ' +
            'another topic (-Pshape=copy), or write records at a fixed rate (-Pshape=produce) on Dataflow ' +
            '([-Ppipeline=python|java|rust (copy only)] [-Pshape=load|copy|produce] ' +
            '[-Preader=java|rust (Python) or java-native|java-managed|rust (Java) or rust (Rust), copy only] ' +
            '-Pwriter=java|rust (Python) or java-native|java-managed|rust (Java) or rust (Rust) ' +
            '-PbootstrapServers=<host:port> -Ptopic=<topic> ' +
            '[-PdestinationTopic=<topic>, copy only] [-Pformat=JSON|RAW|BYTES] ' +
            '[-Ptable=<spec>, load only] [-ProwRestriction=<filter>, load only] ' +
            '[-Prate=<records/s> -PsamplePath=<file> (or example.benchmark.kafkaSamplePath), produce or copy companion] ' +
            '[-PloadSeconds=300 -PidleAfterLoadSeconds=300 -PproducerExtraArgs="<producer flags>" ' +
            '-PsharedProducer=<id> -PsharedProducerTasks=2, copy companion] ' +
            '[-PautoOffsetReset=earliest|latest] [-PgroupId=<id>] ' +
            '[-PexpectedRows=<n>] [-PdrainAfterSeconds=<s>] [-Pdetach] ' +
            '[-PnumWorkers=4] [-PmaxNumWorkers=<n>] [-PworkerMachineType=n2-standard-4] ' +
            '[-PrunnerV1, Java pipeline without Rust transforms only] ' +
            '[-PextraArgs="<pipeline flags>"])',
        configure: { RustIoBenchmarkSupport.RunContext run ->
          String bootstrapServers = support.benchProp('bootstrapServers')
          String topic = project.findProperty('topic')?.toString()
          if (!bootstrapServers || !topic) {
            throw new GradleException('Set -PbootstrapServers and -Ptopic (or ' +
                'example.benchmark.bootstrapServers in sdks/rust/.local/sdk.properties).')
          }
          String format = (project.findProperty('format') ?: 'JSON').toString()
          if (!['JSON', 'RAW', 'BYTES'].contains(format)) {
            throw new GradleException("Unknown format '${format}': use JSON, RAW or BYTES.")
          }
          Map<String, String> bytesVariants = [rust: 'rust', java: 'java-native']
          if (format == 'BYTES' && (run.shape != 'copy' || !bytesVariants.containsKey(run.pipeline) ||
              run.reader != bytesVariants[run.pipeline] || run.writer != bytesVariants[run.pipeline])) {
            throw new GradleException('-Pformat=BYTES runs only -Pshape=copy, with -Ppipeline=rust ' +
                '-Preader=rust -Pwriter=rust, or -Ppipeline=java -Preader=java-native -Pwriter=java-native.')
          }
          List<String> args = [
            "${run.flag('bootstrap_servers')}=${bootstrapServers}".toString(),
            "--topic=${topic}".toString(),
            "--format=${format}".toString(),
          ]
          Map<String, String> labels = [format: format.toLowerCase()]
          String readerLabel = null

          if (run.shape == 'produce') {
            if (run.pipeline != 'java') {
              throw new GradleException('-Pshape=produce runs in a Java pipeline only: add -Ppipeline=java.')
            }
            ['destinationTopic', 'autoOffsetReset', 'groupId', 'table', 'rowRestriction'].each { String prop ->
              if (project.hasProperty(prop)) {
                throw new GradleException("-P${prop} does not apply to -Pshape=produce.")
              }
            }
            String rate = project.findProperty('rate')?.toString()
            if (!rate) {
              throw new GradleException('-Pshape=produce needs -Prate=<records/s>.')
            }
            String samplePath = support.benchSamplePath('kafkaSamplePath')
            args.add("--rate=${rate}".toString())
            args.add("--samplePath=${samplePath}".toString())
            labels['rate'] = rate
            readerLabel = 'producer'
          } else if (run.shape == 'load') {
            ['destinationTopic', 'autoOffsetReset', 'groupId', 'drainAfterSeconds', 'detach'].each { String prop ->
              if (project.hasProperty(prop)) {
                throw new GradleException("-P${prop} applies to -Pshape=copy only, not to load.")
              }
            }
            String table = support.benchProp('table')
            if (!table) {
              throw new GradleException(
                  'Set -Ptable or example.benchmark.table in sdks/rust/.local/sdk.properties.')
            }
            args.add("--table=${table}".toString())
            String rowRestriction = project.findProperty('rowRestriction')?.toString()
            if (rowRestriction != null && !rowRestriction.isEmpty()) {
              args.add("${run.flag('row_restriction')}=${rowRestriction}".toString())
              labels['restricted'] = 'true'
            }
          } else {
            if (project.hasProperty('rowRestriction')) {
              throw new GradleException('-ProwRestriction applies to -Pshape=load only, not to copy.')
            }
            String destinationTopic = project.findProperty('destinationTopic')?.toString()
            if (!destinationTopic) {
              throw new GradleException('-Pshape=copy needs -PdestinationTopic.')
            }
            String autoOffsetReset = (project.findProperty('autoOffsetReset') ?: 'earliest').toString()
            if (!['earliest', 'latest'].contains(autoOffsetReset)) {
              throw new GradleException(
                  "Unknown autoOffsetReset '${autoOffsetReset}': use earliest or latest.")
            }
            args.add("${run.flag('destination_topic')}=${destinationTopic}".toString())
            args.add("${run.flag('auto_offset_reset')}=${autoOffsetReset}".toString())
            args.addAll(support.benchArgs(run, [groupId: 'group_id']))
            labels['offset'] = autoOffsetReset
          }
          return [
            readerLabel: (Object) readerLabel,
            labels     : labels,
            args       : args,
          ]
        }))
  }
}
