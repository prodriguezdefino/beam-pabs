###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

# Custom container images for the wordcount example.
#
# BASE_IMAGE is the Rust SDK image (container/Dockerfile), which holds only the
# `boot` launcher. The build context is the directory holding the Linux
# `wordcount` binary. `./gradlew :sdks:rust:prebakedImage -Pexample=wordcount`
# builds that binary and passes both in.
#
# Both stages keep the base image's ENTRYPOINT, /opt/apache/beam/boot, which
# runs the binary baked in at /opt/apache/beam/worker_binary.

ARG BASE_IMAGE

# worker: the SDK image with the pipeline binary pre-baked, so no binary has to
# be staged per job.
FROM ${BASE_IMAGE} AS worker
COPY wordcount /opt/apache/beam/worker_binary

# flex: the worker image plus Google's Go Flex Template launcher, so the same
# image serves as the Flex Template launcher and as the SDK worker. The launcher
# runs the binary named by FLEX_TEMPLATE_GO_BINARY to submit the job.
FROM worker AS flex
COPY --from=gcr.io/dataflow-templates-base/go-template-launcher-base:latest \
    /opt/google/dataflow/go_template_launcher \
    /opt/google/dataflow/go_template_launcher
ENV FLEX_TEMPLATE_GO_BINARY=/opt/apache/beam/worker_binary
