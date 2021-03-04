# Copyright 2017 Heptio Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM rubiklabs/base:alpine3
LABEL maintainer="Rubik Labs <labs@rubik.ai>"

WORKDIR /app
RUN apk update --no-cache && apk add ca-certificates
COPY zstd_cgo.go /root/go/pkg/mod/github.com/apache/pulsar-client-go@v0.3.0/pulsar/internal/compression/zstd_cgo.go
ADD eventrouter /app/
USER nobody:nobody

CMD ["/bin/sh", "-c", "/app/eventrouter -v 3 -logtostderr"]
