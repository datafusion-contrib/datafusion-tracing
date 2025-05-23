#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# This script runs all the Rust lints locally the same way the
# DataFusion CI does

set -e
if ! command -v taplo &> /dev/null; then
    echo "Installing taplo using cargo"
    cargo install taplo-cli
fi

ci/scripts/hawkeye_fmt.sh
ci/scripts/readme_sync.sh
ci/scripts/rust_clippy.sh
ci/scripts/rust_docs.sh
ci/scripts/rust_fmt.sh
ci/scripts/rust_toml_fmt.sh
