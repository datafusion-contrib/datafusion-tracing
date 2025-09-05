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

set -euo pipefail

# Generate TPCH Parquet data at scale factor 0.1 into integration-utils/data
# Requires: tpchgen-cli in PATH (install with: cargo install tpchgen-cli)

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$ROOT_DIR/integration-utils/data"

mkdir -p "$OUT_DIR"

if ! command -v tpchgen-cli >/dev/null 2>&1; then
  echo "Error: tpchgen-cli not found in PATH. Install it with: cargo install tpchgen-cli" >&2
  exit 1
fi

echo "Generating TPCH Parquet data (SF=0.1) into $OUT_DIR"

# Generate all tables, single file per table, Parquet format
tpchgen-cli --scale-factor 0.1 --format parquet --output-dir "$OUT_DIR"

echo "TPCH Parquet generation complete. Files in $OUT_DIR:"
ls -1 "$OUT_DIR" | sed 's/^/ - /'


