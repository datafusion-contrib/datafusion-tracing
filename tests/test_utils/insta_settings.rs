// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

use insta::Settings;

pub fn settings() -> Settings {
    let mut settings = Settings::clone_current();
    // Replace timestamps like 2025-04-09 14:58:57.542308 UTC
    // with 1970-01-01 00:00:00 UTC.
    settings.add_filter(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)? UTC",
        "1970-01-01 00:00:00 UTC",
    );

    // Replace timestamps like 2025-04-09T14:58:57.542308781Z
    // with 1970-01-01T00:00:00Z.
    settings.add_filter(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z",
        "1970-01-01T00:00:00Z",
    );

    // Replace e-tags like e_tag: Some(\"3632ae8a-631df76d066c5-b7d\")
    // with e_tag: Some(\"ffffffff-fffffffffffff-fff\").
    settings.add_filter(
        r#"e_tag: Some\(\\"([0-9a-fA-F-]+)\\"\)"#,
        r#"e_tag: Some\("ffffffff-fffffffffffff-fff"\)"#,
    );

    // Replace durations like 1.18ms or 523µs
    // with 0.00ms.
    settings.add_filter(r"\d+(\.\d+)?(ns|µs|ms|s|m|h)", "0.00ms");

    // Redact environment dependent paths to parquet files
    // from: "object_store.location": "usr/home/user.name/workspace/datafusion-tracing/integration/data/nation.parquet"
    // to  : "object_store.location": "<TPCH_TABLES_DIR>/nation.parquet"
    let tpch_tables_dir = integration_utils::tpch_tables_dir()
        .to_string_lossy()
        .trim_start_matches(std::path::is_separator)
        .to_string();
    settings.add_filter(tpch_tables_dir.as_str(), "<TPCH_TABLES_DIR>");

    settings.set_prepend_module_to_snapshot(false);
    settings.set_sort_maps(true);

    settings
}

pub fn preview_redacted_settings() -> Settings {
    let mut settings = Settings::clone_current();

    settings.add_filter(
        r#""datafusion.preview": .*,"#,
        r#""datafusion.preview": "<redacted>","#,
    );

    settings
}
