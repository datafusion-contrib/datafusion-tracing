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

use comfy_table::{Cell, ContentArrangement, Table};
use datafusion::arrow::{
    array::RecordBatch,
    error::ArrowError,
    util::display::{ArrayFormatter, FormatOptions},
};
use std::fmt::Display;
use unicode_width::UnicodeWidthStr;

const DEFAULT_PRESET: &str = "||--|=+||-+||++++++";
const TRUNCATED_PRESET: &str = "|…--|=+…|-+|…+++…+…";

/// Formats a `RecordBatch` as a neatly aligned ASCII table,
/// constraining the total width to `max_width`. Columns are
/// dynamically resized or truncated, and columns that cannot
/// fit within the given width may be dropped.
pub fn pretty_format_compact_batch(
    batch: &RecordBatch,
    max_width: usize,
    max_row_height: usize,
    min_compacted_col_width: usize,
) -> Result<impl Display, ArrowError> {
    let schema = batch.schema();
    let total_fields = schema.fields().len();
    let format_opts = FormatOptions::default().with_display_error(true);

    // Initialize header and column formatters
    let header: Vec<Cell> = schema
        .fields()
        .iter()
        .map(|f| Cell::new(f.name()))
        .collect();
    let formatters: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| ArrayFormatter::try_new(col.as_ref(), &format_opts))
        .collect::<Result<_, ArrowError>>()?;

    // Generate the 2d array of formatted values
    let formatted_values: Vec<Vec<Cell>> = (0..batch.num_rows())
        .map(|row_idx| {
            formatters
                .iter()
                .map(|fmt| Cell::new(fmt.value(row_idx)))
                .collect()
        })
        .collect();

    // Compute column widths as the maximum width of each cell for that column, header included
    let mut column_widths = vec![0; total_fields];
    for row in std::iter::once(&header).chain(formatted_values.iter()) {
        for (col_idx, cell) in row.iter().enumerate() {
            let cell_width = cell.content().width() + 3; // +3 for left separator + left padding + right padding
            column_widths[col_idx] = column_widths[col_idx].max(cell_width);
        }
    }

    // Count how many columns fit within the maximum table width constraint
    let nb_displayed_columns = if max_width == 0 {
        // no constraint: all columns fit
        total_fields
    } else {
        let mut table_width = 1; // initial width of 1 for the rightmost table separator
        let mut fit_columns = 0; // number of columns that fit within max_width: the remaining columns will be dropped
        for width in column_widths {
            let col_width = width.min(min_compacted_col_width).max(4); // lower bound of 4 for each column width: left separator + left padding + data + right padding
            if table_width + col_width > max_width {
                break;
            }
            table_width += col_width;
            fit_columns += 1;
        }
        fit_columns
    };

    // Adjust the preset for eventual truncated columns
    let table_preset = if nb_displayed_columns == total_fields {
        DEFAULT_PRESET
    } else {
        TRUNCATED_PRESET
    };

    // Build the final adjusted table
    let mut table = Table::new();
    table
        .force_no_tty()
        .load_preset(table_preset)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(header.into_iter().take(nb_displayed_columns));

    // Ellipsis truncation indicator requires comfy-table >= 7.1.4.
    // Arrow currently pins comfy-table to 7.1.2 to preserve its MSRV
    // (comfy-table 7.2.0 bumped MSRV to Rust 1.85 while Arrow remains at 1.84).
    // See https://github.com/apache/arrow-rs/issues/8243 and https://github.com/apache/arrow-rs/pull/8244.
    // Arrow chose an exact pin instead of a `~7.1` requirement; the latter would
    // also preserve MSRV while allowing 7.1.x (including 7.1.4).
    // Re-enable once Arrow relaxes this pin to allow >= 7.1.4.
    //table.set_truncation_indicator("…");

    for formatted_row in formatted_values {
        table.add_row(formatted_row.into_iter().take(nb_displayed_columns));
    }

    // Apply row height and table width constraints if provided
    if max_row_height > 0 {
        for row in table.row_iter_mut() {
            row.max_height(max_row_height);
        }
    }

    if max_width > 0 {
        table.set_width(max_width as u16);
    }

    Ok(table.to_string())
}

#[cfg(test)]
mod tests {
    use super::pretty_format_compact_batch;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::record_batch::RecordBatch;
    use insta::{Settings, assert_snapshot};
    use std::sync::Arc;

    fn insta_settings() -> Settings {
        let mut settings = Settings::clone_current();

        settings.set_prepend_module_to_snapshot(false);

        settings
    }

    #[test]
    fn test_pretty_format_no_constraints() -> Result<(), ArrowError> {
        assert_formatting(0, 0, 0, "pretty_format_no_constraints")
    }
    #[test]
    fn test_pretty_format_table_width() -> Result<(), ArrowError> {
        assert_formatting(25, 3, 0, "pretty_format_table_width")
    }
    #[test]
    fn test_pretty_format_all_constraints_narrow() -> Result<(), ArrowError> {
        assert_formatting(25, 3, 12, "pretty_format_all_constraints_narrow")
    }
    #[test]
    fn test_pretty_format_all_constraints_wide() -> Result<(), ArrowError> {
        assert_formatting(76, 3, 12, "pretty_format_all_constraints_wide")
    }

    fn assert_formatting(
        max_width: usize,
        max_row_height: usize,
        min_compacted_col_width: usize,
        test_name: &str,
    ) -> Result<(), ArrowError> {
        let batch = create_sample_batch();
        let result = pretty_format_compact_batch(
            &batch,
            max_width,
            max_row_height,
            min_compacted_col_width,
        )?
        .to_string();

        insta_settings().bind(|| assert_snapshot!(test_name, result));

        Ok(())
    }

    fn create_sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("country", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("city_name", DataType::Utf8, false),
            Field::new("emojis", DataType::Utf8, false),
            Field::new("chinese name", DataType::Utf8, false),
            Field::new("pop_count", DataType::Int64, false),
        ]));

        let a = Arc::new(Int64Array::from(vec![1, 3, 7]));
        let country =
            Arc::new(StringArray::from(vec!["France", "United Kingdom", "Spain"]));
        let description = Arc::new(StringArray::from(vec![
            "Paris is renowned as the City of Light, celebrated for its rich history, magnificent architecture, and vibrant arts scene. The city boasts iconic landmarks such as the Eiffel Tower and the Louvre, along with charming streets, quaint cafés, and a deep cultural heritage that continues to inspire artists, writers, and travelers from around the world.",
            "London is a dynamic and cosmopolitan metropolis that seamlessly blends its storied past with modern innovation. The city offers an array of historical sites, diverse neighborhoods, and world-class museums and theaters. Its bustling markets, green parks, and ever-evolving cultural scene make London a hub of creativity, commerce, and community life.",
            "Barcelona is a lively coastal city known for its striking modernist architecture, Mediterranean beaches, and eclectic cultural offerings. From the whimsical creations of Antoni Gaudí to the vibrant street life and renowned culinary delights, Barcelona captivates visitors with its unique blend of historic charm and contemporary energy.",
        ]));
        let city_name = Arc::new(StringArray::from(vec!["Paris", "London", "Barcelona"]));
        let emojis = Arc::new(StringArray::from(vec![
            "🗼🥖🍷🎨🚲🏰🌟",
            "🇬🇧🚕🏰🎡🎩☕",
            "🌞🎨⚽🍴🎉🌊",
        ]));
        let chinese_name = Arc::new(StringArray::from(vec!["巴黎", "倫敦", "巴塞隆納"]));
        let pop_count = Arc::new(Int64Array::from(vec![321, 987654, 2]));

        RecordBatch::try_new(
            schema,
            vec![
                a,
                country,
                description,
                city_name,
                emojis,
                chinese_name,
                pop_count,
            ],
        )
        .unwrap()
    }
}
