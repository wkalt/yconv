# Test Coverage Analysis for yconv

This document provides an analysis of the current test coverage in the yconv codebase and identifies areas where test coverage should be improved.

## Executive Summary

The codebase has **228 tests across 21 files**, which provides reasonable coverage for core parsing and deserialization logic. However, several critical areas lack adequate testing, particularly around I/O operations, format conversions, and CLI functionality.

## Current Test Coverage by Module

### Well-Tested Modules (Good Coverage)

| Module | Test Count | Assessment |
|--------|-----------|------------|
| `ros1/msg_parser.rs` | 41 | Comprehensive parser testing for ROS1 message definitions |
| `ros1/deserializer.rs` | 37 | Thorough primitive, array, nested message, and error handling tests |
| `ros1/to_arrow.rs` | 27 | Good coverage for ROS1 to Arrow conversion |
| `schema/evolution.rs` | 21 | Excellent schema evolution and type widening tests |
| `arrow/builder.rs` | 19 | Solid Arrow RecordBatch construction tests |
| `mcap/reader.rs` | 12 | Reasonable MCAP reading tests |
| `cdr/deserializer.rs` | 11 | Basic CDR deserialization with alignment tests |

### Under-Tested Modules (Needs Improvement)

| Module | Test Count | Assessment |
|--------|-----------|------------|
| `ros1/transcoder.rs` | 9 | Large file (~37KB), needs more edge case testing |
| `ros1/writer.rs` | 7 | Limited serialization round-trip tests |
| `ros1/from_arrow.rs` | 7 | Minimal Arrow to ROS1 conversion tests |
| `arrow/sink.rs` | 6 | Basic tests, needs more error handling coverage |
| `arrow/source.rs` | 5 | Limited source operation tests |
| `cdr/transcoder.rs` | 5 | CDR noted as "has bugs" in README; needs more tests |
| `mcap/cdr_reader.rs` | 3 | Very limited CDR MCAP reading tests |

### Critically Under-Tested Modules (High Priority)

| Module | Test Count | Priority | Impact |
|--------|-----------|----------|--------|
| `output/parquet.rs` | 0 | **Critical** | No tests for Parquet output writer |
| `output/vortex.rs` | 0 | **Critical** | No tests for Vortex output writer |
| `output/duckdb.rs` | 2 | **High** | Minimal DuckDB output tests |
| `input/parquet.rs` | 0 | **Critical** | No tests for Parquet input reader |
| `input/vortex.rs` | 0 | **Critical** | No tests for Vortex input reader |
| `input/duckdb.rs` | 1 | **High** | Single test for DuckDB input |
| `lance/writer.rs` | 1 | **High** | Single test for LanceDB writer (supports cloud!) |
| `lance/reader.rs` | 2 | **High** | Minimal LanceDB to MCAP conversion tests |
| `protobuf/schema.rs` | 1 | **High** | Single test for Protobuf schema parsing |
| `protobuf/transcoder.rs` | 0 | **Critical** | No tests for Protobuf transcoding |
| `protobuf/writer.rs` | 1 | **High** | Single test for Protobuf message writing |
| `main.rs` | 0 | **High** | No CLI integration tests |
| `shell/` | 0 | **Medium** | No interactive shell tests |

---

## Recommended Test Improvements

### 1. Output Format Writers (Critical Priority)

**Current State:** The `output/parquet.rs`, `output/vortex.rs` modules have zero tests.

**Recommended Tests:**
```rust
// src/output/parquet.rs - Add tests for:
mod tests {
    // Basic functionality
    - test_create_parquet_output()
    - test_write_single_batch()
    - test_write_multiple_batches()
    - test_finish_closes_file()

    // Topic handling
    - test_multiple_topics_create_separate_files()
    - test_duplicate_topic_returns_error()
    - test_topic_name_sanitization()

    // Schema handling
    - test_complex_nested_schema()
    - test_all_primitive_types()
    - test_array_types()

    // Error handling
    - test_write_to_readonly_directory()
    - test_write_with_mismatched_schema()
}
```

### 2. Input Format Readers (Critical Priority)

**Current State:** `input/parquet.rs` and `input/vortex.rs` have no tests.

**Recommended Tests:**
```rust
// src/input/parquet.rs - Add tests for:
mod tests {
    - test_read_parquet_file()
    - test_list_tables()
    - test_read_all_primitive_types()
    - test_read_nested_structs()
    - test_read_arrays()
    - test_invalid_file_error()
    - test_corrupted_file_error()
}
```

### 3. End-to-End Integration Tests (Critical Priority)

**Current State:** No integration tests exist.

**Create new file: `tests/integration_tests.rs`**
```rust
mod tests {
    // Round-trip conversion tests
    - test_mcap_to_parquet_and_back()
    - test_mcap_to_lancedb_and_back()
    - test_mcap_to_duckdb_and_back()
    - test_mcap_to_vortex_and_back()

    // Format comparison tests
    - test_data_preserved_across_formats()
    - test_schema_preserved_across_formats()

    // Multi-file input tests
    - test_multiple_mcap_files_to_single_output()

    // Schema evolution tests
    - test_append_with_new_field()
    - test_append_with_widened_type()
}
```

### 4. LanceDB Integration (High Priority)

**Current State:** `lance/writer.rs` has only 1 test, `lance/reader.rs` has 2 tests.

**Recommended Tests:**
```rust
// src/lance/writer.rs - Add tests for:
mod tests {
    // Write modes
    - test_write_mode_error_if_exists()
    - test_write_mode_overwrite()
    - test_write_mode_append()

    // Schema evolution during append
    - test_append_with_schema_evolution()
    - test_append_incompatible_schema_error()

    // Multi-topic handling
    - test_multiple_topics()
    - test_topic_name_sanitization()

    // Error handling
    - test_write_invalid_batch()
}

// src/lance/reader.rs - Add tests for:
mod tests {
    - test_read_single_table()
    - test_read_multiple_tables()
    - test_reconstruct_mcap_messages()
    - test_timestamp_reconstruction()
    - test_empty_table()
}
```

### 5. Protobuf Support (High Priority)

**Current State:** Protobuf modules have minimal testing (1-0 tests each).

**Recommended Tests:**
```rust
// src/protobuf/schema.rs - Add tests for:
mod tests {
    - test_parse_simple_message()
    - test_parse_nested_message()
    - test_parse_repeated_fields()
    - test_parse_map_fields()
    - test_parse_oneof_fields()
    - test_parse_enums()
    - test_arrow_schema_generation()
    - test_invalid_schema_error()
}

// src/protobuf/transcoder.rs - Add tests for:
mod tests {
    - test_transcode_primitives()
    - test_transcode_nested_messages()
    - test_transcode_repeated_fields()
    - test_transcode_bytes_field()
}
```

### 6. CDR/ROS2 Support (High Priority)

**Current State:** CDR is noted as "has bugs" in README; testing is minimal.

**Recommended Tests:**
```rust
// src/cdr/transcoder.rs - Add tests for:
mod tests {
    - test_transcode_with_alignment()
    - test_transcode_nested_with_padding()
    - test_transcode_variable_length_arrays()
    - test_transcode_fixed_length_arrays()
    - test_encapsulation_header_handling()

    // Known bug scenarios
    - test_complex_nested_alignment()
    - test_string_with_alignment()
}

// src/mcap/cdr_reader.rs - Add tests for:
mod tests {
    - test_read_cdr_message()
    - test_read_multiple_topics()
    - test_schema_extraction()
}
```

### 7. CLI Integration Tests (High Priority)

**Current State:** No CLI tests exist.

**Create new file: `tests/cli_tests.rs`**
```rust
mod tests {
    // Convert command
    - test_convert_mcap_to_parquet()
    - test_convert_mcap_to_lancedb()
    - test_convert_with_stdout()
    - test_convert_multiple_inputs()
    - test_convert_with_overwrite()
    - test_convert_with_append()

    // Analyze command
    - test_analyze_mcap()
    - test_analyze_with_clean()

    // Error handling
    - test_missing_input_file()
    - test_invalid_output_format()
    - test_conflicting_options()
}
```

### 8. Error Handling Tests (Medium Priority)

Many modules lack comprehensive error handling tests.

**Add to existing test modules:**
```rust
// Pattern for each module:
mod error_handling {
    - test_eof_during_read()
    - test_invalid_data()
    - test_missing_required_field()
    - test_type_mismatch()
    - test_unsupported_encoding()
}
```

### 9. Property-Based Testing (Medium Priority)

Consider adding property-based tests using `proptest` or `quickcheck` for:

```rust
// Example with proptest:
proptest! {
    #[test]
    fn test_ros1_round_trip(data: Vec<u8>) {
        // Serialize and deserialize should be identity
    }

    #[test]
    fn test_schema_evolution_commutative(schema1: Schema, schema2: Schema) {
        // Order of schema comparison shouldn't matter for compatibility
    }
}
```

---

## Testing Infrastructure Recommendations

### 1. Add Test Fixtures

Create a `tests/fixtures/` directory with sample data files:
```
tests/fixtures/
├── mcap/
│   ├── ros1_simple.mcap
│   ├── ros1_complex.mcap
│   ├── protobuf_simple.mcap
│   └── cdr_simple.mcap
├── parquet/
│   └── sample.parquet
└── schemas/
    ├── simple.msg
    └── complex.proto
```

### 2. Add Test Helpers

Create shared test utilities in `src/test_utils.rs` (gated by `#[cfg(test)]`):
```rust
#[cfg(test)]
pub mod test_utils {
    pub fn create_test_recordbatch() -> RecordBatch { ... }
    pub fn create_temp_mcap() -> TempFile { ... }
    pub fn assert_recordbatch_equal(a: &RecordBatch, b: &RecordBatch) { ... }
}
```

### 3. Add CI Coverage Reporting

Add `cargo-tarpaulin` or `llvm-cov` to CI for coverage tracking:
```yaml
# In CI config:
- name: Generate coverage
  run: cargo tarpaulin --out Xml
```

---

## Priority Order for Implementation

1. **Immediate (Week 1):**
   - Output format writers (Parquet, Vortex)
   - Input format readers (Parquet, Vortex)
   - Basic integration tests

2. **Short-term (Week 2-3):**
   - LanceDB integration tests
   - Protobuf module tests
   - CDR transcoder tests (fix known bugs)

3. **Medium-term (Week 4+):**
   - CLI integration tests
   - Shell tests
   - Property-based testing
   - Error handling coverage

---

## Summary

The codebase has solid testing for core parsing logic (ROS1 message parsing, deserialization) but lacks coverage in:

1. **I/O operations** - Input/output format handlers have almost no tests
2. **Integration testing** - No end-to-end conversion tests
3. **Alternative encodings** - Protobuf and CDR have minimal testing
4. **CLI functionality** - Main application entry points are untested

Addressing these gaps will significantly improve reliability, especially for the I/O paths that handle real-world data conversion workflows.
