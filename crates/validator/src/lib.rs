//! Kanoniv identity spec validator — library API.
//!
//! This module re-exports the core validation, compilation, hashing,
//! and diffing functions for use by other Rust crates (including PyO3 bindings).

pub mod validator;
pub mod parser;
pub mod commands;

#[cfg(feature = "python")]
pub mod python;

// Re-export the primary public functions
pub use validator::{validate_schema, validate_semantics};
pub use parser::parse_yaml;
pub use commands::diff::{compute_diff, DiffResult, RuleChange};
pub use commands::compile::compile_to_ir;
pub use commands::plan::{generate_plan, PlanResult};

/// Convenience: validate a YAML string and return all errors.
pub fn validate_yaml(yaml: &str) -> anyhow::Result<Vec<String>> {
    let spec = parse_yaml(yaml)?;
    let mut errors = validate_schema(&spec)?;
    errors.extend(validate_semantics(&spec)?);
    Ok(errors)
}
