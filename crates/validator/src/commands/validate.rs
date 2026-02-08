use anyhow::{Context, Result};
use colored::Colorize;
use std::fs;
use std::path::Path;

use crate::parser;
use crate::validator;

pub fn run(file: &Path, format: &str) -> Result<()> {
    // Read file
    let content = fs::read_to_string(file)
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    // Parse YAML
    let spec = parser::parse_yaml(&content).with_context(|| "Failed to parse YAML")?;

    // Validate schema
    let schema_errors = validator::validate_schema(&spec)?;
    if !schema_errors.is_empty() {
        if format == "json" {
            println!("{}", serde_json::to_string_pretty(&schema_errors)?);
        } else {
            eprintln!("{} Schema validation failed:", "✗".red().bold());
            for error in &schema_errors {
                eprintln!("  {} {}", "→".red(), error);
            }
        }
        return Err(anyhow::anyhow!("{} schema error(s)", schema_errors.len()));
    }

    if format == "text" {
        println!("{} Schema valid", "✓".green().bold());
    }

    // Validate semantics
    let semantic_errors = validator::validate_semantics(&spec)?;
    if !semantic_errors.is_empty() {
        if format == "json" {
            println!("{}", serde_json::to_string_pretty(&semantic_errors)?);
        } else {
            eprintln!("{} Semantic validation failed:", "✗".red().bold());
            for error in &semantic_errors {
                eprintln!("  {} {}", "→".red(), error);
            }
        }
        return Err(anyhow::anyhow!(
            "{} semantic error(s)",
            semantic_errors.len()
        ));
    }

    if format == "text" {
        println!("{} Semantic checks passed", "✓".green().bold());
        println!("{} {} is valid", "✓".green().bold(), file.display());
    } else {
        println!(r#"{{"valid": true, "errors": []}}"#);
    }

    Ok(())
}
