use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

use crate::parser;

pub fn run(file: &Path, output: Option<&Path>) -> Result<()> {
    let content = fs::read_to_string(file)
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    let spec = parser::parse_yaml(&content).with_context(|| "Failed to parse YAML")?;

    let ir = compile_to_ir(&spec)?;

    let output_json = serde_json::to_string_pretty(&ir)?;

    if let Some(output_path) = output {
        fs::write(output_path, &output_json)?;
        println!("Compiled to: {}", output_path.display());
    } else {
        println!("{}", output_json);
    }

    Ok(())
}

pub fn compile_to_ir(spec: &serde_json::Value) -> Result<serde_json::Value> {
    let ir = serde_json::json!({
        "api_version": spec.get("api_version"),
        "identity_version": spec.get("identity_version"),
        "entity": spec.get("entity").and_then(|e| e.get("name")),
        "sources": spec.get("sources").map(|s| {
            s.as_array().map(|arr| {
                arr.iter().map(|source| {
                    serde_json::json!({
                        "name": source.get("name"),
                        "system": source.get("system"),
                        "table": source.get("table"),
                    })
                }).collect::<Vec<_>>()
            })
        }),
        "rule_count": spec.get("rules").and_then(|r| r.as_array()).map(|a| a.len()),
        "blocking_strategy": spec.get("blocking").and_then(|b| b.get("strategy")),
        "thresholds": spec.get("decision").and_then(|d| d.get("thresholds")),
    });

    // Compute plan hash over the IR (without the hash itself)
    let canonical_json = serde_json::to_string(&ir)?;
    let mut hasher = Sha256::new();
    hasher.update(canonical_json.as_bytes());
    let hash = format!("sha256:{:x}", hasher.finalize());

    let mut ir_with_hash = ir;
    ir_with_hash["plan_hash"] = serde_json::Value::String(hash);

    Ok(ir_with_hash)
}
