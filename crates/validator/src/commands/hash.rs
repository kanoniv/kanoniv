use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

pub fn run(file: &Path) -> Result<()> {
    // Read file
    let content = fs::read_to_string(file)
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    // Parse YAML to JSON for canonical representation
    let spec: serde_json::Value =
        serde_yaml::from_str(&content).with_context(|| "Failed to parse YAML")?;

    // Convert to canonical JSON (sorted keys, no whitespace variation)
    let canonical = serde_json::to_string(&spec)?;

    // Compute SHA-256 hash
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let hash = format!("sha256:{:x}", hasher.finalize());

    println!("{}", hash);

    Ok(())
}
