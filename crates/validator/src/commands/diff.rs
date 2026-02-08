use anyhow::{Context, Result};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DiffResult {
    pub rules_added: Vec<String>,
    pub rules_removed: Vec<String>,
    pub rules_modified: Vec<RuleChange>,
    pub thresholds_changed: bool,
    pub summary: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RuleChange {
    pub name: String,
    pub field: String, // "weight", "threshold", etc.
    pub old_value: String,
    pub new_value: String,
}

pub fn run(file1: &Path, file2: &Path) -> Result<()> {
    // Read files
    let content1 = fs::read_to_string(file1)
        .with_context(|| format!("Failed to read file: {}", file1.display()))?;
    let content2 = fs::read_to_string(file2)
        .with_context(|| format!("Failed to read file: {}", file2.display()))?;

    let diff = compute_diff(&content1, &content2)?;

    // Print readable output (preserving CLI behavior)
    println!(
        "{} {} vs {}",
        "Comparing:".bold(),
        file1.display(),
        file2.display()
    );
    println!();

    if !diff.rules_added.is_empty() {
        println!("{}:", "Rules Added".green());
        for r in &diff.rules_added {
            println!("  + {}", r);
        }
    }

    if !diff.rules_removed.is_empty() {
        println!("{}:", "Rules Removed".red());
        for r in &diff.rules_removed {
            println!("  - {}", r);
        }
    }

    if !diff.rules_modified.is_empty() {
        println!("{}:", "Rules Modified".yellow());
        for m in &diff.rules_modified {
            println!("  ~ {} ({} changed from {} to {})", m.name, m.field, m.old_value, m.new_value);
        }
    }

    if diff.thresholds_changed {
        println!("{}:", "Thresholds".cyan());
        println!("  {} Thresholds have changed.", "⚠".yellow());
    }

    if diff.rules_added.is_empty() && diff.rules_removed.is_empty() && diff.rules_modified.is_empty() && !diff.thresholds_changed {
        println!("No significant changes detected.");
    }

    Ok(())
}

pub fn compute_diff(content1: &str, content2: &str) -> Result<DiffResult> {
    let spec1: serde_json::Value = serde_yaml::from_str(content1)?;
    let spec2: serde_json::Value = serde_yaml::from_str(content2)?;

    let mut diff = DiffResult::default();

    // Compare identity versions (minor, included in summary)
    let v1 = spec1.get("identity_version").and_then(|v| v.as_str()).unwrap_or("unknown");
    let v2 = spec2.get("identity_version").and_then(|v| v.as_str()).unwrap_or("unknown");

    // Compare rules
    let rules1 = spec1.get("rules").and_then(|r| r.as_array());
    let rules2 = spec2.get("rules").and_then(|r| r.as_array());

    if let (Some(r1), Some(r2)) = (rules1, rules2) {
        // Collect names
        let names1: Vec<&str> = r1.iter().filter_map(|r| r.get("name").and_then(|n| n.as_str())).collect();
        let names2: Vec<&str> = r2.iter().filter_map(|r| r.get("name").and_then(|n| n.as_str())).collect();

        for name in &names2 {
            if !names1.contains(name) {
                diff.rules_added.push(name.to_string());
            } else {
                // Check for modifications
                let rule1 = r1.iter().find(|r| r.get("name").and_then(|n| n.as_str()) == Some(name)).unwrap();
                let rule2 = r2.iter().find(|r| r.get("name").and_then(|n| n.as_str()) == Some(name)).unwrap();

                // Compare weight
                if rule1.get("weight") != rule2.get("weight") {
                    diff.rules_modified.push(RuleChange {
                        name: name.to_string(),
                        field: "weight".to_string(),
                        old_value: rule1.get("weight").map(|v| v.to_string()).unwrap_or_default(),
                        new_value: rule2.get("weight").map(|v| v.to_string()).unwrap_or_default(),
                    });
                }
            }
        }

        for name in &names1 {
            if !names2.contains(name) {
                diff.rules_removed.push(name.to_string());
            }
        }
    }

    // Compare thresholds
    let t1 = spec1.get("decision").and_then(|d| d.get("thresholds"));
    let t2 = spec2.get("decision").and_then(|d| d.get("thresholds"));
    if t1 != t2 {
        diff.thresholds_changed = true;
    }

    diff.summary = format!(
        "Diff: {} added, {} removed, {} modified. Thresholds changed: {}. Version: {} -> {}",
        diff.rules_added.len(),
        diff.rules_removed.len(),
        diff.rules_modified.len(),
        diff.thresholds_changed,
        v1, v2
    );

    Ok(diff)
}
