use anyhow::{Context, Result};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

use crate::parser;

// ── Types ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct PlanResult {
    pub entity: String,
    pub identity_version: String,
    pub plan_hash: String,
    pub sources: Vec<PlanSource>,
    pub execution_stages: Vec<ExecutionStage>,
    pub match_strategies: Vec<MatchStrategySummary>,
    pub survivorship_summary: Vec<SurvivorshipSummary>,
    pub blocking_analysis: BlockingAnalysis,
    pub risk_flags: Vec<RiskFlag>,
    pub summary: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlanSource {
    pub name: String,
    pub system: String,
    pub field_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionStage {
    pub stage: usize,
    pub name: String,
    pub description: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MatchStrategySummary {
    pub rule_name: String,
    pub match_type: String,
    pub field: String,
    pub algorithm: Option<String>,
    pub threshold: Option<f64>,
    pub weight: f64,
    pub evaluation_order: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SurvivorshipSummary {
    pub field: String,
    pub strategy: String,
    pub source_priority: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockingAnalysis {
    pub strategy: String,
    pub keys: Vec<BlockingKeySummary>,
    pub estimated_reduction: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockingKeySummary {
    pub name: String,
    pub transformation: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RiskFlag {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub recommendation: String,
}

// ── CLI entry point ────────────────────────────────────────────────

pub fn run(file: &Path) -> Result<()> {
    let content = fs::read_to_string(file)
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    let plan = generate_plan(&content)?;

    // Print human-readable summary
    println!("{}", "Plan Summary:".bold());
    println!("{}", plan.summary);

    if !plan.risk_flags.is_empty() {
        println!();
        println!("{}:", "Risk Flags".bold());
        for flag in &plan.risk_flags {
            let severity = match flag.severity.as_str() {
                "critical" => flag.severity.red().bold().to_string(),
                "high" => flag.severity.red().to_string(),
                "medium" => flag.severity.yellow().to_string(),
                _ => flag.severity.cyan().to_string(),
            };
            println!("  [{}] {} — {}", severity, flag.code, flag.message);
            println!("         {}", flag.recommendation.dimmed());
        }
    }

    Ok(())
}

// ── Core logic ─────────────────────────────────────────────────────

pub fn generate_plan(yaml_str: &str) -> Result<PlanResult> {
    let spec = parser::parse_yaml(yaml_str)
        .with_context(|| "Failed to parse YAML for plan generation")?;

    // Extract identity info
    let entity = spec
        .get("entity")
        .and_then(|e| e.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("unknown")
        .to_string();

    let identity_version = spec
        .get("identity_version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    // Extract sources
    let sources = extract_sources(&spec);

    // Extract match strategies from rules
    let match_strategies = extract_match_strategies(&spec);

    // Extract survivorship
    let survivorship_summary = extract_survivorship(&spec);

    // Analyse blocking
    let blocking_analysis = analyse_blocking(&spec);

    // Build execution stages
    let source_names: Vec<String> = sources.iter().map(|s| s.name.clone()).collect();
    let execution_stages = build_execution_stages(&source_names, &match_strategies, &blocking_analysis);

    // Static analysis risk flags
    let risk_flags = analyse_risks(&spec, &match_strategies, &blocking_analysis, &survivorship_summary, &sources);

    // Compute plan hash
    let plan_hash = compute_plan_hash(&spec)?;

    // Build human-readable summary
    let summary = build_summary(
        &entity,
        &identity_version,
        &sources,
        &match_strategies,
        &blocking_analysis,
        &spec,
        &survivorship_summary,
        &risk_flags,
        &plan_hash,
    );

    Ok(PlanResult {
        entity,
        identity_version,
        plan_hash,
        sources,
        execution_stages,
        match_strategies,
        survivorship_summary,
        blocking_analysis,
        risk_flags,
        summary,
    })
}

fn extract_sources(spec: &serde_json::Value) -> Vec<PlanSource> {
    spec.get("sources")
        .and_then(|s| s.as_array())
        .map(|arr| {
            arr.iter()
                .map(|source| {
                    let name = source
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let system = source
                        .get("system")
                        .and_then(|s| s.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let field_count = source
                        .get("attributes")
                        .and_then(|a| a.as_object())
                        .map(|o| o.len())
                        .unwrap_or(0);
                    PlanSource {
                        name,
                        system,
                        field_count,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn extract_match_strategies(spec: &serde_json::Value) -> Vec<MatchStrategySummary> {
    spec.get("rules")
        .and_then(|r| r.as_array())
        .map(|rules| {
            rules
                .iter()
                .map(|rule| {
                    let rule_name = rule
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let match_type = rule
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let field = rule
                        .get("field")
                        .and_then(|f| f.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let algorithm = rule.get("algorithm").and_then(|a| a.as_str()).map(String::from);
                    let threshold = rule.get("threshold").and_then(|t| t.as_f64());
                    let weight = rule.get("weight").and_then(|w| w.as_f64()).unwrap_or(0.0);

                    // Exact rules evaluate before fuzzy
                    let evaluation_order = match match_type.as_str() {
                        "exact" => 3,
                        "fuzzy" | "phonetic" => 4,
                        "composite" => 4,
                        _ => 4,
                    };

                    MatchStrategySummary {
                        rule_name,
                        match_type,
                        field,
                        algorithm,
                        threshold,
                        weight,
                        evaluation_order,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn extract_survivorship(spec: &serde_json::Value) -> Vec<SurvivorshipSummary> {
    spec.get("survivorship")
        .and_then(|s| s.get("rules"))
        .and_then(|r| r.as_array())
        .map(|rules| {
            rules
                .iter()
                .map(|rule| {
                    let field = rule
                        .get("field")
                        .and_then(|f| f.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let strategy = rule
                        .get("strategy")
                        .and_then(|s| s.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let source_priority = rule
                        .get("source_priority")
                        .and_then(|sp| sp.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        });
                    SurvivorshipSummary {
                        field,
                        strategy,
                        source_priority,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn analyse_blocking(spec: &serde_json::Value) -> BlockingAnalysis {
    let blocking = spec.get("blocking");

    let strategy = blocking
        .and_then(|b| b.get("strategy"))
        .and_then(|s| s.as_str())
        .unwrap_or("none")
        .to_string();

    let keys: Vec<BlockingKeySummary> = blocking
        .and_then(|b| b.get("keys"))
        .and_then(|k| k.as_array())
        .map(|arr| {
            arr.iter()
                .map(|key| {
                    let name = key
                        .get("field")
                        .or_else(|| key.get("name"))
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let transformation = key
                        .get("transform")
                        .or_else(|| key.get("transformation"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("identity")
                        .to_string();
                    BlockingKeySummary {
                        name,
                        transformation,
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let mut warnings = Vec::new();
    let estimated_reduction;

    if keys.is_empty() && strategy == "none" {
        warnings.push("No blocking keys defined — O(n\u{00B2}) pairwise comparisons".to_string());
        estimated_reduction = "none".to_string();
    } else if keys.len() == 1 {
        estimated_reduction = "low".to_string();
    } else if keys.len() <= 3 {
        estimated_reduction = "medium".to_string();
    } else {
        estimated_reduction = "high".to_string();
    }

    BlockingAnalysis {
        strategy,
        keys,
        estimated_reduction,
        warnings,
    }
}

fn build_execution_stages(
    source_names: &[String],
    match_strategies: &[MatchStrategySummary],
    blocking: &BlockingAnalysis,
) -> Vec<ExecutionStage> {
    let source_list = source_names.join(", ");

    let exact_rules: Vec<&MatchStrategySummary> = match_strategies
        .iter()
        .filter(|m| m.match_type == "exact")
        .collect();
    let fuzzy_rules: Vec<&MatchStrategySummary> = match_strategies
        .iter()
        .filter(|m| m.match_type != "exact")
        .collect();

    let exact_desc = if exact_rules.is_empty() {
        "No exact match rules defined".to_string()
    } else {
        exact_rules
            .iter()
            .map(|r| format!("{} on {} (w={})", r.rule_name, r.field, r.weight))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let fuzzy_desc = if fuzzy_rules.is_empty() {
        "No fuzzy match rules defined".to_string()
    } else {
        fuzzy_rules
            .iter()
            .map(|r| {
                let algo = r.algorithm.as_deref().unwrap_or("default");
                format!("{} on {} via {} (w={})", r.rule_name, r.field, algo, r.weight)
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let blocking_desc = if blocking.keys.is_empty() {
        "No blocking keys — full pairwise comparison".to_string()
    } else {
        blocking
            .keys
            .iter()
            .map(|k| format!("{} ({})", k.name, k.transformation))
            .collect::<Vec<_>>()
            .join(", ")
    };

    vec![
        ExecutionStage {
            stage: 1,
            name: "Normalize sources".to_string(),
            description: format!("Ingest and normalize fields from: {}", source_list),
            inputs: source_names.to_vec(),
            outputs: vec!["normalized_entities".to_string()],
        },
        ExecutionStage {
            stage: 2,
            name: "Generate blocking keys".to_string(),
            description: format!("Blocking strategy: {}. Keys: {}", blocking.strategy, blocking_desc),
            inputs: vec!["normalized_entities".to_string()],
            outputs: vec!["candidate_pairs".to_string()],
        },
        ExecutionStage {
            stage: 3,
            name: "Exact matches".to_string(),
            description: exact_desc,
            inputs: vec!["candidate_pairs".to_string()],
            outputs: vec!["exact_match_scores".to_string()],
        },
        ExecutionStage {
            stage: 4,
            name: "Fuzzy matches".to_string(),
            description: fuzzy_desc,
            inputs: vec!["candidate_pairs".to_string()],
            outputs: vec!["fuzzy_match_scores".to_string()],
        },
        ExecutionStage {
            stage: 5,
            name: "Score & decide".to_string(),
            description: "Aggregate weighted scores and apply thresholds".to_string(),
            inputs: vec![
                "exact_match_scores".to_string(),
                "fuzzy_match_scores".to_string(),
            ],
            outputs: vec!["match_decisions".to_string()],
        },
        ExecutionStage {
            stage: 6,
            name: "Cluster entities".to_string(),
            description: "Transitive closure via UnionFind to group matched entities".to_string(),
            inputs: vec!["match_decisions".to_string()],
            outputs: vec!["entity_clusters".to_string()],
        },
        ExecutionStage {
            stage: 7,
            name: "Apply survivorship".to_string(),
            description: "Apply field-level survivorship rules to build golden records".to_string(),
            inputs: vec!["entity_clusters".to_string()],
            outputs: vec!["golden_records".to_string()],
        },
        ExecutionStage {
            stage: 8,
            name: "Emit outputs".to_string(),
            description: "Produce canonical table, lineage table, and audit trail".to_string(),
            inputs: vec!["golden_records".to_string()],
            outputs: vec![
                "canonical_entities".to_string(),
                "identity_lineage".to_string(),
                "audit_trail".to_string(),
            ],
        },
    ]
}

fn analyse_risks(
    spec: &serde_json::Value,
    match_strategies: &[MatchStrategySummary],
    blocking: &BlockingAnalysis,
    survivorship: &[SurvivorshipSummary],
    sources: &[PlanSource],
) -> Vec<RiskFlag> {
    let mut flags = Vec::new();

    // NO_BLOCKING — critical
    if blocking.keys.is_empty() && blocking.strategy == "none" {
        flags.push(RiskFlag {
            severity: "critical".to_string(),
            code: "NO_BLOCKING".to_string(),
            message: "No blocking keys defined — all pairs will be compared (O(n\u{00B2}))".to_string(),
            recommendation: "Add blocking keys to reduce comparison space".to_string(),
        });
    }

    // SINGLE_SIGNAL — high
    if match_strategies.len() == 1 {
        flags.push(RiskFlag {
            severity: "high".to_string(),
            code: "SINGLE_SIGNAL".to_string(),
            message: "Only one match rule — identity resolution depends on a single signal".to_string(),
            recommendation: "Add additional match rules for more robust identity resolution".to_string(),
        });
    }

    // LOW_THRESHOLD — high
    for strategy in match_strategies {
        if let Some(threshold) = strategy.threshold {
            if threshold < 0.8 && strategy.match_type != "exact" {
                flags.push(RiskFlag {
                    severity: "high".to_string(),
                    code: "LOW_THRESHOLD".to_string(),
                    message: format!(
                        "Rule '{}' has threshold {:.2} — risk of over-merging",
                        strategy.rule_name, threshold
                    ),
                    recommendation: "Consider raising threshold to 0.8+ or adding verification rules".to_string(),
                });
            }
        }
    }

    // HIGH_WEIGHT_FUZZY — medium
    for strategy in match_strategies {
        if strategy.match_type != "exact" && strategy.weight > 0.9 {
            flags.push(RiskFlag {
                severity: "medium".to_string(),
                code: "HIGH_WEIGHT_FUZZY".to_string(),
                message: format!(
                    "Fuzzy rule '{}' has weight {:.2} — high trust in approximate matching",
                    strategy.rule_name, strategy.weight
                ),
                recommendation: "Verify fuzzy algorithm accuracy or reduce weight".to_string(),
            });
        }
    }

    // NO_SURVIVORSHIP — medium
    if survivorship.is_empty() {
        flags.push(RiskFlag {
            severity: "medium".to_string(),
            code: "NO_SURVIVORSHIP".to_string(),
            message: "No survivorship rules defined — field selection will be arbitrary".to_string(),
            recommendation: "Define survivorship rules to control golden record field selection".to_string(),
        });
    }

    // PHONE_WITHOUT_BLOCKING — high
    let has_phone_rule = match_strategies
        .iter()
        .any(|m| m.field.contains("phone"));
    let has_phone_blocking = blocking
        .keys
        .iter()
        .any(|k| k.name.contains("phone"));
    if has_phone_rule && !has_phone_blocking {
        flags.push(RiskFlag {
            severity: "high".to_string(),
            code: "PHONE_WITHOUT_BLOCKING".to_string(),
            message: "Phone match rule without phone-based blocking key — phone reuse risk".to_string(),
            recommendation: "Add a phone-based blocking key (e.g., area code)".to_string(),
        });
    }

    // NO_REVIEW_THRESHOLD — medium
    let has_review = spec
        .get("decision")
        .and_then(|d| d.get("thresholds"))
        .and_then(|t| t.get("review"))
        .is_some();
    if !has_review {
        flags.push(RiskFlag {
            severity: "medium".to_string(),
            code: "NO_REVIEW_THRESHOLD".to_string(),
            message: "No review threshold — all decisions are merge-or-reject with no review band".to_string(),
            recommendation: "Add a review threshold for ambiguous matches".to_string(),
        });
    }

    // SINGLE_SOURCE — low
    if sources.len() == 1 {
        flags.push(RiskFlag {
            severity: "low".to_string(),
            code: "SINGLE_SOURCE".to_string(),
            message: "Only one source — no cross-system identity resolution".to_string(),
            recommendation: "Add additional sources for cross-system matching".to_string(),
        });
    }

    // MISSING_TEMPORAL — low
    let has_temporal = spec.get("temporal").is_some();
    if !has_temporal {
        flags.push(RiskFlag {
            severity: "low".to_string(),
            code: "MISSING_TEMPORAL".to_string(),
            message: "No temporal configuration — identity resolution is not time-aware".to_string(),
            recommendation: "Add temporal config if entities have time-dependent attributes".to_string(),
        });
    }

    flags
}

fn compute_plan_hash(spec: &serde_json::Value) -> Result<String> {
    let canonical = serde_json::to_string(spec)?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[allow(clippy::too_many_arguments)]
fn build_summary(
    entity: &str,
    identity_version: &str,
    sources: &[PlanSource],
    match_strategies: &[MatchStrategySummary],
    blocking: &BlockingAnalysis,
    spec: &serde_json::Value,
    survivorship: &[SurvivorshipSummary],
    risk_flags: &[RiskFlag],
    plan_hash: &str,
) -> String {
    let source_names: Vec<&str> = sources.iter().map(|s| s.name.as_str()).collect();
    let source_list = source_names.join(", ");

    let signals: Vec<String> = match_strategies
        .iter()
        .map(|m| {
            if let Some(ref algo) = m.algorithm {
                format!("{} ({}/{}, w={})", m.field, m.match_type, algo, m.weight)
            } else {
                format!("{} ({}, w={})", m.field, m.match_type, m.weight)
            }
        })
        .collect();
    let signals_str = signals.join(", ");

    let blocking_keys: Vec<&str> = blocking.keys.iter().map(|k| k.name.as_str()).collect();
    let blocking_str = if blocking_keys.is_empty() {
        "none".to_string()
    } else {
        blocking_keys.join(", ")
    };

    let match_threshold = spec
        .get("decision")
        .and_then(|d| d.get("thresholds"))
        .and_then(|t| t.get("match"))
        .and_then(|m| m.as_f64());
    let review_threshold = spec
        .get("decision")
        .and_then(|d| d.get("thresholds"))
        .and_then(|t| t.get("review"))
        .and_then(|r| r.as_f64());

    let thresholds_str = match (match_threshold, review_threshold) {
        (Some(m), Some(r)) => format!("merge >= {}, review >= {}", m, r),
        (Some(m), None) => format!("merge >= {}", m),
        _ => "not configured".to_string(),
    };

    let critical_count = risk_flags.iter().filter(|f| f.severity == "critical").count();
    let high_count = risk_flags.iter().filter(|f| f.severity == "high").count();
    let medium_count = risk_flags.iter().filter(|f| f.severity == "medium").count();

    let short_hash = if plan_hash.len() > 15 {
        &plan_hash[..15]
    } else {
        plan_hash
    };

    format!(
        "  Identity:     {} ({})\n  Sources:      {} ({})\n  Signals:      {}\n  Blocking:     {}\n  Thresholds:   {}\n  Stages:       8 execution stages\n  Survivorship: {} fields configured\n  Risk flags:   {} critical, {} high, {} medium\n  Plan hash:    {}...",
        entity,
        identity_version,
        sources.len(),
        source_list,
        signals_str,
        blocking_str,
        thresholds_str,
        survivorship.len(),
        critical_count,
        high_count,
        medium_count,
        short_hash,
    )
}
