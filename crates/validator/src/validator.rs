use anyhow::Result;
use serde_json::Value;

/// Validate against JSON Schema
pub fn validate_schema(spec: &Value) -> Result<Vec<String>> {
    let mut errors = Vec::new();

    // Check required top-level fields
    if spec.get("api_version").is_none() {
        errors.push("Missing required field: api_version".to_string());
    }

    if spec.get("identity_version").is_none() {
        errors.push("Missing required field: identity_version".to_string());
    }

    if spec.get("entity").is_none() {
        errors.push("Missing required field: entity".to_string());
    }

    // Validate api_version format
    if let Some(api_version) = spec.get("api_version").and_then(|v| v.as_str()) {
        if !api_version.starts_with("kanoniv/v") {
            errors.push(format!(
                "Invalid api_version format: '{}'. Expected 'kanoniv/v<N>'",
                api_version
            ));
        }
    }

    // Validate entity structure
    if let Some(entity) = spec.get("entity") {
        if entity.get("name").is_none() {
            errors.push("entity.name is required".to_string());
        }
    }

    // Validate rules
    if let Some(rules) = spec.get("rules").and_then(|r| r.as_array()) {
        if rules.len() > 50 {
            errors.push(format!("Too many rules: {}. Maximum is 50.", rules.len()));
        }

        for (i, rule) in rules.iter().enumerate() {
            if rule.get("name").is_none() {
                errors.push(format!("rules[{}]: missing required field 'name'", i));
            }
            if rule.get("type").is_none() {
                errors.push(format!("rules[{}]: missing required field 'type'", i));
            }

            // Validate weight bounds
            if let Some(weight) = rule.get("weight").and_then(|w| w.as_f64()) {
                if !(0.0..=1.0).contains(&weight) {
                    errors.push(format!(
                        "rules[{}]: weight {} must be between 0 and 1",
                        i, weight
                    ));
                }
            }

            // Validate threshold bounds
            if let Some(threshold) = rule.get("threshold").and_then(|t| t.as_f64()) {
                if !(0.0..=1.0).contains(&threshold) {
                    errors.push(format!(
                        "rules[{}]: threshold {} must be between 0 and 1",
                        i, threshold
                    ));
                }
            }
        }
    }

    // Validate sources
    if let Some(sources) = spec.get("sources").and_then(|s| s.as_array()) {
        if sources.len() > 10 {
            errors.push(format!(
                "Too many sources: {}. Maximum is 10.",
                sources.len()
            ));
        }

        for (i, source) in sources.iter().enumerate() {
            for field in &["name", "system", "table", "id", "attributes"] {
                if source.get(*field).is_none() {
                    errors.push(format!(
                        "sources[{}]: missing required field '{}'",
                        i, field
                    ));
                }
            }
        }
    }

    // Validate blocking keys
    if let Some(blocking) = spec.get("blocking") {
        if let Some(keys) = blocking.get("keys").and_then(|k| k.as_array()) {
            if keys.len() > 5 {
                errors.push(format!(
                    "Too many blocking keys: {}. Maximum is 5.",
                    keys.len()
                ));
            }
        }
    }

    Ok(errors)
}

/// Validate semantic/business rules
pub fn validate_semantics(spec: &Value) -> Result<Vec<String>> {
    let mut errors = Vec::new();

    // Collect all field names from sources
    let mut available_fields: Vec<String> = Vec::new();
    if let Some(sources) = spec.get("sources").and_then(|s| s.as_array()) {
        for source in sources {
            if let Some(attrs) = source.get("attributes").and_then(|a| a.as_object()) {
                for key in attrs.keys() {
                    if !available_fields.contains(key) {
                        available_fields.push(key.clone());
                    }
                }
            }
        }
    }

    // Validate rule field references
    if let Some(rules) = spec.get("rules").and_then(|r| r.as_array()) {
        for rule in rules {
            if let Some(field) = rule.get("field").and_then(|f| f.as_str()) {
                if !available_fields.is_empty() && !available_fields.contains(&field.to_string()) {
                    let rule_name = rule
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown");

                    // Suggest similar field names
                    let suggestion = available_fields
                        .iter()
                        .find(|f| f.contains(field) || field.contains(f.as_str()))
                        .map(|f| format!(" Did you mean '{}'?", f))
                        .unwrap_or_default();

                    errors.push(format!(
                        "Rule '{}' references unknown field '{}'.{}",
                        rule_name, field, suggestion
                    ));
                }
            }
        }
    }

    // Check for duplicate rule names
    if let Some(rules) = spec.get("rules").and_then(|r| r.as_array()) {
        let mut seen_names: Vec<&str> = Vec::new();
        for rule in rules {
            if let Some(name) = rule.get("name").and_then(|n| n.as_str()) {
                if seen_names.contains(&name) {
                    errors.push(format!("Duplicate rule name: '{}'", name));
                } else {
                    seen_names.push(name);
                }
            }
        }
    }

    // Check for duplicate source names
    if let Some(sources) = spec.get("sources").and_then(|s| s.as_array()) {
        let mut seen_names: Vec<&str> = Vec::new();
        for source in sources {
            if let Some(name) = source.get("name").and_then(|n| n.as_str()) {
                if seen_names.contains(&name) {
                    errors.push(format!("Duplicate source name: '{}'", name));
                } else {
                    seen_names.push(name);
                }
            }
        }
    }

    // Validate threshold ordering
    if let Some(decision) = spec.get("decision") {
        if let Some(thresholds) = decision.get("thresholds") {
            let match_t = thresholds
                .get("match")
                .and_then(|t| t.as_f64())
                .unwrap_or(1.0);
            let review_t = thresholds
                .get("review")
                .and_then(|t| t.as_f64())
                .unwrap_or(0.0);
            let reject_t = thresholds
                .get("reject")
                .and_then(|t| t.as_f64())
                .unwrap_or(0.0);

            if match_t < review_t {
                errors.push("Threshold error: 'match' should be >= 'review'".to_string());
            }
            if review_t < reject_t {
                errors.push("Threshold error: 'review' should be >= 'reject'".to_string());
            }
        }
    }

    Ok(errors)
}
