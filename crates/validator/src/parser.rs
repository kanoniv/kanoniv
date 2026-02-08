use anyhow::Result;
use serde_json::Value;

pub fn parse_yaml(content: &str) -> Result<Value> {
    let value: Value = serde_yaml::from_str(content)?;
    Ok(value)
}
