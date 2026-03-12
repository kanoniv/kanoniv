use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::commands::compile::compile_to_ir;
use crate::commands::diff::compute_diff;
use crate::commands::plan::generate_plan;
use crate::parser::parse_yaml;
use crate::validate_yaml;
use crate::validator::{validate_schema, validate_semantics};

fn json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.to_object(py)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.to_object(py))
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.to_object(py)),
        serde_json::Value::Array(arr) => {
            let list: Vec<PyObject> = arr
                .iter()
                .map(|v| json_value_to_py(py, v))
                .collect::<PyResult<_>>()?;
            Ok(list.to_object(py))
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
    }
}

#[pyfunction]
fn validate(yaml_str: &str) -> PyResult<Vec<String>> {
    validate_yaml(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

#[pyfunction]
fn validate_schema_py(yaml_str: &str) -> PyResult<Vec<String>> {
    let spec = parse_yaml(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    validate_schema(&spec)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

#[pyfunction]
fn validate_semantics_py(yaml_str: &str) -> PyResult<Vec<String>> {
    let spec = parse_yaml(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    validate_semantics(&spec)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

#[pyfunction]
fn parse(py: Python<'_>, yaml_str: &str) -> PyResult<PyObject> {
    let value = parse_yaml(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    json_value_to_py(py, &value)
}

#[pyfunction]
fn compile_ir(py: Python<'_>, yaml_str: &str) -> PyResult<PyObject> {
    let spec = parse_yaml(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let ir = compile_to_ir(&spec)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    json_value_to_py(py, &ir)
}

#[pyfunction]
fn diff(py: Python<'_>, yaml_a: &str, yaml_b: &str) -> PyResult<PyObject> {
    let result = compute_diff(yaml_a, yaml_b)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let value = serde_json::to_value(&result)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    json_value_to_py(py, &value)
}

#[pyfunction]
fn hash(yaml_str: &str) -> PyResult<String> {
    use sha2::Digest;
    let spec: serde_json::Value = serde_yaml::from_str(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let canonical = serde_json::to_string(&spec)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical.as_bytes());
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[pyfunction]
fn plan(py: Python<'_>, yaml_str: &str) -> PyResult<PyObject> {
    let result = generate_plan(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let value = serde_json::to_value(&result)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    json_value_to_py(py, &value)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(validate_schema_py, m)?)?;
    m.add_function(wrap_pyfunction!(validate_semantics_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    m.add_function(wrap_pyfunction!(compile_ir, m)?)?;
    m.add_function(wrap_pyfunction!(diff, m)?)?;
    m.add_function(wrap_pyfunction!(hash, m)?)?;
    m.add_function(wrap_pyfunction!(plan, m)?)?;
    Ok(())
}
