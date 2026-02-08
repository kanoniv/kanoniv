use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_validate_minimal_success() {
    let mut cmd = Command::cargo_bin("kanoniv").unwrap();
    cmd.arg("validate").arg("tests/fixtures/valid/minimal.yaml");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("is valid"));
}

#[test]
fn test_validate_missing_entity_failure() {
    let mut cmd = Command::cargo_bin("kanoniv").unwrap();
    cmd.arg("validate")
        .arg("tests/fixtures/invalid/missing_entity.yaml");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Missing required field: entity"));
}

#[test]
fn test_validate_unknown_field_failure() {
    let mut cmd = Command::cargo_bin("kanoniv").unwrap();
    cmd.arg("validate")
        .arg("tests/fixtures/invalid/unknown_field.yaml");

    cmd.assert().failure().stderr(predicate::str::contains(
        "references unknown field 'unknown_field'",
    ));
}

#[test]
fn test_hash_success() {
    let mut cmd = Command::cargo_bin("kanoniv").unwrap();
    cmd.arg("hash").arg("tests/fixtures/valid/minimal.yaml");

    cmd.assert()
        .success()
        .stdout(predicate::str::starts_with("sha256:"));
}
