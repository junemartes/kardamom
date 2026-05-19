//! Read forge build artifacts (creation bytecode) from `contracts/out/<Name>.sol/<Name>.json`.
//!
//! Generalizes the `read_bytecode` helper from `crates/node/tests/deposit_e2e.rs` and
//! gives a single typed error for the "forge artifacts missing" case so callers can
//! report it consistently.

use std::path::{Path, PathBuf};

use alloy_primitives::Bytes;

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error(
        "artifact file not found at {path}: install Foundry and run `forge build` in contracts/"
    )]
    Missing { path: PathBuf },
    #[error("artifact at {path} is malformed: {detail}")]
    Malformed { path: PathBuf, detail: String },
}

/// Default `contracts/` location resolved relative to the kardamom workspace root.
/// Honors the `KARDAMOM_CONTRACTS_DIR` env var for tests that compile in tmpdirs.
pub fn default_contracts_root() -> PathBuf {
    if let Ok(p) = std::env::var("KARDAMOM_CONTRACTS_DIR") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("contracts")
}

pub fn artifact_path(contracts_root: &Path, name: &str) -> PathBuf {
    contracts_root
        .join("out")
        .join(format!("{name}.sol"))
        .join(format!("{name}.json"))
}

/// Read creation bytecode (`bytecode.object`) from a forge artifact JSON.
pub fn creation_bytecode(contracts_root: &Path, name: &str) -> Result<Bytes, ArtifactError> {
    let path = artifact_path(contracts_root, name);
    let contents = std::fs::read_to_string(&path)
        .map_err(|_| ArtifactError::Missing { path: path.clone() })?;
    let v: serde_json::Value =
        serde_json::from_str(&contents).map_err(|e| ArtifactError::Malformed {
            path: path.clone(),
            detail: format!("not valid JSON: {e}"),
        })?;
    let hex_str = v["bytecode"]["object"]
        .as_str()
        .ok_or_else(|| ArtifactError::Malformed {
            path: path.clone(),
            detail: "missing bytecode.object".into(),
        })?;
    let hex_str = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = alloy_primitives::hex::decode(hex_str).map_err(|e| ArtifactError::Malformed {
        path,
        detail: format!("bytecode.object is not valid hex: {e}"),
    })?;
    Ok(Bytes::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_artifact(dir: &Path, name: &str, body: &str) -> PathBuf {
        let nested = dir.join("out").join(format!("{name}.sol"));
        std::fs::create_dir_all(&nested).unwrap();
        let path = nested.join(format!("{name}.json"));
        std::fs::File::create(&path)
            .unwrap()
            .write_all(body.as_bytes())
            .unwrap();
        path
    }

    #[test]
    fn missing_artifact_returns_missing_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = creation_bytecode(dir.path(), "Foo").unwrap_err();
        assert!(matches!(err, ArtifactError::Missing { .. }));
    }

    #[test]
    fn malformed_json_returns_malformed() {
        let dir = tempfile::tempdir().unwrap();
        write_artifact(dir.path(), "Foo", "{ not json");
        let err = creation_bytecode(dir.path(), "Foo").unwrap_err();
        assert!(matches!(err, ArtifactError::Malformed { .. }));
    }

    #[test]
    fn missing_bytecode_object_returns_malformed() {
        let dir = tempfile::tempdir().unwrap();
        write_artifact(dir.path(), "Foo", r#"{"bytecode":{}}"#);
        let err = creation_bytecode(dir.path(), "Foo").unwrap_err();
        assert!(matches!(err, ArtifactError::Malformed { .. }));
    }

    #[test]
    fn well_formed_returns_bytes() {
        let dir = tempfile::tempdir().unwrap();
        write_artifact(dir.path(), "Foo", r#"{"bytecode":{"object":"0xdeadbeef"}}"#);
        let b = creation_bytecode(dir.path(), "Foo").unwrap();
        assert_eq!(b.as_ref(), &[0xde, 0xad, 0xbe, 0xef]);
    }
}
