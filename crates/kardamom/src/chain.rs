//! Load a `Genesis` from a TOML file on disk.
//!
//! Lives in the binary crate so the node crate doesn't pick up a `toml`
//! dependency. The `Genesis` struct itself (and field-level parsing) lives
//! in `kardamom_node::genesis`; here we just glue `toml::from_str` to the
//! filesystem and run the post-deserialize `validate()`.

use std::path::Path;

use anyhow::Context;
use kardamom_node::Genesis;

pub fn load(path: &Path) -> anyhow::Result<Genesis> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading genesis file {}", path.display()))?;
    let genesis: Genesis = toml::from_str(&contents)
        .with_context(|| format!("parsing genesis file {}", path.display()))?;
    genesis
        .validate()
        .with_context(|| format!("validating genesis file {}", path.display()))?;
    Ok(genesis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes, U256, address, hex};

    fn parse(s: &str) -> anyhow::Result<Genesis> {
        let g: Genesis = toml::from_str(s)?;
        g.validate()?;
        Ok(g)
    }

    fn find<'a>(g: &'a Genesis, addr: &Address) -> Option<&'a kardamom_node::AllocEntry> {
        g.alloc.iter().find(|e| e.address == *addr)
    }

    #[test]
    fn parses_chain_id_and_empty_alloc() {
        let g = parse("chain_id = 412346\n").expect("parses");
        assert_eq!(g.chain_id, 412346);
        assert!(g.alloc.is_empty());
    }

    #[test]
    fn parses_balance_decimal_and_hex() {
        let g = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
                balance = "1000"
                [[alloc]]
                address = "0x0000000000000000000000000000000000000002"
                balance = "0x3e8"
            "#,
        )
        .expect("parses");
        let a1: Address = address!("0000000000000000000000000000000000000001");
        let a2: Address = address!("0000000000000000000000000000000000000002");
        assert_eq!(find(&g, &a1).unwrap().balance, U256::from(1000u64));
        assert_eq!(find(&g, &a2).unwrap().balance, U256::from(1000u64));
    }

    #[test]
    fn omitted_balance_defaults_to_zero() {
        let g = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
            "#,
        )
        .expect("parses");
        let a: Address = address!("0000000000000000000000000000000000000001");
        assert_eq!(find(&g, &a).unwrap().balance, U256::ZERO);
    }

    #[test]
    fn omitted_nonce_parses_as_none() {
        // The conditional nonce default (1 for code-bearing entries, 0
        // otherwise) lives in `Node::new`, not in the loader. The loader
        // surfaces what was on disk.
        let g = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000001234"
                code = "0x604260005260206000f3"
            "#,
        )
        .expect("parses");
        let a: Address = address!("0000000000000000000000000000000000001234");
        let entry = find(&g, &a).unwrap();
        let expected_code = Bytes::from(hex!("604260005260206000f3").to_vec());
        assert_eq!(entry.code.as_ref(), Some(&expected_code));
        assert!(entry.nonce.is_none());
    }

    #[test]
    fn explicit_nonce_is_parsed() {
        let g = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000001234"
                code = "0x604260005260206000f3"
                nonce = 0
            "#,
        )
        .expect("parses");
        let a: Address = address!("0000000000000000000000000000000000001234");
        assert_eq!(find(&g, &a).unwrap().nonce, Some(0));
    }

    #[test]
    fn duplicate_address_errors() {
        let err = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
                balance = "1"
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
                balance = "2"
            "#,
        )
        .expect_err("dup should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("duplicate alloc address"), "msg = {msg}");
    }

    #[test]
    fn chain_id_zero_errors() {
        let err = parse("chain_id = 0\n").expect_err("zero should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("chain_id"), "msg = {msg}");
    }

    #[test]
    fn missing_chain_id_errors() {
        let err = parse("").expect_err("missing should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("chain_id"), "msg = {msg}");
    }

    #[test]
    fn unknown_top_level_field_errors() {
        let err = parse(
            r#"
                chain_id = 1
                garbage = true
            "#,
        )
        .expect_err("unknown field should fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("garbage") || msg.contains("unknown"),
            "msg = {msg}"
        );
    }

    #[test]
    fn unknown_alloc_field_errors() {
        let err = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
                storage = "nope"
            "#,
        )
        .expect_err("unknown alloc field should fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("storage") || msg.contains("unknown"),
            "msg = {msg}"
        );
    }

    #[test]
    fn bad_address_errors() {
        let err = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0xnothex"
                balance = "1"
            "#,
        )
        .expect_err("bad address should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("address"), "msg = {msg}");
    }

    #[test]
    fn bad_balance_errors() {
        let err = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
                balance = "not-a-number"
            "#,
        )
        .expect_err("bad balance should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("balance"), "msg = {msg}");
    }

    #[test]
    fn bad_code_hex_errors() {
        let err = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
                code = "0xZZ"
            "#,
        )
        .expect_err("bad hex should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("hex"), "msg = {msg}");
    }

    #[test]
    fn empty_code_string_treated_as_no_code() {
        let g = parse(
            r#"
                chain_id = 1
                [[alloc]]
                address = "0x0000000000000000000000000000000000000001"
                code = "0x"
            "#,
        )
        .expect("parses");
        let a: Address = address!("0000000000000000000000000000000000000001");
        let entry = find(&g, &a).unwrap();
        assert!(entry.code.is_none());
        assert!(entry.nonce.is_none());
    }

    #[test]
    fn dev_genesis_file_parses() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = std::path::Path::new(manifest_dir)
            .join("../..")
            .join("chains/dev.toml");
        let g = load(&path).expect("dev.toml parses");
        assert_eq!(g.chain_id, 412346);
        let dev: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
        let expected = U256::from(1_000u64) * U256::from(10u64).pow(U256::from(18u64));
        let entry = find(&g, &dev).expect("dev account present");
        assert_eq!(entry.balance, expected);
        assert!(entry.code.is_none());
    }
}
