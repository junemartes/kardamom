//! The pure steps of a sealer fleet seed: the head that a seed file
//! names, and the job edits that start the members from the seed and the
//! da-watcher after the seed's L1 origin.

use anyhow::Context;
use serde_json::Value;

/// Where the cluster job mounts the seed directory on a sealer node.
pub(super) const SEED_DIR: &str = "/opt/kardamom/seed";

/// The head that a seed names: H, `E_H` (the canonical end of H) and the
/// L1 origin of H.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SeedHead {
    pub(super) block: u64,
    pub(super) end_tx_idx: u64,
    pub(super) l1_origin: u64,
}

impl SeedHead {
    const MAGIC: &'static [u8] = b"KSED";
    const VERSION: [u8; 4] = 1_u32.to_be_bytes();
    /// The byte offsets of the big-endian fields: after the magic and
    /// the version come the chain id, H, `E_H`, the timestamp of H and the
    /// L1 origin of H.
    const BLOCK_AT: usize = 16;
    const END_AT: usize = 24;
    const ORIGIN_AT: usize = 40;

    /// The head of the seed file `bytes`.
    ///
    /// # Errors
    ///
    /// Returns an error for a file that is not a version 1 seed.
    pub(super) fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.get(..4) == Some(Self::MAGIC) && bytes.get(4..8) == Some(&Self::VERSION[..]),
            "the file is not a version 1 sealer seed"
        );
        Ok(Self {
            block: Self::word(bytes, Self::BLOCK_AT)?,
            end_tx_idx: Self::word(bytes, Self::END_AT)?,
            l1_origin: Self::word(bytes, Self::ORIGIN_AT)?,
        })
    }

    fn word(bytes: &[u8], at: usize) -> anyhow::Result<u64> {
        let word = bytes
            .get(at..at + 8)
            .with_context(|| format!("the seed ends before byte {}", at + 8))?;
        Ok(u64::from_be_bytes(word.try_into()?))
    }

    /// The cursor a consumer at the rebuilt head resumes from, as the
    /// sealer logs it: `(E_H,H+1)`.
    pub(super) fn resume_cursor(self) -> String {
        format!("({},{})", self.end_tx_idx, self.block.saturating_add(1))
    }
}

/// A space-separated list of JVM options, as the cluster job passes it in
/// `JAVA_TOOL_OPTIONS`.
struct JavaOptions<'a>(&'a str);

impl JavaOptions<'_> {
    /// The options with the system property `key` set to `value`. The
    /// option of the key moves to the end; the JVM reads each property
    /// once, so the order does not matter.
    fn with_property(&self, key: &str, value: &str) -> String {
        let prefix = format!("-D{key}=");
        let option = format!("{prefix}{value}");
        self.0
            .split_whitespace()
            .filter(|o| !o.starts_with(&prefix))
            .chain(std::iter::once(option.as_str()))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// A registered job definition, as Nomad returns it.
pub(super) struct JobDefinition<'a>(pub(super) &'a Value);

impl JobDefinition<'_> {
    /// The cluster job with every member started from the seed at
    /// `seed`. A seed carries no remote-origin anchor, so a seeded member
    /// runs with an empty allowlist.
    ///
    /// # Errors
    ///
    /// Returns an error if no task of the job passes `JAVA_TOOL_OPTIONS`.
    pub(super) fn seeded(&self, seed: &str) -> anyhow::Result<Value> {
        let mut job = self.0.clone();
        let edited = Self::tasks(&mut job)
            .filter_map(|task| task.pointer_mut("/Env/JAVA_TOOL_OPTIONS"))
            .map(|options| {
                let with_seed = JavaOptions(options.as_str().unwrap_or_default())
                    .with_property("kardamom.cluster.seedSnapshot", seed);
                *options = Value::String(
                    JavaOptions(&with_seed).with_property("kardamom.cluster.remoteOrigins", ""),
                );
            })
            .count();
        anyhow::ensure!(edited > 0, "no task of the job passes JAVA_TOOL_OPTIONS");
        Ok(job)
    }

    /// The da-watcher job that starts after L1 block `origin`.
    ///
    /// # Errors
    ///
    /// Returns an error if no task of the job has an argument list.
    pub(super) fn resumed_after(&self, origin: u64) -> anyhow::Result<Value> {
        let mut job = self.0.clone();
        let edited = Self::tasks(&mut job)
            .filter_map(|task| task.pointer_mut("/Config/args"))
            .filter_map(Value::as_array_mut)
            .map(|args| {
                args.extend([
                    Value::String("--l1-resume-after".to_string()),
                    Value::String(origin.to_string()),
                ]);
            })
            .count();
        anyhow::ensure!(edited > 0, "no task of the job has an argument list");
        Ok(job)
    }

    /// Every task of every group of `job`.
    fn tasks(job: &mut Value) -> impl Iterator<Item = &mut Value> {
        job.get_mut("TaskGroups")
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
            .filter_map(|group| group.get_mut("Tasks").and_then(Value::as_array_mut))
            .flatten()
    }
}

#[cfg(test)]
mod tests;
