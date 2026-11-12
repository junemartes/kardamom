use super::*;

struct Fixture {
    source: tempfile::TempDir,
    target: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let source = tempfile::tempdir().unwrap();
        std::fs::create_dir(source.path().join("dir")).unwrap();
        std::fs::write(source.path().join("dir/16-0.rec"), b"victim bytes").unwrap();
        std::fs::write(source.path().join("dir/24-0.rec"), b"mirror bytes").unwrap();
        std::fs::write(source.path().join("dir/archive.catalog"), b"catalog").unwrap();
        std::fs::File::create(source.path().join("dir/unrelated.rec"))
            .unwrap()
            .set_len(64 * 1024 * 1024)
            .unwrap();
        Self {
            source,
            target: tempfile::tempdir().unwrap(),
        }
    }

    async fn assert_selected(&self, name: &str, expected: &[u8]) {
        let output = tokio::process::Command::new("sh")
            .args(["-c", &SegmentStage::new(name).command(self.source.path())])
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        assert!(output.stdout.len() < 32 * 1024);
        SegmentStage::unpack(self.target.path(), output.stdout)
            .await
            .unwrap();
        let dir = self.target.path().join("dir");
        assert_eq!(std::fs::read(dir.join(name)).unwrap(), expected);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        assert!(!dir.join("archive.catalog").exists());
        assert!(!dir.join("unrelated.rec").exists());
    }
}

#[tokio::test]
async fn staging_copies_only_the_selected_victim_segment() {
    Fixture::new()
        .assert_selected("16-0.rec", b"victim bytes")
        .await;
}

#[tokio::test]
async fn staging_reads_the_mirrors_own_recording_name() {
    Fixture::new()
        .assert_selected("24-0.rec", b"mirror bytes")
        .await;
}
