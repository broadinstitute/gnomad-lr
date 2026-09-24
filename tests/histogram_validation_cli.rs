//! CLI-level proof that validation has no target, emits JSON, and fails nonzero.
use base64::{engine::general_purpose::STANDARD, Engine};
use md5::{Digest, Md5};
use serde_json::Value;
use std::process::Command;

fn command(body: &[u8], path: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_gnomad-lr"));
    cmd.args([
        "validate-histograms",
        "--source",
        path,
        "--source-size-bytes",
        &body.len().to_string(),
        "--source-md5-base64",
        &STANDARD.encode(Md5::digest(body)),
    ]);
    cmd
}

#[test]
fn histogram_validation_cli_target_free_read_only_success_and_failure() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/loader/histograms/fixtures/aou.tsv"
    );
    let body = std::fs::read(path).unwrap();
    let output = command(&body, path).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "complete_success");
    assert_eq!(report["source"], path);
    assert_eq!(
        report["expected_md5_base64"],
        STANDARD.encode(Md5::digest(&body))
    );
    assert!(report["expected_generation"].is_null());
    assert_eq!(report["complete_body_identity_verified"], true);
    assert_eq!(report["clickhouse_writes"], 0);
    for target in [
        "--clickhouse-url",
        "--endpoint",
        "--database",
        "--target",
        "--region",
        "--skip-invalid",
    ] {
        assert!(!command(&body, path)
            .args([target, "forbidden"])
            .output()
            .unwrap()
            .status
            .success());
    }
    let output = command(&body, path)
        .args(["--max-rows", "1"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "bounded_validation");
    assert_eq!(report["complete_body_identity_verified"], false);

    // Synthetic malformed row: no private sample data written for diagnostics.
    let invalid = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/loader/histograms/fixtures/invalid-row.tsv"
    );
    let invalid_body = std::fs::read(invalid).unwrap();
    let output = command(&invalid_body, invalid).output().unwrap();
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "stopped_first_error");
    assert_eq!(report["diagnostic"], "invalid_histogram_row");
    assert_eq!(report["rejected_rows"], 1);
    assert_eq!(report["clickhouse_writes"], 0);
}

fn bounded_output(mut command: Command) -> std::process::Output {
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("validator did not exit within five seconds: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn histogram_validation_cli_rejected_identity_never_echoes_secrets() {
    const SECRET: &str = "REVIEW_SYNTHETIC_SECRET";
    let valid_md5 = STANDARD.encode([0; 16]);
    for (source, generation, md5, diagnostic) in [
        (
            format!("gs://synthetic/fixture.tsv?access_token={SECRET}"),
            "123".to_owned(),
            valid_md5.clone(),
            "source_open_or_identity_error",
        ),
        (
            format!("gs://user:{SECRET}@synthetic/fixture.tsv"),
            "123".to_owned(),
            valid_md5.clone(),
            "source_open_or_identity_error",
        ),
        (
            format!("gs://synthetic/fixture.tsv#{SECRET}"),
            "123".to_owned(),
            valid_md5.clone(),
            "source_open_or_identity_error",
        ),
        (
            "gs://synthetic/fixture.tsv".to_owned(),
            SECRET.to_owned(),
            valid_md5.clone(),
            "source_open_or_identity_error",
        ),
        (
            "gs://synthetic/fixture.tsv".to_owned(),
            "123".to_owned(),
            SECRET.to_owned(),
            "invalid_md5_base64",
        ),
        (
            format!("https://user:{SECRET}@synthetic/fixture.tsv"),
            "123".to_owned(),
            valid_md5.clone(),
            "source_open_or_identity_error",
        ),
        (
            format!("missing-{SECRET}.tsv"),
            "123".to_owned(),
            valid_md5.clone(),
            "source_open_or_identity_error",
        ),
    ] {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_gnomad-lr"));
        cmd.args([
            "validate-histograms",
            "--source",
            &source,
            "--source-generation",
            &generation,
            "--source-size-bytes",
            "1",
            "--source-md5-base64",
            &md5,
        ]);
        let output = bounded_output(cmd);
        assert!(!output.status.success());
        for bytes in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(bytes).contains(SECRET));
        }
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["diagnostic"], diagnostic);
        assert!(report["source"].is_null());
        assert!(report["expected_generation"].is_null());
        if diagnostic == "invalid_md5_base64" {
            assert!(report["expected_md5_base64"].is_null());
        } else {
            assert_eq!(report["expected_md5_base64"], valid_md5);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn histogram_validation_cli_nonregular_local_inputs_fail_without_blocking() {
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDirectory(std::path::PathBuf);
    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory = TempDirectory(std::env::temp_dir().join(format!(
        "gnomad-lr-fifo-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    )));
    std::fs::create_dir(&directory.0).unwrap();
    let fifo = directory.0.join("source.fifo");
    assert!(Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let link = directory.0.join("source.link");
    std::os::unix::fs::symlink(&fifo, &link).unwrap();
    for path in [
        fifo.as_path(),
        link.as_path(),
        directory.0.as_path(),
        std::path::Path::new("/dev/null"),
    ] {
        // No writer is ever opened. The subprocess deadline also bounds regressions.
        let output = bounded_output(command(b"x", path.to_str().unwrap()));
        assert!(!output.status.success());
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["diagnostic"], "source_open_or_identity_error");
        assert!(report["source"].is_null());
        assert_eq!(report["bytes_read"], 0);
        assert_eq!(report["complete_body_identity_verified"], false);
    }
}

#[test]
fn histogram_validation_path_cannot_construct_database_clients() {
    // Architecture guard: the only public entry point takes source-only arguments;
    // its module has no insertion callback or database dependency. The production
    // loader remains a separate sibling and is never called by this CLI branch.
    let module = include_str!("../src/loader/histograms/validation.rs");
    for forbidden in [
        "ClickHouse",
        "load_str_histograms(",
        "stream_histograms(",
        "inserter",
        "reqwest::",
    ] {
        assert!(
            !module.contains(forbidden),
            "validation acquired a write-capable dependency: {forbidden}"
        );
    }
    let main = include_str!("../src/main.rs");
    let branch = main
        .split("Commands::ValidateHistograms(args) => {")
        .nth(1)
        .unwrap()
        .split("Commands::Load { target }")
        .next()
        .unwrap();
    assert!(branch.contains("histograms::validation::validate(&args)"));
    assert!(!branch.contains("clickhouse") && !branch.contains("run_load"));
}
