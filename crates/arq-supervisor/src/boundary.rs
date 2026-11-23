//! The parent crate does not link CPython.

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    #[test]
    fn the_manifest_does_not_name_pyo3_or_the_python_bridge() {
        let manifest = include_str!("../Cargo.toml");
        let lower = manifest.to_ascii_lowercase();
        assert!(!lower.contains("pyo3"), "{manifest}");
        assert!(!lower.contains("arq-pybridge"), "{manifest}");
        assert!(manifest.contains("watchexec-supervisor"));
    }

    #[test]
    fn cargo_tree_has_no_pyo3_and_does_link_watchexec() {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.toml");
        let pyo3 = Command::new("cargo")
            .args(["tree", "-p", "arq-supervisor", "-i", "pyo3", "--offline", "--manifest-path"])
            .arg(&manifest)
            .output()
            .expect("cargo tree");
        let stdout = String::from_utf8_lossy(&pyo3.stdout);
        let stderr = String::from_utf8_lossy(&pyo3.stderr);
        assert!(!stdout.to_ascii_lowercase().contains("pyo3"), "stdout={stdout} stderr={stderr}");
        assert!(stdout.trim().is_empty(), "pyo3 tree should be empty: {stdout}");
        let missing = !pyo3.status.success() && stderr.contains("did not match any packages");
        assert!(pyo3.status.success() || missing, "{stderr}");

        let linked = Command::new("cargo")
            .args(["tree", "-p", "arq-supervisor", "-i", "watchexec-supervisor", "--offline", "--manifest-path"])
            .arg(&manifest)
            .output()
            .expect("cargo tree");
        let stdout = String::from_utf8_lossy(&linked.stdout);
        assert!(linked.status.success(), "{}", String::from_utf8_lossy(&linked.stderr));
        assert!(stdout.contains("watchexec-supervisor"), "{stdout}");
        let _ = crate::supervisor_job();
        assert_eq!(crate::spawn::SPAWN_CRATE, "watchexec-supervisor");
    }
}
