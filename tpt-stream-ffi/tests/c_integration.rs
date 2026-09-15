//! Compiles and runs the C integration smoke test against the generated
//! header and the cdylib. Requires a C compiler (`gcc`/`cc`) on PATH.
//!
//! Run with:
//!   cargo test -p tpt-stream-ffi --test c_integration -- --ignored

use std::path::PathBuf;
use std::process::Command;

#[test]
#[ignore]
fn c_integration() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.parent().unwrap().to_path_buf();

    // 1. Ensure the cdylib is freshly built.
    let status = Command::new("cargo")
        .current_dir(&workspace)
        .args(["build", "-p", "tpt-stream-ffi"])
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "cargo build failed");

    // 2. Locate the build artifacts.
    let target_dir = match std::env::var("CARGO_TARGET_DIR") {
        Ok(dir) => PathBuf::from(dir).join("debug"),
        Err(_) => workspace.join("target").join("debug"),
    };
    let dll = target_dir.join("tpt_stream_ffi.dll");
    assert!(dll.exists(), "dll not found at {}", dll.display());
    let header_inc = manifest.join("include");

    // 3. Compile the C program, linking directly against the dll.
    let runner = target_dir.join("tpt_c_integration.exe");
    let compile = Command::new("gcc")
        .current_dir(&manifest)
        .args([
            "-std=c11",
            "-Wall",
            "-Wextra",
            "-Werror",
            "tests/c_integration/main.c",
            "-I",
            header_inc.to_str().unwrap(),
            "-o",
            runner.to_str().unwrap(),
            dll.to_str().unwrap(),
        ])
        .status()
        .expect("failed to run gcc (is a C compiler on PATH?)");
    assert!(compile.success(), "gcc compilation failed");

    // 4. Run it. The exe lives next to the dll so Windows finds it.
    let run = Command::new(&runner).output().expect("failed to run ctest");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "ctest failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    println!("{stdout}");
}
