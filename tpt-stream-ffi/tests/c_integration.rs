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
    let dll = target_dir.join(format!(
        "{}tpt_stream_ffi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    assert!(dll.exists(), "dll not found at {}", dll.display());
    let header_inc = manifest.join("include");

    // 3. Compile the C program, linking directly against the dll.
    let runner = target_dir.join(format!("tpt_c_integration{}", std::env::consts::EXE_SUFFIX));
    let mut cmd = Command::new("gcc");
    cmd.current_dir(&manifest).args([
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
    ]);
    // On Linux/macOS the linker only records the shared lib's soname, so the
    // runtime loader needs an rpath back to the build dir to find it again.
    if cfg!(target_os = "linux") {
        cmd.arg(format!("-Wl,-rpath,{}", target_dir.display()));
    } else if cfg!(target_os = "macos") {
        cmd.args(["-Wl,-rpath", &target_dir.display().to_string()]);
    }
    let compile = cmd
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
