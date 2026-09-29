//! Compiles and runs the C integration smoke test against the generated
//! header and the cdylib. Requires a C compiler on PATH (`gcc`/`cc`, or
//! MinGW-w64 `gcc` on Windows); the test **skips with a message** when none is
//! available rather than failing, so it is safe to wire into CI on every
//! platform.
//!
//! Run with:
//!   cargo test -p tpt-stream-ffi --test c_integration -- --ignored --nocapture
//!
//! Windows note: `rustc` emits a `cdylib` but no import library, so a C linker
//! cannot resolve the exports straight from the `.dll`. This test synthesizes
//! an import library with `dlltool` (part of the MinGW-w64 toolchain that ships
//! in the `windows-latest` runner image) and copies the `.dll` next to the test
//! executable, which is where the Windows loader looks for it. If `dlltool` is
//! missing the test skips with that reason rather than silently passing.

use std::path::{Path, PathBuf};
use std::process::Command;

/// First C compiler on PATH, or `None` with the reason to report.
fn find_cc() -> Result<&'static str, String> {
    for candidate in ["cc", "gcc", "clang"] {
        if Command::new(candidate)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
        {
            return Ok(if candidate == "cc" { "cc" } else { "gcc" });
        }
    }
    Err("no C compiler on PATH (tried cc, gcc, clang)".to_string())
}

fn dll_name() -> String {
    format!(
        "{}tpt_stream_ffi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    )
}

/// Link against the shared library. On Windows that means an import library
/// synthesized from the DLL exports; elsewhere the DLL is passed directly and
/// the loader is pointed back at the build dir.
fn link_args(target_dir: &Path, dll: &Path, cc: &str) -> Result<Vec<String>, String> {
    if cfg!(target_os = "windows") {
        let stem = "tpt_stream_ffi";
        let import_lib = target_dir.join(format!("lib{stem}.dll.a"));
        let status = Command::new("dlltool")
            .args([
                "-d",
                &dll.to_string_lossy(),
                "-D",
                &dll_name(),
                "-l",
                &import_lib.to_string_lossy(),
            ])
            .status()
            .map_err(|_| {
                "dlltool not found (needed to synthesize a Windows import library)".to_string()
            })?;
        if !status.success() {
            return Err("dlltool failed to build an import library".to_string());
        }
        Ok(vec![
            format!("-L{}", target_dir.display()),
            format!("-l{stem}"),
        ])
    } else if cfg!(target_os = "macos") {
        Ok(vec![format!("-Wl,-rpath,{}", target_dir.display())])
    } else if cfg!(target_os = "linux") {
        Ok(vec![
            format!("-Wl,-rpath,{}", target_dir.display()),
            dll.to_string_lossy().into_owned(),
        ])
    } else {
        Ok(vec![dll.to_string_lossy().into_owned()])
    }
}

#[test]
#[ignore]
fn c_integration() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.parent().unwrap().to_path_buf();

    let cc = match find_cc() {
        Ok(cc) => cc,
        Err(why) => {
            println!("SKIPPED c_integration: {why}");
            return;
        }
    };

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
    let dll = target_dir.join(dll_name());
    assert!(dll.exists(), "dll not found at {}", dll.display());
    let header_inc = manifest.join("include");

    let extra = match link_args(&target_dir, &dll, cc) {
        Ok(extra) => extra,
        Err(why) => {
            println!("SKIPPED c_integration: {why}");
            return;
        }
    };

    // 3. Compile the C program.
    let runner = target_dir.join(format!("tpt_c_integration{}", std::env::consts::EXE_SUFFIX));
    let mut cmd = Command::new(cc);
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
    ]);
    cmd.args(&extra);
    let compile = cmd
        .status()
        .expect("failed to run the C compiler (is one on PATH?)");
    assert!(compile.success(), "{cc} compilation failed");

    // 4. Run it. The exe lives next to the dll so the Windows loader finds it.
    let run = Command::new(&runner).output().expect("failed to run ctest");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "ctest failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    println!("{stdout}");
}

/// The header must give each handle kind its own opaque type, so handing a
/// batch to a pipeline function is a compile-time error rather than UB.
/// Syntax-only (no linking), so it needs a C compiler but no DLL.
#[test]
fn header_handle_types_are_distinct() {
    let cc = match find_cc() {
        Ok(cc) => cc,
        Err(why) => {
            println!("SKIPPED header_handle_types_are_distinct: {why}");
            return;
        }
    };
    let include = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("include");
    let dir = std::env::temp_dir().join(format!("tpt-ffi-hdr-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let compile = |name: &str, body: &str| {
        let src = dir.join(name);
        std::fs::write(&src, format!("#include \"tpt_streamforge.h\"\n{body}\n")).unwrap();
        Command::new(cc)
            .args(["-std=c11", "-Wall", "-Werror", "-fsyntax-only", "-I"])
            .arg(&include)
            .arg(&src)
            .output()
            .expect("failed to run the C compiler")
    };

    let good = compile(
        "good.c",
        "void ok(TptPipeline *p, TptRecordBatch *b) { \
           tpt_pipeline_free(p); tpt_record_batch_free(b); }",
    );
    assert!(
        good.status.success(),
        "correct handle use must compile: {}",
        String::from_utf8_lossy(&good.stderr)
    );

    let bad = compile(
        "bad.c",
        "void oops(TptRecordBatch *b) { tpt_pipeline_free(b); }",
    );
    assert!(
        !bad.status.success(),
        "passing a batch handle to a pipeline function must not compile"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
