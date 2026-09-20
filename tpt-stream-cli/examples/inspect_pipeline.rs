//! Print the plan of a pipeline TOML, run it, and report per-stage stats.
//!
//! This drives the same library API that `tptforge run` uses, so it is also a
//! template for embedding the CLI's spec format in your own binary.
//!
//! ```sh
//! cargo run -p tpt-stream-cli --example inspect_pipeline
//! cargo run -p tpt-stream-cli --example inspect_pipeline -- path/to/pipeline.toml
//! ```

use std::path::PathBuf;

use anyhow::{Context, Result};
use tpt_stream_cli::{build_pipeline, parse_pipeline_toml};

#[tokio::main]
async fn main() -> Result<()> {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    // No path on the command line means "use the bundled example", whose
    // source/sink paths are relative to this crate — so run from the crate
    // root rather than wherever `cargo run` was invoked.
    let path: PathBuf = match std::env::args().nth(1) {
        Some(arg) => PathBuf::from(arg),
        None => {
            std::env::set_current_dir(manifest_dir)
                .with_context(|| format!("changing directory to {}", manifest_dir.display()))?;
            PathBuf::from("examples/pipeline.toml")
        }
    };

    let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let spec = parse_pipeline_toml(&text).with_context(|| format!("parsing {}", path.display()))?;
    let mut pipeline = build_pipeline(&spec)?;

    println!("plan for {}:\n{}", path.display(), pipeline.explain());

    let stats = pipeline.execute().await?;
    println!(
        "{} rows in {} batch(es), {} bytes out, in {:.1?}",
        stats.rows, stats.batches, stats.bytes_out, stats.elapsed
    );

    for stage in pipeline.stage_stats() {
        println!(
            "  stage {}: {} rows in / {} rows out in {:.1?}",
            stage.name, stage.rows_in, stage.rows_out, stage.elapsed
        );
    }
    Ok(())
}
