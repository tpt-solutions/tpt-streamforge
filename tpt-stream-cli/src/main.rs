use anyhow::{Context, Result};
use clap::Parser;
use tpt_stream_cli::{preview_command, run_command, schema_command, Cli, Command};

#[tokio::main]
async fn main() {
    if let Err(err) = real_main().await {
        eprintln!("tptforge: {err:#}");
        std::process::exit(1);
    }
}

async fn real_main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run { pipeline, quiet } => {
            let summary = run_command(&pipeline, quiet)
                .await
                .with_context(|| format!("running {}", pipeline.display()))?;
            println!("{summary}");
        }
        Command::Schema { input, rows } => {
            print!("{}", schema_command(&input, rows).await?);
        }
        Command::Preview { input, num } => {
            print!("{}", preview_command(&input, num).await?);
        }
    }
    Ok(())
}
