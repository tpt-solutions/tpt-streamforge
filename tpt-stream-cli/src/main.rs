use std::io::Write as _;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tpt_stream_cli::tools;
use tpt_stream_cli::{
    completions_command, explain_command, man_command, preview_command, run_with, schema_command,
    sql_command, validate_command, Cli, Command, RunOptions,
};

#[tokio::main]
async fn main() {
    match real_main().await {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("tptforge: {err:#}");
            std::process::exit(1);
        }
    }
}

/// Exit codes: 0 success, 1 error, 2 schema drift (`schema --against`).
/// `diff --exit-code` and `doctor` exit 1 on differences / failed checks.
async fn real_main() -> Result<i32> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run {
            pipeline,
            quiet,
            metrics,
            metrics_name,
            metrics_allow_remote,
            manifest,
            dry_run,
            watch,
        } => {
            let options = RunOptions {
                quiet,
                metrics,
                metrics_name,
                metrics_allow_remote,
                manifest,
                dry_run,
            };
            if watch {
                if dry_run {
                    bail!("--watch and --dry-run cannot be combined");
                }
                tools::watch_command(&pipeline, &options, Duration::from_millis(500), None).await?;
            } else {
                let summary = run_with(&pipeline, &options)
                    .await
                    .with_context(|| format!("running {}", pipeline.display()))?;
                if dry_run {
                    print!("{summary}");
                } else {
                    println!("{summary}");
                }
            }
        }
        Command::Validate { pipeline, no_env } => {
            print!("{}", validate_command(&pipeline, no_env)?);
        }
        Command::Explain { pipeline, no_env } => {
            print!("{}", explain_command(&pipeline, no_env)?);
        }
        Command::SchemaJson => {
            print!("{}", tpt_stream_cli::schema_json::pipeline_schema_string());
        }
        Command::Completions { shell } => {
            print!("{}", completions_command(shell));
        }
        Command::Man { out_dir } => {
            print!("{}", man_command(out_dir.as_deref())?);
        }
        Command::Init {
            input,
            out,
            force,
            rows,
        } => {
            let text = tools::init_toml(&input, rows).await?;
            match out {
                Some(path) => {
                    if path.exists() && !force {
                        bail!(
                            "{} already exists (use --force to overwrite)",
                            path.display()
                        );
                    }
                    std::fs::write(&path, text)
                        .with_context(|| format!("writing {}", path.display()))?;
                    println!("wrote {}", path.display());
                }
                None => print!("{text}"),
            }
        }
        Command::Doctor { pipeline } => {
            let report = tools::doctor_command(pipeline.as_deref());
            print!("{}", report.text);
            if report.failures > 0 {
                return Ok(1);
            }
        }
        Command::Convert {
            input,
            output,
            on_error,
            zstd,
        } => {
            println!(
                "{}",
                tools::convert_command(&input, &output, &on_error, zstd).await?
            );
        }
        Command::Diff {
            a,
            b,
            key,
            out,
            exit_code,
        } => {
            let summary = match out {
                Some(path) => {
                    let file = std::fs::File::create(&path)
                        .with_context(|| format!("creating {}", path.display()))?;
                    let mut writer = std::io::BufWriter::new(file);
                    tools::diff_command(&a, &b, &key, &mut writer).await?
                }
                None => {
                    let stdout = std::io::stdout();
                    let mut lock = std::io::BufWriter::new(stdout.lock());
                    let summary = tools::diff_command(&a, &b, &key, &mut lock).await?;
                    lock.flush()?;
                    summary
                }
            };
            eprintln!("{summary}");
            if exit_code && summary.differs() {
                return Ok(1);
            }
        }
        Command::Sql {
            query,
            out,
            on_error,
        } => {
            let output = sql_command(&query, out.as_deref(), &on_error).await?;
            if output.ends_with('\n') {
                print!("{output}");
            } else {
                println!("{output}");
            }
        }
        Command::Schema {
            input,
            rows,
            save,
            against,
        } => {
            if let Some(path) = save {
                println!("{}", tools::schema_save(&input, rows, &path).await?);
            } else if let Some(path) = against {
                let drift = tools::schema_against(&input, rows, &path).await?;
                if drift.drifted {
                    eprint!("{}", drift.report);
                    return Ok(2);
                }
                print!("{}", drift.report);
            } else {
                print!("{}", schema_command(&input, rows).await?);
            }
        }
        Command::Preview { input, num } => {
            print!("{}", preview_command(&input, num).await?);
        }
    }
    Ok(0)
}
