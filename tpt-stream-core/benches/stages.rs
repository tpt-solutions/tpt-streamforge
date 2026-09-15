//! Criterion benchmark covering every pipeline stage on a shared 1M-row CSV
//! fixture. Numbers are published in the project README.
//!
//! Run with: `cargo bench --bench stages --features zstd -p tpt-stream-core`

use std::io::Write;
use std::time::{Duration, Instant};

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use tpt_stream_core::{AggSpec, JoinType, Pipeline};

const ROWS: usize = 1_000_000;

/// Generate the shared fixture once: `id,k,score,name,flag` with 50 groups.
fn fixture_path() -> String {
    let dir = std::env::temp_dir();
    let path = dir.join("tpt-streamforge-stages-1m.csv");
    let path_str = path.to_string_lossy().into_owned();
    if !path.exists() {
        let mut w =
            std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(&path).unwrap());
        writeln!(w, "id,k,score,name,flag").unwrap();
        for i in 0..ROWS {
            writeln!(
                w,
                "{i},{},{:.2},user{i},{}",
                i % 50,
                ((i * 7919) % 100_000) as f64 / 100.0,
                if i % 3 == 0 { "true" } else { "false" }
            )
            .unwrap();
        }
        w.flush().unwrap();
    }
    path_str
}

/// Build the small join build-side relation (`k,name` for the first 50 keys).
fn join_side_path() -> String {
    let dir = std::env::temp_dir();
    let path = dir.join("tpt-streamforge-stages-join.csv");
    let path_str = path.to_string_lossy().into_owned();
    if !path.exists() {
        let mut w = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
        writeln!(w, "k,label").unwrap();
        for k in 0..50i64 {
            writeln!(w, "{k},label{k}").unwrap();
        }
        w.flush().unwrap();
    }
    path_str
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn run(pipeline: &mut Pipeline) -> u64 {
    let rt = runtime();
    rt.block_on(async {
        let stats = pipeline.execute().await.unwrap();
        stats.rows
    })
}

fn bench_stages(c: &mut Criterion) {
    let csv = fixture_path();
    let join_csv = join_side_path();
    let out = std::env::temp_dir().join("tpt-streamforge-stages-out.csv");
    let out_str = out.to_string_lossy().into_owned();

    let mut group = c.benchmark_group("pipeline_stages");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));

    group.bench_function("source_csv_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                assert_eq!(run(p.with_chunk_size(65_536).read_csv(&csv)), ROWS as u64);
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("filter_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                assert_eq!(
                    run(p.read_csv(&csv).filter_expr("score > 500")),
                    ROWS as u64
                );
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("map_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                assert_eq!(
                    run(p.read_csv(&csv).map_expr(&[("doubled", "score * 2")])),
                    ROWS as u64
                );
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("select_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                assert_eq!(
                    run(p.read_csv(&csv).select(&["id", "score", "name"])),
                    ROWS as u64
                );
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("aggregate_groupby_sum_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                assert_eq!(
                    run(p.read_csv(&csv).aggregate(
                        &["k"],
                        &[
                            AggSpec::sum("score"),
                            AggSpec::avg("score"),
                            AggSpec::count_all("n")
                        ],
                    )),
                    ROWS as u64
                );
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("sort_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                assert_eq!(run(p.read_csv(&csv).sort_by(&["score"])), ROWS as u64);
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("dedup_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                // `name` is unique per row: full bloom + exact path.
                assert_eq!(run(p.read_csv(&csv).dedup(&["name"])), ROWS as u64);
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("join_hash_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                p.read_csv(&csv)
                    .join_csv(&join_csv, &["k"], &["k"], JoinType::Inner)
                    .unwrap();
                assert_eq!(run(&mut p), ROWS as u64);
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("csv_to_csv_1m", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let mut p = Pipeline::new();
                p.read_csv(&csv).write_csv(&out_str);
                assert_eq!(run(&mut p), ROWS as u64);
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

criterion_group!(benches, bench_stages);
criterion_main!(benches);
