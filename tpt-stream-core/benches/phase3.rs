use std::io::Write;
use std::time::{Duration, Instant};

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use tpt_stream_core::{AggSpec, Pipeline};

fn generate_grouped_csv(path: &str, rows: usize) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    writeln!(f, "dept,score").unwrap();
    for i in 0..rows {
        writeln!(f, "{},{}", i % 25, i * 37 % 1_000).unwrap();
    }
    f.flush().unwrap();
}

fn aggregate_10m(path: &str, chunk_rows: usize) -> u64 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut pipeline = Pipeline::new();
        pipeline
            .with_chunk_size(chunk_rows)
            .read_csv(path)
            .aggregate(&["dept"], &[AggSpec::sum("score"), AggSpec::count_all("n")]);
        let stats = pipeline.execute().await.unwrap();
        stats.rows
    })
}

fn bench_aggregate_10m(c: &mut Criterion) {
    let dir = std::env::temp_dir();
    let path = dir.join("tpt-streamforge-phase3-aggregate.csv");
    let rows = 10_000_000usize;
    let path_str = path.to_string_lossy().into_owned();
    generate_grouped_csv(&path_str, rows);

    let mut group = c.benchmark_group("phase3_aggregate");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    group.bench_function("group_sum_25_groups_10m_rows", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let count = aggregate_10m(&path_str, 65_536);
                total += start.elapsed();
                assert_eq!(count, rows as u64);
            }
            total
        });
    });
    group.finish();
}

/// A wide numeric CSV: the shape that stresses per-row parsing rather than
/// per-field string handling, and the case whole-buffer ingestion is tuned for.
fn generate_wide_numeric(path: &str, rows: usize, columns: usize) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    let header: Vec<String> = (0..columns).map(|c| format!("c{c}")).collect();
    writeln!(f, "{}", header.join(",")).unwrap();
    for r in 0..rows {
        let row: Vec<String> = (0..columns)
            .map(|c| ((r * (c + 7)) % 100_000).to_string())
            .collect();
        writeln!(f, "{}", row.join(",")).unwrap();
    }
    f.flush().unwrap();
}

/// Compare the sequential and parallel whole-buffer CSV ingestion paths on the
/// same in-memory input.
///
/// The parallel path splits the buffer at record boundaries and parses the
/// slices across the rayon pool, so this isolates the Phase 3 boundary-scan +
/// rayon work: both entries do identical parsing, differing only in threading.
/// The sequential entry forces the old path by staying under the parallel
/// threshold, so run this on a multi-core machine for a meaningful number.
fn bench_csv_ingest(c: &mut Criterion) {
    let dir = std::env::temp_dir();
    let path = dir.join("tpt-streamforge-csv-ingest-wide.csv");
    let rows = 400_000usize;
    let columns = 16usize;
    let path_str = path.to_string_lossy().into_owned();
    generate_wide_numeric(&path_str, rows, columns);
    let text = std::fs::read_to_string(&path).unwrap();
    // Comfortably above the 1 MiB threshold so `csv_to_batches` takes the
    // parallel path; assert it rather than trusting the fixture size.
    assert!(text.len() >= tpt_stream_core::source::PARALLEL_CSV_MIN_BYTES);
    let chunk_rows = 65_536usize;

    let mut group = c.benchmark_group("csv_ingest_wide_numeric");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    let expected_rows = rows;
    group.bench_function("parallel", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let batches = tpt_stream_core::source::csv_to_batches(&text, chunk_rows).unwrap();
                total += start.elapsed();
                assert_eq!(
                    batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    expected_rows
                );
            }
            total
        });
    });

    group.bench_function("sequential", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let batches =
                    tpt_stream_core::source::csv_to_batches_sequential(&text, chunk_rows).unwrap();
                total += start.elapsed();
                assert_eq!(
                    batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    expected_rows
                );
            }
            total
        });
    });
    group.finish();
}

criterion_group!(benches, bench_aggregate_10m, bench_csv_ingest);
criterion_main!(benches);
