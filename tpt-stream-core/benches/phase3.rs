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

criterion_group!(benches, bench_aggregate_10m);
criterion_main!(benches);
