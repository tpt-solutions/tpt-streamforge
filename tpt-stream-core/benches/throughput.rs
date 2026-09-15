use std::io::Write;
use std::time::{Duration, Instant};

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use tpt_stream_core::Pipeline;

fn generate_csv(path: &str, rows: usize) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    writeln!(f, "id,name,score,active").unwrap();
    for i in 0..rows {
        writeln!(f, "{i},user{i},{}.{} ,{}", i % 1000, i % 97, i % 2 == 0).unwrap();
    }
    f.flush().unwrap();
}

fn csv_throughput(path: &str, chunk_rows: usize) -> u64 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut pipeline = Pipeline::new();
        pipeline.with_chunk_size(chunk_rows).read_csv(path);
        let stats = pipeline.execute().await.unwrap();
        stats.rows
    })
}

fn bench_csv_throughput(c: &mut Criterion) {
    let dir = std::env::temp_dir();
    let path = dir.join("tpt-streamforge-bench.csv");
    let rows = 5_000_000usize;
    let path_str = path.to_string_lossy().into_owned();
    generate_csv(&path_str, rows);

    let mut group = c.benchmark_group("csv_throughput");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    group.bench_function("read_5m_rows_65536", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let count = csv_throughput(&path_str, 65_536);
                total += start.elapsed();
                assert_eq!(count, rows as u64);
            }
            total
        });
    });
    group.finish();
}

criterion_group!(benches, bench_csv_throughput);
criterion_main!(benches);
