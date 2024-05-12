///use criterion::{black_box};
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_cast(_: &mut Criterion) {
}

criterion_group!(
    name=group_bench_cast;
    config = Criterion::default();
    targets=bench_cast
);

criterion_main!(group_bench_cast);
