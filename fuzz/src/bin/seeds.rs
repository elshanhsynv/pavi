//! Writes the seed corpus into fuzz/corpus/<target>/.
//! Run: cargo +nightly run --manifest-path fuzz/Cargo.toml --bin seeds

use std::{fs, path::Path};

fn write(dir: &Path, name: &str, bytes: &[u8]) {
    fs::create_dir_all(dir).expect("create corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed");
}

fn main() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
    for (name, bytes) in pavi_fuzz::parquet_seeds() {
        write(&corpus.join("parquet_file"), &name, &bytes);
    }
    for (index, text) in pavi_fuzz::FILTER_SEEDS.iter().enumerate() {
        write(
            &corpus.join("filter_expr"),
            &format!("seed-{index}"),
            text.as_bytes(),
        );
    }
    for (index, text) in pavi_fuzz::SQL_SEEDS.iter().enumerate() {
        write(
            &corpus.join("sql_query"),
            &format!("seed-{index}"),
            text.as_bytes(),
        );
    }
    println!("seeds written to {}", corpus.display());
}
