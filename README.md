# Pavi

Pavi is a desktop data explorer for **Parquet** files with spreadsheet-like interactions.

## Goals

* Open large Parquet files instantly
* Smooth scrolling with viewport virtualization
* Sort and filter without loading entire datasets
* Column statistics and charts
* Native desktop application written in Rust

## Tech Stack

* Rust
* Apache Arrow
* Apache Parquet
* egui (?)
* Tokio

## Project Structure

```text
crates/
├── cli/        # CLI for development and testing
└── parquet/    # Parquet reader and data source
```

