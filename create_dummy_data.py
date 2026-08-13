import polars as pl

N = 1_000_000_000
OUTPUT = "dummy_1b.parquet"

lf = (
    pl.select(
        pl.int_range(0, N, eager=False).alias("id")
    )
    .lazy()
    .with_columns(
        [
            (pl.col("id") % 1_000_000).alias("user_id"),
            (pl.col("id") % 100).alias("category_id"),
            (pl.col("id") * 10).alias("value_int"),
            (pl.col("id") * 0.001).alias("value_float"),
            (pl.col("id") % 2 == 0).alias("is_even"),
            (pl.col("id") % 10 == 0).alias("is_special"),
            (
                pl.lit("dummy_")
                + (pl.col("id") % 1000).cast(pl.String)
            ).alias("label"),
            (pl.col("id") % 50).cast(pl.Int32).alias("group_id"),
            (pl.col("id") % 7).cast(pl.Int8).alias("day_of_week"),
        ]
    )
)

lf.sink_parquet(
    OUTPUT,
    compression="zstd",
    row_group_size=1_000_000,
)

print(f"Created {OUTPUT}")