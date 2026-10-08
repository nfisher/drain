use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let frame = df!(
        "name" => ["Alice", "Bob", "Charlie"],
        "score" => [10, 20, 30],
    )?;

    println!("{frame}");
    Ok(())
}
