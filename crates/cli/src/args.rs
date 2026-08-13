use clap::Parser;

#[derive(Parser)]
pub struct Args {
    pub file: String,

    #[arg(long, default_value_t = 10)]
    pub head: usize,

    #[arg(long)]
    pub columns: Option<String>,

    #[arg(long)]
    pub filter: Option<String>,
}
