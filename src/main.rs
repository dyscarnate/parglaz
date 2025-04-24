use clap::Parser;
use std::path::PathBuf;
use std::fs;
use std::fs::File;
use arrow2::io::parquet::read;



#[derive(Parser, Debug)]
#[command(version, about = "simple CLI for parquet analyze")]
struct Args {
    #[arg(required=true, long)]
    path: String,
    #[arg(long, required=false)]
    head: i32,

}




fn main() -> std::io::Result<()> {

    let args = Args::parse();
    
    let abs_path = fs::canonicalize(&PathBuf::from(&args.path))?;

    let mut reader = File::open(&abs_path)?;

    let metadata = read::read_metadata(&mut reader).unwrap();
    let schema = read::infer_schema(&metadata).unwrap();

    let schema = schema.filter(|_index, _field| true);

    let statistics = read::statistics::deserialize(&schema.fields[2], &metadata.row_groups).unwrap();

    println!("{:#?}", statistics);



    print!("{:?}",abs_path);
    Ok(())
}
