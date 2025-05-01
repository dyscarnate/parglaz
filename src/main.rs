use clap::Parser;
use std::fmt::Debug;
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
    head: Option<i32>,


}




fn main() -> std::io::Result<()> {

    let args = Args::parse();
    
    let abs_path = fs::canonicalize(&PathBuf::from(&args.path))?;

    let mut reader = File::open(&abs_path)?;

    let metadata = read::read_metadata(&mut reader).unwrap();
    let schema = read::infer_schema(&metadata).unwrap();

    let schema = schema.filter(|_index, _field| true);

    // read::statistics::deserialize(&schema.fields[1], &metadata.row_groups).unwrap();

    for field in &schema.fields {
        let stats = read::statistics::deserialize(field, &metadata.row_groups).unwrap();
    
        let nulls = stats.null_count;
        let distinct = stats.distinct_count;
        let min = stats.min_value;

    
        let max = stats.max_value;

    
        println!(
            "{:<20}| nulls: {:<12}| distinct: {:<15}| min: {:<20}| max: {:<20}|",
            field.name,
            format!("{:?}", nulls),
            format!("{:?}", distinct),
            format!("{:?}", min),
            format!("{:?}", max),
        );
        
    }
    

    
    Ok(())
}
