use std::path::PathBuf;

use clap::{Parser, Subcommand};

use rocrate_indexer::{AddResult, CrateIndex, CrateSource};

#[derive(Parser)]
#[command(name = "rocrate-idx")]
#[command(about = "RO-Crate indexing and search CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add an RO-Crate from a path or URL
    Add {
        /// Path to directory/zip or URL to ro-crate-metadata.json
        source: String,
    },
    /// Search for crates matching a query (Tantivy query syntax)
    Search {
        /// Query string (supports Tantivy syntax)
        ///
        /// Examples:
        ///   "e.coli" - full text search
        ///   "entity_type:Person" - search by type
        ///   "author.name:Smith" - search by nested property
        ///   "name:Test AND entity_type:Dataset" - boolean query
        query: String,
        /// Maximum number of results
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
    /// List all indexed crate IDs
    List,
    /// Show full metadata JSON for a crate
    Show {
        /// Crate ID to show
        crate_id: String,
    },
    /// Remove a crate from the index
    Remove {
        /// Crate ID to remove
        crate_id: String,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut index = CrateIndex::open_or_create()?;

    println!("Opened index with {} crates.", index.list_crates().len());

    match cli.command {
        Commands::Add { source } => {
            let crate_source = parse_source(&source);
            let result = index.add_from_source(&crate_source)?;
            print_add_result(&result, 0);
        }
        Commands::Search { query, limit } => {
            let hits = index.search(&query, limit)?;
            if hits.is_empty() {
                println!("No results found.");
            } else {
                // Deduplicate by crate_id, keep highest score
                let mut seen = std::collections::HashSet::new();
                for hit in hits {
                    if seen.insert(hit.crate_id.clone()) {
                        println!("{}", hit.crate_id);
                    }
                }
            }
        }
        Commands::List => {
            let crates = index.list_crates();
            if crates.is_empty() {
                println!("No crates indexed.");
            } else {
                for crate_id in crates {
                    println!("{}", crate_id);
                }
            }
        }
        Commands::Show { crate_id } => match index.get_crate_json(&crate_id)? {
            Some(json) => println!("{}", json),
            None => {
                eprintln!("Crate not found: {}", crate_id);
                std::process::exit(1);
            }
        },
        Commands::Remove { crate_id } => {
            index.remove(&crate_id)?;
            println!("Removed crate: {}", crate_id);
        }
    }

    Ok(())
}

fn parse_source(source: &str) -> CrateSource {
    if source.starts_with("http://") || source.starts_with("https://") {
        CrateSource::Url(source.to_string())
    } else {
        let path = PathBuf::from(source);
        if path.is_dir() {
            CrateSource::Directory(path)
        } else {
            CrateSource::ZipFile(path)
        }
    }
}

fn print_add_result(result: &AddResult, indent: usize) {
    let prefix = "  ".repeat(indent);
    println!("{}Added: {}", prefix, result.crate_id);
    println!("{}  Entities indexed: {}", prefix, result.entity_count);

    if !result.subcrates.is_empty() {
        println!(
            "{}  Subcrates discovered: {}",
            prefix,
            result.subcrates.len()
        );
        for subcrate in &result.subcrates {
            print_add_result(subcrate, indent + 2);
        }
    }
}
