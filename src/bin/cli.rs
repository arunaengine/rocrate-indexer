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
        /// Show only unique crate IDs (deduplicate by crate)
        #[arg(long)]
        crates_only: bool,
    },
    /// List all indexed crate IDs
    List {
        /// Show detailed info (name, description, path) for each crate
        #[arg(short, long)]
        verbose: bool,
        /// Output as JSON (includes full info for all crates)
        #[arg(long)]
        json: bool,
    },
    /// Show full metadata JSON for a crate
    Show {
        /// Crate ID to show
        crate_id: String,
    },
    /// Show short info (name, description, ancestry path) for a crate
    Info {
        /// Crate ID to get info for
        crate_id: String,
        /// Output as JSON
        #[arg(long)]
        json: bool,
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

    match cli.command {
        Commands::Add { source } => {
            let crate_source = parse_source(&source);
            let result = index.add_from_source(&crate_source)?;
            print_add_result(&result, 0);
        }
        Commands::Search {
            query,
            limit,
            crates_only,
        } => {
            let hits = index.search(&query, limit)?;
            if hits.is_empty() {
                println!("No results found.");
            } else if crates_only {
                // Deduplicate by crate_id
                let mut seen = std::collections::HashSet::new();
                for hit in hits {
                    if seen.insert(hit.crate_id.clone()) {
                        println!("{}", hit.crate_id);
                    }
                }
            } else {
                // Show entity and crate for each hit
                for hit in hits {
                    println!("Entity: {}", hit.entity_id);
                    println!("  Crate: {}", hit.crate_id);
                    println!("  Score: {:.4}", hit.score);
                    println!();
                }
            }
        }
        Commands::List { verbose, json } => {
            let entries = index.list_crate_entries();
            if entries.is_empty() {
                if json {
                    println!("[]");
                } else {
                    println!("No crates indexed.");
                }
            } else if json {
                // Output as JSON array with full info
                let output: Vec<_> = entries
                    .iter()
                    .map(|entry| {
                        serde_json::json!({
                            "crate_id": entry.crate_id,
                            "name": entry.name,
                            "description": entry.description,
                            "full_path": entry.full_path,
                            "is_root": entry.is_root(),
                            "parent_id": entry.parent_id(),
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else if verbose {
                for entry in entries {
                    println!("ID: {}", entry.crate_id);
                    if let Some(ref name) = entry.name {
                        println!("  Name: {}", name);
                    }
                    if let Some(ref desc) = entry.description {
                        // Truncate long descriptions
                        let truncated = if desc.len() > 100 {
                            format!("{}...", &desc[..100])
                        } else {
                            desc.clone()
                        };
                        println!("  Description: {}", truncated);
                    }
                    if entry.full_path.len() > 1 {
                        println!("  Path: {}", entry.full_path.join(" > "));
                    }
                    println!();
                }
            } else {
                let crates = index.list_crates();
                if crates.is_empty() {
                    println!("No crates indexed.");
                } else {
                    for crate_id in crates {
                        println!("{}", crate_id);
                    }
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
        Commands::Info { crate_id, json } => match index.get_crate_info(&crate_id) {
            Some(entry) => {
                if json {
                    // Output as JSON
                    let output = serde_json::json!({
                        "crate_id": entry.crate_id,
                        "name": entry.name,
                        "description": entry.description,
                        "full_path": entry.full_path,
                        "is_root": entry.is_root(),
                        "parent_id": entry.parent_id(),
                    });
                    println!("{}", serde_json::to_string_pretty(&output)?);
                } else {
                    // Human-readable output
                    println!("Crate ID: {}", entry.crate_id);

                    if let Some(ref name) = entry.name {
                        println!("Name: {}", name);
                    } else {
                        println!("Name: (not set)");
                    }

                    if let Some(ref desc) = entry.description {
                        println!("Description: {}", desc);
                    } else {
                        println!("Description: (not set)");
                    }

                    if entry.is_root() {
                        println!("Type: Root crate");
                    } else {
                        println!("Type: Subcrate");
                        println!("Parent: {}", entry.parent_id().unwrap_or("unknown"));
                    }

                    println!("Full path: {}", entry.full_path.join(" > "));
                }
            }
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
            // Use the zip constructor which extracts name from path
            CrateSource::zip(path)
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
