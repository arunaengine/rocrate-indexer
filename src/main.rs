use rocrate_indexer::{CrateIndex, SharedCrateIndex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create shared index
    let index: SharedCrateIndex = CrateIndex::new_in_memory()?.into_shared();

    // Add crates
    {
        let mut idx = index.write().unwrap();
        idx.add_from_source(&rocrate_indexer::loader::CrateSource::Url(
            "https://rocrate.s3.computational.bio.uni-giessen.de/ro-crate-metadata.json"
                .to_string(),
        ))
        .unwrap();
    }

    // Search
    {
        let idx = index.read().unwrap();

        // Find crates mentioning "e.coli"
        let crates = idx.find_crates("e.coli")?;
        println!("Crates with e.coli: {:?}", crates);

        // Find all crates by a specific author
        let author_crates = idx.find_crates_by_entity("https://orcid.org/0000-0001-2345-6789")?;
        println!("Author's crates: {:?}", author_crates);

        // Find Person entities named "Smith"
        let people = idx.search_typed("Person", "Smith", 10)?;
        for hit in people {
            println!("Found {} in crate {}", hit.entity_id, hit.crate_id);
        }
    }

    Ok(())
}
