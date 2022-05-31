use clap::Parser;
use blake2::{Blake2b, Digest, digest::consts::U16};
use core_affinity::CoreId;

use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Parser, Debug)]
struct Args {
    /// Number of cores to use for deduping files
    #[clap(short, long, default_value_t = 4)]
    cores: usize,

    /// Directory contains files to de-duplicate (non-recursive)
    #[clap(short, long, default_value = ".")]
    input_directory: String,

}

/// Worker used to remove duplicate files in `entries` by comparing SHA1 hashes.
///
/// This worker works in chunks. It will deduplicate files in `entries[start:start + count]`
fn worker(entries: Arc<Vec<DirEntry>>, start: usize, count: usize, core_id: CoreId) 
        -> BTreeMap<Vec<u8>, PathBuf> {
    // Pin this worker to this core
    core_affinity::set_for_current(core_id);

    // Collection of unique entries, used to determine if an entry has been seen already 
    let mut seen = BTreeMap::new();

    // Timer used for printing progress
    let time_start = std::time::Instant::now();

    type BlakeHasher = Blake2b<U16>;

    for (i, entry) in entries.iter().skip(start).take(count).enumerate() {
        // Basic log to show progress of this worker
        if i > 0 && i % 1000 == 0 {
            let elapsed = time_start.elapsed();
            println!("{core_id:?}: {i}/{count}: {:6.2} entry/sec",  
                1.0 / (elapsed.as_secs_f64() / i as f64));
        }

        // Get a hasher for the current entry
        let mut hasher = BlakeHasher::new();

        // Get the path of the current entry
        let entry_path = entry.path();

        // Ignore directories since this isn't a recursive worker
        if entry_path.is_dir() {
            continue;
        }

        // Read the contents of the current entry
        let entry_data = std::fs::read(&entry_path);
        if entry_data.is_err() {
            println!("Data Error: {entry_path:?} {entry_data:?}");
            continue;
        }

        // Get the SHA1 of the entry contents
        hasher.update(&entry_data.unwrap());
        let val = hasher.finalize();

        // If this file has been seen before, move it to the duplicate dir
        if let Some(old_path) = seen.insert(val, entry_path) {
            std::fs::remove_file(old_path).expect("Failed to remove file");
        } 
    }

    seen.iter().map(|(k, v)| (k.to_vec(), v.into())).collect()
}

fn main() {
    // Get the command line arguments
    let args = Args::parse();

    // Execute attempt to execute
    let path = Path::new(&args.input_directory);

    // Get the number of entries in the directory
    let entries: Vec<_> = path.read_dir().unwrap()
        .map(|x| x.unwrap())
        .collect();
    let num = entries.len();

    println!("Entries: {num}");

    // Chunk size based on the number of wanted cores, rounding up so that the last core
    // has fewer entries
    let chunk_size = (entries.len() as f64 / args.cores as f64).ceil() as usize;

    // Wrap read-only objects in Arc to pass to the worker threads
    let entries  = Arc::new(entries);

    // Create the collection of threads
    let mut threads = Vec::new();

    // Start each core with the subsection of the total entries
    for core in 0..args.cores {
        let core_id = CoreId { id: usize::from(core) };
        let entries  = entries.clone();

        let thread = std::thread::spawn(move ||  {
            worker(entries, core * chunk_size, chunk_size, core_id)
        });

        threads.push(thread);
    }

    let mut results = Vec::new();
    let mut total_seen = BTreeSet::new();

    // Join all threads
    for thread in threads {
        results.push(thread.join().unwrap());
    }

    // Remove duplicate entries found by each core
    for result in results {
        for (hash, entry) in result {
            if !total_seen.insert(hash) {
                std::fs::remove_file(entry).expect("Failed to remove file");
            }
        }
    }

    // Print remaining number of files
    println!("Remaining files: {}", total_seen.len());
}
