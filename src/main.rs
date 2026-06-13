// dedoop — High-performance duplicate file finder
// =====================================================
// Algorithm pipeline:
//   1. Walk      → rayon + walkdir (parallel readdir)
//   2. Inode     → HashMap<(dev,ino)> (hard‑link dedup, zero I/O)
//   3. Size      → HashMap<size> → discard singletons
//   4. Byte‑by‑byte → mmap + memcmp for groups ≤ 8 files (jdupes‑style)
//   5. Partial   → BLAKE3 at 3 offsets (start / mid / end)
//   6. Full      → BLAKE3 + mmap on survivors
//   7. Output    → symlink tree with per‑group original/duplicates folders

use blake3::Hasher;
use clap::Parser;
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::os::unix::fs::{symlink, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use walkdir::WalkDir;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const PARTIAL_SIZE: usize = 64 * 1024;        // 64 KiB per sample point
const MMAP_THRESHOLD: u64 = 16 * 1024;        // mmap for files ≥ 16 KiB
const BYTE_CMP_THRESHOLD: usize = 8;          // byte‑compare groups ≤ 8 files
const PROGRESS_EVERY: usize = 2000;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
#[derive(Parser, Debug)]
#[command(
    name = "dedoop",
    about = "Find duplicate files by content — Rust edition",
)]
struct Args {
    /// First directory to scan (required)
    folder1: PathBuf,

    /// Second directory to scan (optional — if omitted, finds duplicates within folder1 only)
    folder2: Option<PathBuf>,

    #[arg(short = 'o', long, default_value = "./duplicates_output")]
    output: PathBuf,

    #[arg(short = 's', long, default_value = "1")]
    min_size: u64,

    #[arg(short = 'w', long)]
    workers: Option<usize>,

    #[arg(short = 'p', long)]
    prefer: Option<PathBuf>,

    /// Include hidden files and folders (names starting with .)
    #[arg(long)]
    hidden: bool,

    #[arg(long)]
    delete: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long, default_value = "4")]
    hdd_workers: usize,
}

// ---------------------------------------------------------------------------
// Terminal colour helpers
// ---------------------------------------------------------------------------
fn is_tty() -> bool { std::io::stdout().is_terminal() }
fn dim(s: &str) -> String   { if is_tty() { format!("\x1b[90m{s}\x1b[0m") } else { s.into() } }
fn green(s: &str) -> String { if is_tty() { format!("\x1b[92m{s}\x1b[0m") } else { s.into() } }
fn yellow(s: &str) -> String{ if is_tty() { format!("\x1b[93m{s}\x1b[0m") } else { s.into() } }
fn cyan_bold(s: &str)->String{if is_tty() { format!("\x1b[1;96m{s}\x1b[0m") } else { s.into() } }

fn format_bytes(n: u64) -> String {
    const U: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = n as f64;
    for unit in U {
        if v < 1024.0 { return format!("{v:.2} {unit}"); }
        v /= 1024.0;
    }
    format!("{v:.2} EiB")
}

fn timestamp() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let s = d.as_secs();
    format!("{:02}:{:02}:{:02}", (s/3600)%24, (s/60)%60, s%60)
}

// ---------------------------------------------------------------------------
// File entry
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
struct FileEntry {
    path: PathBuf,
    dev: u64,
    ino: u64,
    size: u64,
    on_hdd: bool,
}

impl FileEntry {
    fn from_path(p: &Path, min_size: u64, hdd_devs: &HashSet<u64>) -> Option<Self> {
        let meta = p.symlink_metadata().ok()?;
        if !meta.is_file() || meta.len() < min_size { return None; }
        // Always store absolute path so symlinks resolve correctly
        let abs = std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
        Some(Self {
            path: abs,
            dev: meta.dev(),
            ino: meta.ino(),
            size: meta.len(),
            on_hdd: hdd_devs.contains(&meta.dev()),
        })
    }
}

// ---------------------------------------------------------------------------
// Storage detection
// ---------------------------------------------------------------------------
fn detect_hdd_devices() -> HashSet<u64> {
    let mut hdd = HashSet::new();
    let dir = match fs::read_dir("/sys/block") { Ok(d) => d, Err(_) => return hdd };
    for entry in dir.flatten() {
        let rot = entry.path().join("queue/rotational");
        if let Ok(c) = fs::read_to_string(&rot) {
            if c.trim() == "1" {
                let dev = Path::new("/dev").join(entry.file_name());
                if let Ok(m) = dev.metadata() { hdd.insert(m.dev()); }
            }
        }
    }
    hdd
}

// ---------------------------------------------------------------------------
// Stage 1 — Parallel file collection
// ---------------------------------------------------------------------------
fn walk_subtree(root: &Path, skip_hidden: bool, hdd_devs: &HashSet<u64>) -> Vec<FileEntry> {
    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| {
            if skip_hidden {
                e.file_name().to_str().map_or(false, |s| !s.starts_with('.'))
            } else { true }
        })
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && !e.path_is_symlink())
        .filter_map(|e| FileEntry::from_path(e.path(), 1, hdd_devs))
        .collect()
}

fn collect_files(folders: &[PathBuf], skip_hidden: bool) -> Vec<FileEntry> {
    let hdd_devs = detect_hdd_devices();
    let mut top_dirs: Vec<PathBuf> = Vec::new();
    let mut direct: Vec<FileEntry> = Vec::new();

    for folder in folders {
        if !folder.exists() {
            eprintln!("{} Folder not found: {}", yellow("⚠"), folder.display());
            continue;
        }
        let entries: Vec<_> = match fs::read_dir(folder) {
            Ok(e) => e.filter_map(|x| x.ok()).collect(),
            Err(_) => continue,
        };
        for entry in entries {
            let p = entry.path();
            let ft = match entry.file_type() { Ok(t) => t, Err(_) => continue };
            let hidden = p.file_name()
                .and_then(|n| n.to_str())
                .map_or(false, |s| s.starts_with('.'));
            if ft.is_dir() {
                if skip_hidden && hidden { continue; }
                top_dirs.push(p);
            } else if ft.is_file() && !ft.is_symlink() {
                if skip_hidden && hidden { continue; }
                if let Some(fe) = FileEntry::from_path(&p, 1, &hdd_devs) {
                    direct.push(fe);
                }
            }
        }
    }

    let subtree_files: Vec<Vec<FileEntry>> = top_dirs
        .par_iter()
        .map(|d| walk_subtree(d, skip_hidden, &hdd_devs))
        .collect();

    let mut all = direct;
    for mut chunk in subtree_files { all.append(&mut chunk); }
    all
}

// ---------------------------------------------------------------------------
// Stage 2 — Inode + size grouping
// ---------------------------------------------------------------------------
struct FilteredGroups {
    size_groups: BTreeMap<u64, Vec<FileEntry>>,
    inode_dupes_saved: u64,
    inode_expand: HashMap<PathBuf, Vec<PathBuf>>,
}

fn filter_by_inode_and_size(entries: Vec<FileEntry>) -> FilteredGroups {
    let mut inode_map: HashMap<(u64, u64), Vec<FileEntry>> = HashMap::new();
    for e in entries {
        inode_map.entry((e.dev, e.ino)).or_default().push(e);
    }

    let mut unique: Vec<FileEntry> = Vec::new();
    let mut inode_expand: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
    let mut inode_dupes_saved: u64 = 0;

    for ((_, _), group) in inode_map {
        let rep = group[0].clone();
        if group.len() > 1 {
            inode_expand.insert(rep.path.clone(),
                group.iter().map(|e| e.path.clone()).collect());
            inode_dupes_saved += (group.len() - 1) as u64;
        }
        unique.push(rep);
    }

    let mut size_map: BTreeMap<u64, Vec<FileEntry>> = BTreeMap::new();
    for e in unique {
        size_map.entry(e.size).or_default().push(e);
    }
    size_map.retain(|_, v| v.len() > 1);

    FilteredGroups { size_groups: size_map, inode_dupes_saved, inode_expand }
}

// ---------------------------------------------------------------------------
// Stage 3 — Byte‑by‑byte comparison (jdupes‑style)
// ---------------------------------------------------------------------------
fn byte_compare_groups(
    mut entries: Vec<FileEntry>,
) -> (Vec<Vec<PathBuf>>, Vec<FileEntry>) {
    if entries.len() <= 1 { return (vec![], entries); }

    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let len = entries.len();
    let mut matched = vec![false; len];
    let mut dup_groups: Vec<Vec<PathBuf>> = Vec::new();
    let mut remaining: Vec<FileEntry> = Vec::new();

    for i in 0..len {
        if matched[i] { continue; }
        let ref_path = &entries[i].path;

        let ref_file = match fs::File::open(ref_path) {
            Ok(f) => f, Err(_) => { remaining.push(entries[i].clone()); continue; }
        };
        let ref_map = match unsafe { Mmap::map(&ref_file) } {
            Ok(m) => m, Err(_) => { remaining.push(entries[i].clone()); continue; }
        };
        let ref_bytes: &[u8] = &ref_map;

        let mut group = vec![ref_path.clone()];
        matched[i] = true;

        for j in (i + 1)..len {
            if matched[j] { continue; }
            let cmp_path = &entries[j].path;
            let cmp_file = match fs::File::open(cmp_path) {
                Ok(f) => f, Err(_) => continue,
            };
            let cmp_map = match unsafe { Mmap::map(&cmp_file) } {
                Ok(m) => m, Err(_) => continue,
            };

            if ref_bytes == &cmp_map[..] {
                group.push(cmp_path.clone());
                matched[j] = true;
            }
        }

        if group.len() > 1 { dup_groups.push(group); }
        else { remaining.push(entries[i].clone()); }
    }

    (dup_groups, remaining)
}

// ---------------------------------------------------------------------------
// Stage 4 — Multi‑offset partial BLAKE3 hash
// ---------------------------------------------------------------------------
fn partial_hash(path: &Path, size: u64) -> Option<blake3::Hash> {
    let mut file = fs::File::open(path).ok()?;
    let mut hasher = Hasher::new();
    let mut buf = vec![0u8; PARTIAL_SIZE];

    // Start block
    let read_len = (PARTIAL_SIZE as u64).min(size) as usize;
    let n = file.read(&mut buf[..read_len]).ok()?;
    hasher.update(&buf[..n]);

    if size > PARTIAL_SIZE as u64 * 3 {
        // Middle block
        let mid = (size / 2).saturating_sub(PARTIAL_SIZE as u64 / 2);
        io::Seek::seek(&mut file, io::SeekFrom::Start(mid)).ok()?;
        let n = file.read(&mut buf).ok()?;
        hasher.update(&buf[..n]);

        // End block
        let end = size.saturating_sub(PARTIAL_SIZE as u64);
        io::Seek::seek(&mut file, io::SeekFrom::Start(end)).ok()?;
        let n = file.read(&mut buf).ok()?;
        hasher.update(&buf[..n]);
    } else if size > PARTIAL_SIZE as u64 {
        // End block only
        let end = size.saturating_sub(PARTIAL_SIZE as u64);
        io::Seek::seek(&mut file, io::SeekFrom::Start(end)).ok()?;
        let remaining = (size - end) as usize;
        let n = file.read(&mut buf[..remaining]).ok()?;
        hasher.update(&buf[..n]);
    }

    Some(hasher.finalize())
}

// ---------------------------------------------------------------------------
// Stage 5 — Full BLAKE3 hash with mmap
// ---------------------------------------------------------------------------
fn full_hash(path: &Path, size: u64) -> Option<blake3::Hash> {
    let file = fs::File::open(path).ok()?;

    if size >= MMAP_THRESHOLD {
        let mmap = unsafe { Mmap::map(&file).ok()? };
        let mut hasher = Hasher::new();
        hasher.update(&mmap[..]);
        Some(hasher.finalize())
    } else {
        let mut hasher = Hasher::new();
        let mut reader = io::BufReader::new(file);
        io::copy(&mut reader, &mut hasher).ok()?;
        Some(hasher.finalize())
    }
}

// ---------------------------------------------------------------------------
// Pipeline: partial → full hash
// ---------------------------------------------------------------------------
fn hash_pipeline(
    size_groups: BTreeMap<u64, Vec<FileEntry>>,
    inode_expand: &HashMap<PathBuf, Vec<PathBuf>>,
) -> (HashMap<blake3::Hash, Vec<PathBuf>>, u64) {
    let total: usize = size_groups.values().map(|v| v.len()).sum();
    eprintln!("{} Pass 1 — multi‑offset partial BLAKE3 on {total} files …", dim(&timestamp()));

    let counter = AtomicU64::new(0);
    let all_entries: Vec<&FileEntry> = size_groups.values().flatten().collect();

    // Partial hashes — parallel via rayon
    let partial_results: Vec<(PathBuf, blake3::Hash)> = all_entries
        .par_iter()
        .filter_map(|e| {
            let h = partial_hash(&e.path, e.size)?;
            let cnt = counter.fetch_add(1, Ordering::Relaxed);
            if cnt % PROGRESS_EVERY as u64 == 0 {
                eprintln!("{}   partial … {cnt}/{total}", dim(&timestamp()));
            }
            Some((e.path.clone(), h))
        })
        .collect();

    // Size lookup
    let size_of: HashMap<PathBuf, u64> = all_entries
        .iter().map(|e| (e.path.clone(), e.size)).collect();

    // Re‑group by (size, partial_hash)
    let mut partial_groups: HashMap<(u64, blake3::Hash), Vec<PathBuf>> = HashMap::new();
    for (path, hash) in &partial_results {
        if let Some(&sz) = size_of.get(path) {
            partial_groups.entry((sz, *hash)).or_default().push(path.clone());
        }
    }
    partial_groups.retain(|_, v| v.len() > 1);

    let remaining: usize = partial_groups.values().map(|v| v.len()).sum();
    eprintln!("{} After partial hash: {remaining} files in {} groups → full hash",
              dim(&timestamp()), partial_groups.len());

    if partial_groups.is_empty() { return (HashMap::new(), 0); }

    // Full hashes — parallel via rayon
    let all_paths: Vec<PathBuf> = partial_groups.values().flatten().cloned().collect();
    let full_counter = AtomicU64::new(0);

    let full_results: Vec<(PathBuf, blake3::Hash)> = all_paths
        .par_iter()
        .filter_map(|path| {
            let sz = *size_of.get(path)?;
            let h = full_hash(path, sz)?;
            let cnt = full_counter.fetch_add(1, Ordering::Relaxed);
            if cnt % (PROGRESS_EVERY as u64 / 4) == 0 {
                eprintln!("{}   full … {cnt}/{remaining}", dim(&timestamp()));
            }
            Some((path.clone(), h))
        })
        .collect();

    // Final grouping with inode expansion
    let mut final_groups: HashMap<blake3::Hash, Vec<PathBuf>> = HashMap::new();
    for (path, hash) in full_results {
        let siblings = inode_expand.get(&path).cloned().unwrap_or_else(|| vec![path]);
        final_groups.entry(hash).or_default().extend(siblings);
    }
    final_groups.retain(|_, v| v.len() > 1);

    (final_groups, full_counter.load(Ordering::Relaxed))
}

// ---------------------------------------------------------------------------
// Original selection
// ---------------------------------------------------------------------------
fn pick_original(group: &[PathBuf], prefer_folder: &Path) -> (PathBuf, Vec<PathBuf>) {
    if group.len() <= 1 { return (group[0].clone(), vec![]); }

    for p in group {
        if p.starts_with(prefer_folder) {
            let orig = p.clone();
            let rest: Vec<_> = group.iter().filter(|x| *x != &orig).cloned().collect();
            return (orig, rest);
        }
    }

    let mut sorted = group.to_vec();
    sorted.sort_by_key(|p| p.as_os_str().len());
    let orig = sorted[0].clone();
    let rest = sorted[1..].to_vec();
    (orig, rest)
}

// ---------------------------------------------------------------------------
// Symlink helper
// ---------------------------------------------------------------------------
fn symlink_to_dir(src: &Path, dst_dir: &Path) -> io::Result<()> {
    // Skip if any existing symlink in this dir already points to the same target
    if let Ok(entries) = fs::read_dir(dst_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_symlink()).unwrap_or(false) {
                if let Ok(target) = fs::read_link(entry.path()) {
                    if target == src {
                        return Ok(());  // already symlinked, nothing to do
                    }
                }
            }
        }
    }

    // Find an unused name
    let name = src.file_name().unwrap();
    let mut link = dst_dir.join(name);
    let mut n = 1;
    while link.exists() {
        let stem = src.file_stem().map(|s| s.to_string_lossy()).unwrap_or_default();
        let ext = src.extension()
            .map(|e| format!(".{}", e.to_string_lossy()))
            .unwrap_or_default();
        link = dst_dir.join(format!("{stem}_{n}{ext}"));
        n += 1;
    }
    symlink(src, &link)
}

// ---------------------------------------------------------------------------
// Output organisation
// ---------------------------------------------------------------------------
struct Stats {
    total: u64,
    total_dupes: u64,
    inode_dupes: u64,
    wasted: u64,
    byte_cmp_wins: u64,
    hash_ops: u64,
}

fn organise(
    groups: &HashMap<blake3::Hash, Vec<PathBuf>>,
    output_dir: &Path,
    prefer: &Path,
    delete_mode: bool,
    stats: &Stats,
) {
    fs::create_dir_all(output_dir).unwrap();

    let mut report = Vec::new();
    report.push("=".repeat(78));
    report.push("FASTDEDUP — DUPLICATE REPORT (Rust)".into());
    report.push("=".repeat(78));
    report.push(format!("Total scanned    : {}", stats.total));
    report.push(format!("Duplicate groups : {}", groups.len()));
    report.push(format!("Duplicate files  : {}", stats.total_dupes));
    report.push(format!("  (hard links)   : {}", stats.inode_dupes));
    report.push(format!("Wasted space     : {}", format_bytes(stats.wasted)));
    report.push("=".repeat(78));
    report.push(String::new());

    let mut sorted: Vec<_> = groups.iter().collect();
    sorted.sort_by_key(|(h, _)| h.to_hex().to_string());

    for (idx, (hash, group)) in sorted.iter().enumerate() {
        let (orig, dupes) = pick_original(group, prefer);
        let sz = orig.metadata().map(|m| m.len()).unwrap_or(0);

        let gd = output_dir.join(format!("group_{:04}", idx + 1));
        fs::create_dir_all(gd.join("original")).unwrap();
        fs::create_dir_all(gd.join("duplicates")).unwrap();
        let _ = symlink_to_dir(&orig, &gd.join("original"));
        for d in &dupes {
            let _ = symlink_to_dir(d, &gd.join("duplicates"));
        }

        report.push(format!("── Group {}  ({})  hash: {}…",
            idx + 1, format_bytes(sz), &hash.to_hex()[..32]));
        report.push(format!("   ORIGINAL  : {}", orig.display()));
        for d in &dupes {
            report.push(format!("   DUPLICATE : {}", d.display()));
        }
        report.push(String::new());

        if delete_mode {
            let dup_dir = gd.join("duplicates");
            for d in &dupes {
                // Delete the real file
                match fs::remove_file(d) {
                    Ok(()) => eprintln!("{} Deleted: {}", green("✓"), d.display()),
                    Err(e) => eprintln!("{} {}: {e}", yellow("⚠"), d.display()),
                }
                // Remove the now‑dangling symlink from the output folder
                let link_name = d.file_name().unwrap();
                let link = dup_dir.join(link_name);
                let _ = fs::remove_file(&link);
            }
            // If duplicates dir is empty, remove it
            let _ = fs::remove_dir(&dup_dir);
        }
    }

    let rp = output_dir.join("duplicate_report.txt");
    if let Ok(mut f) = fs::File::create(&rp) {
        let _ = f.write_all(report.join("\n").as_bytes());
    }

    if delete_mode {
        // Clean up — review folder has served its purpose
        let _ = fs::remove_dir_all(output_dir);
    } else {
        eprintln!("{} Report: {}", green("✓"), rp.display());
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
fn main() {
    let args = Args::parse();
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers.unwrap_or_else(|| {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
        }))
        .build_global()
        .unwrap();

    eprintln!("\n{}", cyan_bold("FASTDEDUP — Rust Edition"));
    let t0 = Instant::now();

    // 1. Collect
    // Build folder list
    let mut folders = vec![args.folder1.clone()];
    if let Some(ref f2) = args.folder2 {
        if f2 != &args.folder1 {
            folders.push(f2.clone());
        }
    }

    let skip = !args.hidden;
    eprintln!("{} Scanning directories …", dim(&timestamp()));
    let mut files = collect_files(&folders, skip);
    files.retain(|e| e.size >= args.min_size);
    eprintln!("{} Found {} files", dim(&timestamp()), files.len());

    if files.len() < 2 {
        eprintln!("{} Not enough files — exiting.", yellow("⚠"));
        return;
    }

    // 2. Inode + size filter
    let filtered = filter_by_inode_and_size(files);
    let candidates: usize = filtered.size_groups.values().map(|v| v.len()).sum();
    eprintln!("{} Inode + size → {candidates} files in {} groups",
              dim(&timestamp()), filtered.size_groups.len());
    if filtered.inode_dupes_saved > 0 {
        eprintln!("{}   Hard‑link dupes detected: {}",
            dim(&timestamp()), filtered.inode_dupes_saved);
    }

    // 3. Split: byte‑compare small groups, hash large groups
    let mut final_groups: HashMap<blake3::Hash, Vec<PathBuf>> = HashMap::new();
    let mut hash_candidates: BTreeMap<u64, Vec<FileEntry>> = BTreeMap::new();
    let mut byte_cmp_wins = 0u64;

    for (size, entries) in filtered.size_groups {
        if entries.len() <= BYTE_CMP_THRESHOLD {
            let (dups, remaining) = byte_compare_groups(entries);
            byte_cmp_wins += dups.len() as u64;
            for group in dups {
                // Expand each path to include inode siblings
                let mut expanded: Vec<PathBuf> = Vec::new();
                for p in &group {
                    if let Some(sibs) = filtered.inode_expand.get(p) {
                        expanded.extend(sibs.clone());
                    } else {
                        expanded.push(p.clone());
                    }
                }
                let h = blake3::hash(expanded[0].to_string_lossy().as_bytes());
                final_groups.insert(h, expanded);
            }
            if remaining.len() > 1 {
                hash_candidates.insert(size, remaining);
            }
        } else {
            hash_candidates.insert(size, entries);
        }
    }
    if byte_cmp_wins > 0 {
        eprintln!("{} Byte‑comparison resolved {byte_cmp_wins} groups directly",
                  dim(&timestamp()));
    }

    // 4+5. Hash pipeline
    let (hash_groups, hash_ops) = hash_pipeline(hash_candidates, &filtered.inode_expand);
    final_groups.extend(hash_groups);

    // Stats
    let total_dupes: u64 = final_groups.values().map(|v| (v.len() - 1) as u64).sum();
    let wasted: u64 = final_groups.values().map(|v| {
        v.first().and_then(|p| p.metadata().ok()).map(|m| m.len()).unwrap_or(0)
            * (v.len() - 1) as u64
    }).sum();

    let stats = Stats {
        total: candidates as u64 + filtered.inode_dupes_saved,
        total_dupes,
        inode_dupes: filtered.inode_dupes_saved,
        wasted,
        byte_cmp_wins,
        hash_ops,
    };

    let elapsed = t0.elapsed();

    // 6. Output
    let prefer = std::path::absolute(
        args.prefer.clone().unwrap_or_else(|| args.folder1.clone())
    ).unwrap_or_else(|_| args.prefer.unwrap_or(args.folder1));

    if args.dry_run {
        eprintln!("\n{}", yellow("DRY RUN"));
        let mut sorted: Vec<_> = final_groups.iter().collect();
        sorted.sort_by_key(|(h, _)| h.to_hex().to_string());
        for (idx, (hash, group)) in sorted.iter().enumerate() {
            let (orig, dupes) = pick_original(group, &prefer);
            let sz = orig.metadata().map(|m| m.len()).unwrap_or(0);
            println!("\n── Group {}  {}  {}…", idx + 1, format_bytes(sz), &hash.to_hex()[..32]);
            println!("   ORIGINAL  : {}", orig.display());
            for d in &dupes { println!("   DUPLICATE : {}", d.display()); }
        }
    } else {
        eprintln!("{} Organising output …", dim(&timestamp()));
        organise(&final_groups, &args.output, &prefer, args.delete, &stats);
    }

    // Summary
    println!("\n{}", dim(&"━".repeat(55)));
    println!("  Duplicate groups : {:>10}", final_groups.len());
    println!("  Duplicate files  : {:>10}", total_dupes);
    println!("  Hard‑link dupes  : {:>10}", stats.inode_dupes);
    println!("  Byte‑cmp groups  : {:>10}", stats.byte_cmp_wins);
    println!("  Wasted space     : {:>14}", format_bytes(stats.wasted));
    println!("  Elapsed          : {:>12.1?}", elapsed);
    if !args.dry_run {
        if args.delete {
            println!("\n  {}", green("✓  Duplicates deleted. Review folder cleaned up."));
        } else {
            println!("  Output           : {}", args.output.display());
            println!("\n  {}", dim("Review symlinks, then re‑run with --delete"));
        }
    }
    println!();
}
