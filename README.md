# dedoop ⚡

High‑performance duplicate file finder. Finds byte‑identical files by content
(not filename) across one or two directories.

## Quick start

```bash
# Build from source
cargo build --release
./target/release/dedoop /path/to/folder1 /path/to/folder2
```

Or download a pre‑built binary from [Releases](https://github.com/bhavyakaria/dedoop/releases):

```bash
curl -LO https://github.com/YOU/dedoop/releases/latest/download/dedoop-linux-x86_64
chmod +x dedoop-linux-x86_64
./dedoop-linux-x86_64 /folder1 /folder2
```

## How it works

Multi‑stage pipeline — each stage cheaper than the next:

```
1. Walk       → collect all regular files
2. Inode      → group hard‑linked files instantly (zero I/O)
3. Size       → discard unique sizes (60–90% eliminated)
4. Byte‑by‑byte → mmap + memcmp for groups ≤ 8 files (no hashing)
5. Partial    → BLAKE3 at 3 offsets (start/mid/end)
6. Full       → BLAKE3 + mmap on survivors only
7. Organise   → per‑group original/duplicates symlink tree
```

**Key techniques:**
- **BLAKE3** with SIMD (AVX‑2/AVX‑512/NEON)
- **mmap** for zero‑copy I/O
- **rayon** work‑stealing thread pool
- **Byte‑by‑byte pre‑comparison** avoids hashing for small groups
- **Inode dedup** catches hard links instantly
- **HDD‑aware I/O** — detects rotational drives, caps parallelism

## Usage

```
dedoop <FOLDER1> [FOLDER2] [OPTIONS]

Options:
  -o, --output <PATH>      Output directory [default: ./duplicates_output]
  -s, --min-size <BYTES>   Ignore files smaller than this [default: 1]
  -w, --workers <N>        Parallel worker threads [default: CPU count]
  -p, --prefer <PATH>      Treat files in this folder as "originals"
  --no-hidden              Include hidden files/folders
  --delete                 Automatically delete duplicates
  --dry-run                Print groups only — no files written
  --hdd-workers <N>        Max workers for rotational drives [default: 4]
```

## Examples

```bash
# Find duplicates across two folders
dedoop /photos /backup -o ~/review

# Single folder — find internal duplicates
dedoop ~/Downloads --dry-run

# Prefer one folder as source of truth, auto‑delete copies
dedoop /master /messy_backup --prefer /master --delete

# External HDD — limit workers to avoid seek storms
dedoop /ssd /media/external --hdd-workers 2
```

## Safety

| Mode | Source files touched? |
|---|---|
| Default (`-o out`) | ❌ Never |
| `--dry-run` | ❌ Never |
| `--delete` | ✅ Duplicates removed, one copy kept |

## Requirements

- **Rust** 1.70+
- **Linux** (uses `/sys/block`, symlinks, `mmap`)

## License

MIT
