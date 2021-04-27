// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of Parity.

// Parity is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity.  If not, see <http://www.gnu.org/licenses/>.

mod sizes;
mod db;

pub use db::{Key, Value, Db};

use std::{sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, }, thread};
use rand::{SeedableRng, RngCore};
use std::path::PathBuf;

static COMMITS: AtomicUsize = AtomicUsize::new(0);
//static QUERIES: AtomicUsize = AtomicUsize::new(0);

const COMMIT_SIZE: usize = 100;
// Note on db, this is equal to COMMIT_SIZE.
// Having smaller allow validating a given amount
// of keys.
const COMMIT_PRUNE_SIZE: usize = 90;
const COMMIT_PRUNE_WINDOW: usize = 2000;

const USAGE: &str = "
Usage: stress [--readers=<#>][--writers=<#>] [--commits=<l>]

Options:
	--readers=<#>      Number of reading threads [default: 4].
	--writers=<#>      Number of writing threads [default: 1].
	--commits=<n>      Total numbet of inserted commits.
	--seed=<n>         Random seed used for key generation.
	--archive          Do not perform deletions.
	--append           Open an existing database.
";

#[derive(Clone)]
pub struct Args {
	pub readers: usize,
	pub commits: usize,
	pub writers: usize,
	pub seed: Option<u64>,
	pub archive: bool,
	pub append: bool,
	pub check_only: bool,
	pub start_commit: usize,
}

impl Default for Args {
	fn default() -> Args {
		Args {
			readers: 4,
			writers: 1,
			commits: 100000,
			append: false,
			seed: None,
			archive: false,
			check_only: false,
			start_commit: 0,
		}
	}
}

struct SizePool {
	distribution: std::collections::BTreeMap<u32, u32>,
	total: u32,
}

impl SizePool {
	fn from_histogram(h: &[(u32, u32)]) -> SizePool {
		let mut distribution = std::collections::BTreeMap::default();
		let mut total = 0;
		for (size, count) in h {
			total += count;
			distribution.insert(total, *size);
		}
		SizePool { distribution, total }
	}

	fn value(&self, seed: u64) -> Vec<u8> {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let sr = (rng.next_u64() % self.total as u64) as u32;
		let mut range = self.distribution.range((std::ops::Bound::Included(sr), std::ops::Bound::Unbounded));
		let size = *range.next().unwrap().1 as usize;
		let mut v = Vec::new();
		v.resize(size, 0);
		rng.fill_bytes(&mut v);
		v
	}

	fn key(&self, seed: u64) -> db::Key {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let mut key = db::Key::default();
		rng.fill_bytes(&mut key);
		key
	}
}

fn parse<'a, I, T>(mut iter: I) -> T
where
	I: Iterator<Item = &'a str>,
	T: std::str::FromStr,
	<T as std::str::FromStr>::Err: std::fmt::Debug,
{
	iter.next().expect(USAGE).parse().expect(USAGE)
}

impl Args {
	fn parse() -> Args {
		let mut args = Args::default();
		for raw_arg in std::env::args().skip(1) {
			let mut splits = raw_arg[2..].split('=');
			match splits.next().unwrap() {
				"readers" => args.readers = parse(&mut splits),
				"writers" => args.writers = parse(&mut splits),
				"commits" => args.commits = parse(&mut splits),
				"seed" => args.seed = Some(parse(&mut splits)),
				"archive" => args.archive = true,
				"append" => args.append = true,
				other => panic!("unknown option: {}, {}", other, USAGE),
			}
		}
		args
	}
}

fn informant(shutdown: Arc<AtomicBool>, total: usize, start: usize) {
	let mut last = 0;
	let mut last_time = std::time::Instant::now();
	while !shutdown.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_secs(1));
		let commits = COMMITS.load(Ordering::Acquire);
		let now = std::time::Instant::now();
		println!("{}/{} commits, {} cps", commits - start, total,  ((commits - last) as f64) / (now - last_time).as_secs_f64());
		last = commits;
		last_time = now;
	}
}

fn writer<D: Db>(db: Arc<D>, args: Arc<Args>, pool: Arc<SizePool>, shutdown: Arc<AtomicBool>) {
	// Note that multiple worker will run on same range concurrently.
	let mut key: u64 = (args.start_commit * COMMIT_SIZE) as u64;
	let commit_size = COMMIT_SIZE;
	let mut commit = Vec::with_capacity(commit_size);

	for n in args.start_commit .. args.start_commit + args.commits {
		if shutdown.load(Ordering::Relaxed) { break; }
		for _ in 0 .. commit_size {
			commit.push((pool.key(key), Some(pool.value(key))));
			key += 1;
		}
		if !args.archive && n >= COMMIT_PRUNE_WINDOW {
			let prune_start = (n - COMMIT_PRUNE_WINDOW) * COMMIT_SIZE;
			for p in prune_start .. prune_start + COMMIT_PRUNE_SIZE {
				commit.push((pool.key(p as u64), None));
			}
		}

		db.commit(commit.drain(..));
		COMMITS.fetch_add(1, Ordering::Release);
		commit.clear();
	}
}

fn reader<D: Db>(_db: Arc<D>, shutdown: Arc<AtomicBool>) {
	// Query a random  key
	while !shutdown.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_millis(500));
	}
}

pub fn run<D: Db>() {
	env_logger::try_init().unwrap();
	let path: PathBuf = "./test_db".into();
	let args = Args::parse();
	if path.exists() && !args.append {
		std::fs::remove_dir_all(path.as_path()).unwrap();
	}
	let db = Db::open(path.as_path());

	run_internal::<D>(args, db);
}

pub fn run_internal<D: Db>(args: Args, db: D) {
	let args = Arc::new(args);
	let shutdown = Arc::new(AtomicBool::new(false));
	let pool = Arc::new(SizePool::from_histogram(&sizes::KUSAMA_STATE_DISTRIBUTION));
	let db = Arc::new(db) as Arc<D>;
	let start = std::time::Instant::now();

	let mut threads = Vec::new();

	COMMITS.store(args.start_commit, Ordering::SeqCst);

	if !args.check_only {
		{
			let commits = args.commits;
			let start = args.start_commit;
			let shutdown = shutdown.clone();
			threads.push(thread::spawn(move || informant(shutdown, commits, start)));
		}

		for i in 0 .. args.readers {
			let db = db.clone();
			let shutdown = shutdown.clone();

			threads.push(
				thread::Builder::new()
				.name(format!("reader {}", i))
				.spawn(move || reader(db, shutdown))
				.unwrap()
			);
		}

		for i in 0 .. args.writers {
			let db = db.clone();
			let shutdown = shutdown.clone();
			let pool = pool.clone();
			let args = args.clone();

			threads.push(
				thread::Builder::new()
				.name(format!("writer {}", i))
				.spawn(move || writer(db, args, pool, shutdown))
				.unwrap()
			);
		}

		while COMMITS.load(Ordering::Relaxed) < args.start_commit + args.commits {
			thread::sleep(std::time::Duration::from_millis(50));
		}
		shutdown.store(true, Ordering::SeqCst);
	}

	for t in threads.into_iter() {
		t.join().unwrap();
	}

	let commits = COMMITS.load(Ordering::SeqCst);
	let commits = commits - args.start_commit;
	let elapsed = start.elapsed().as_secs_f64();

	println!(
		"Completed {} commits in {} seconds. {} cps",
		commits,
		elapsed,
		commits as f64  / elapsed
	);

	thread::sleep(std::time::Duration::from_secs(1));

	// Verify content
	let start = std::time::Instant::now();
	let pruned_per_commit = if args.archive { 0u64 } else { COMMIT_PRUNE_SIZE as u64 };
	let mut queries = 0;
	for nc in args.start_commit as u64 .. (args.start_commit + commits) as u64 {
		let counter = nc - args.start_commit as u64;
		if counter % 1000 == 0 {
			println!(
				"Query {}/{}",
				counter,
				commits,
			);
		}
		let commits  = (args.start_commit + commits) as u64;
		let prune_window: u64 = COMMIT_PRUNE_WINDOW as u64;
		let start = if commits > prune_window && nc < commits - prune_window {
			let end = nc * COMMIT_SIZE as u64 + pruned_per_commit;
			for key in (nc * COMMIT_SIZE as u64) .. end {
				let k = pool.key(key);
				let db_val = db.get(&k);
				queries += 1;
				assert_eq!(None, db_val);
			}
			end
		} else {
			nc * COMMIT_SIZE as u64
		};
		for key in start .. (nc + 1) * (COMMIT_SIZE as u64) {
			let k = pool.key(key);
			let val = pool.value(key);
			let db_val = db.get(&k);
			queries += 1;
			assert_eq!(Some(val), db_val);
		}
	}

	let elapsed = start.elapsed().as_secs_f64();
	println!(
		"Completed {} queries in {} seconds. {} qps",
		queries,
		elapsed,
		queries as f64  / elapsed
	);
}
