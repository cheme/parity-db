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

/// The database objects is split into `Db` and `DbInner`.
/// `Db` creates shared `DbInner` instance and manages background
/// worker threads that all use the inner object.
///
/// There are 3 worker threads:
/// log_worker: Processes commit queue and reindexing. For each commit
/// in the queue, log worker creates a write-ahead record using `Log`.
/// Additionally, if there are active reindexing, it creates log records
/// for batches of relocated index entries.
/// flush_worker: Flushes log records to disk by calling `fsync` on the
/// log files.
/// commit_worker: Reads flushed log records and applies operations to the
/// index and value tables.
/// Each background worker is signalled with a conditional variable once
/// there is some work to be done.

use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::convert::TryInto;
use std::collections::{HashMap, VecDeque};
use parking_lot::{RwLock, Mutex, Condvar};
use fs2::FileExt;
use crate::{
	Key,
	error::{Error, Result},
	column::{ColId, Column},
	log::{Log, LogAction},
	index::PlanOutcome,
	options::Options,
};
use std::collections::BTreeMap;
use crate::no_indexing::{HandleId, FreeIdHandle};

// These are in memory, so we use usize
const MAX_COMMIT_QUEUE_BYTES: usize = 16 * 1024 * 1024;
// These are disk-backed, so we use u64
const MAX_LOG_QUEUE_BYTES: u64 = 128 * 1024 * 1024;
const MIN_LOG_SIZE: u64 = 16 * 1024 * 1024;

/// Value is just a vector of bytes. Value sizes up to 4Gb are allowed.
pub type Value = Vec<u8>;

// Commit data passed to `commit`
#[derive(Default)]
struct Commit {
	// Commit ID. This is not the same as log record id, as some records
	// are originated within the DB. E.g. reindex.
	id: u64,
	// Size of user data pending insertion (keys + values) or
	// removal (keys)
	bytes: usize,
	// Operations.
	changeset: Vec<(ColId, Key, Option<Value>)>,
}

// Pending commits. This may not grow beyond `MAX_COMMIT_QUEUE_BYTES` bytes.
#[derive(Default)]
struct CommitQueue {
	// Log record.
	record_id: u64,
	// Total size of all commits in the queue.
	bytes: usize,
	// FIFO queue.
	commits: VecDeque<Commit>,
}

#[derive(Default)]
struct IdentityKeyHash(u64);
type IdentityBuildHasher = std::hash::BuildHasherDefault<IdentityKeyHash>;

impl std::hash::Hasher for IdentityKeyHash {
	fn write(&mut self, bytes: &[u8]) {
		self.0 = u64::from_le_bytes((&bytes[0..8]).try_into().unwrap())
	}

	fn finish(&self) -> u64 {
		self.0
	}
}

struct DbInner {
	columns: Vec<Column>,
	options: Options,
	shutdown: AtomicBool,
	log: Log,
	commit_queue: Mutex<CommitQueue>,
	commit_queue_full_cv: Condvar,
	log_worker_cv: Condvar,
	log_work: Mutex<bool>,
	commit_worker_cv: Condvar,
	commit_work: Mutex<bool>,
	// Overlay of most recent values int the commit queue. ColumnId -> (Key -> (RecordId, Value)).
	commit_overlay: RwLock<Vec<HashMap<Key, (u64, Option<Value>), IdentityBuildHasher>>>,
	log_cv: Condvar,
	log_queue_bytes: Mutex<u64>,
	flush_worker_cv: Condvar,
	flush_work: Mutex<bool>,
	enact_mutex: Mutex<()>,
	last_enacted: AtomicU64,
	next_reindex: AtomicU64,
	collect_stats: bool,
	bg_err: Mutex<Option<Arc<Error>>>,
	free_table_id_manager: RwLock<crate::no_indexing::IdManager>, // TODO mutex instead?
	_lock_file: std::fs::File,
}

impl DbInner {
	fn open(options: &Options) -> Result<DbInner> {
		std::fs::create_dir_all(&options.path)?;
		let mut lock_path: std::path::PathBuf = options.path.clone();
		lock_path.push("lock");
		let lock_file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(lock_path.as_path())?;
		lock_file.try_lock_exclusive().map_err(|e| Error::Locked(e))?;

		let salt = options.load_and_validate_metadata()?;
		let mut columns = Vec::with_capacity(options.columns.len());
		let mut commit_overlay = Vec::with_capacity(options.columns.len());
		for c in 0 .. options.columns.len() {
			columns.push(Column::open(c as ColId, &options, salt.clone())?);
			commit_overlay.push(
				HashMap::with_hasher(std::hash::BuildHasherDefault::<IdentityKeyHash>::default())
			);
		}
		log::debug!(target: "parity-db", "Opened db {:?}, salt={:?}", options, salt);
		Ok(DbInner {
			columns,
			options: options.clone(),
			shutdown: std::sync::atomic::AtomicBool::new(false),
			log: Log::open(&options)?,
			commit_queue: Mutex::new(Default::default()),
			commit_queue_full_cv: Condvar::new(),
			log_worker_cv: Condvar::new(),
			log_work: Mutex::new(false),
			commit_worker_cv: Condvar::new(),
			commit_work: Mutex::new(false),
			commit_overlay: RwLock::new(commit_overlay),
			log_queue_bytes: Mutex::new(0),
			log_cv: Condvar::new(),
			flush_worker_cv: Condvar::new(),
			flush_work: Mutex::new(false),
			enact_mutex: Mutex::new(()),
			next_reindex: AtomicU64::new(1),
			last_enacted: AtomicU64::new(1),
			collect_stats: options.stats,
			bg_err: Mutex::new(None),
			free_table_id_manager: RwLock::new(crate::no_indexing::IdManager::new(&options)),
			_lock_file: lock_file,
		})
	}

	fn signal_log_worker(&self) {
		let mut work = self.log_work.lock();
		*work = true;
		self.log_worker_cv.notify_one();
	}

	fn signal_commit_worker(&self) {
		let mut work = self.commit_work.lock();
		*work = true;
		self.commit_worker_cv.notify_one();
	}

	fn signal_flush_worker(&self) {
		let mut work = self.flush_work.lock();
		*work = true;
		self.flush_worker_cv.notify_one();
	}

	fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		let key = self.columns[col as usize].hash(key);
		let overlay = self.commit_overlay.read();
		// Check commit overlay first
		if let Some(v) = overlay.get(col as usize).and_then(|o| o.get(&key).map(|(_, v)| v.clone())) {
			return Ok(v);
		}
		// Go into tables and log overlay.
		let log = self.log.overlays();
		self.columns[col as usize].get(&key, log)
	}

	fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		let key = self.columns[col as usize].hash(key);
		let overlay = self.commit_overlay.read();
		// Check commit overlay first
		if let Some(l) = overlay.get(col as usize).and_then(
			|o| o.get(&key).map(|(_, v)| v.as_ref().map(|v| v.len() as u32))
		) {
			return Ok(l);
		}
		// Go into tables and log overlay.
		let log = self.log.overlays();
		self.columns[col as usize].get_size(&key, log)
	}

	// Commit simply adds the the data to the queue and to the overlay and
	// exits as early as possible.
	fn commit<I, K>(&self, tx: I) -> Result<()>
		where
			I: IntoIterator<Item=(ColId, K, Option<Value>)>,
			K: AsRef<[u8]>,
	{
		self.commit_with_non_canonical::<I, K>(tx, Default::default())
	}

	// Commit simply adds the the data to the queue and to the overlay and
	// exits as early as possible.
	fn commit_with_non_canonical<I, K>(&self, tx: I, non_canonical: BTreeMap<ColId, FreeIdHandle>) -> Result<()>
		where
			I: IntoIterator<Item=(ColId, K, Option<Value>)>,
			K: AsRef<[u8]>,
	{
	
		{
			let bg_err = self.bg_err.lock();
			if let Some(err) = &*bg_err {
				return Err(Error::Background(err.clone()));
			}
		}

		let mut non_canonical_overlay = self.free_table_id_manager.write();
		// TODO since we collect here: there is surely a way to group by col and key and have a
		// more efficient non_canonical resolution (currently it looks up each keys).
		let commit: Vec<_> = tx.into_iter().map(|(c, k, v)| {
			if let Some(handle) = non_canonical.get(&c) {
				if v.is_some() {
					let k = non_canonical_overlay.commit_entry(c, k, handle.id);
					(c, self.columns[c as usize].hash(k.as_ref()), v)
				} else {
					if non_canonical_overlay.removed_index(c, k.as_ref()) {
						(c, self.columns[c as usize].hash(k.as_ref()), v)
					} else {
						// 0 removed is ignored for non_canonical
						(c, self.columns[c as usize].hash(&[0; 8]), v)
					}
				}
			} else {
				(c, self.columns[c as usize].hash(k.as_ref()), v)
			}
		}).collect();
		std::mem::drop(non_canonical_overlay);

		{
			let mut queue = self.commit_queue.lock();
			if queue.bytes > MAX_COMMIT_QUEUE_BYTES {
				log::debug!(target: "parity-db", "Waiting, qb={}", queue.bytes);
				self.commit_queue_full_cv.wait(&mut queue);
			}
			let mut overlay = self.commit_overlay.write();

			queue.record_id += 1;
			let record_id = queue.record_id + 1;

			let mut bytes = 0;
			for (c, k, v) in &commit {
				bytes += k.encoded_len();
				bytes += v.as_ref().map_or(0, |v|v.len());
				// Don't add removed ref-counted values to overlay.
				if !self.options.columns[*c as usize].ref_counted || v.is_some() {
					overlay[*c as usize].insert(k.clone(), (record_id, v.clone()));
				}
			}

			let commit = Commit {
				id: record_id,
				changeset: commit,
				bytes,
			};

			log::debug!(
				target: "parity-db",
				"Queued commit {}, {} bytes",
				commit.id,
				bytes,
			);
			queue.commits.push_back(commit);
			queue.bytes += bytes;
			self.signal_log_worker();
		}

		std::mem::drop(non_canonical);
		self.free_table_id_manager.write().commit();
		Ok(())
	}

	fn process_commits(&self, force: bool) -> Result<bool> {
		{
			// Wait if the queue is too big.
			let mut queue = self.log_queue_bytes.lock();
			if !force && *queue > MAX_LOG_QUEUE_BYTES {
				log::debug!(target: "parity-db", "Waiting, log_bytes={}", queue);
				self.log_cv.wait(&mut queue);
			}
		}
		let commit = {
			let mut queue = self.commit_queue.lock();
			if let Some(commit) = queue.commits.pop_front() {
				queue.bytes -= commit.bytes;
				log::debug!(
					target: "parity-db",
					"Removed {}. Still queued commits {} bytes",
					commit.bytes,
					queue.bytes,
				);
				if queue.bytes <= MAX_COMMIT_QUEUE_BYTES && (queue.bytes + commit.bytes) > MAX_COMMIT_QUEUE_BYTES {
					// Past the waiting threshold.
					log::debug!(
						target: "parity-db",
						"Waking up commit queue worker",
					);
					self.commit_queue_full_cv.notify_one();
				}
				Some(commit)
			} else {
				None
			}
		};

		if let Some(commit) = commit {
			let mut reindex = false;
			let mut writer = self.log.begin_record();
			log::debug!(
				target: "parity-db",
				"Processing commit {}, record {}, {} bytes",
				commit.id,
				writer.record_id(),
				commit.bytes,
			);
			let mut ops: u64 = 0;
			for (c, key, value) in commit.changeset.iter() {
				match self.columns[*c as usize].write_plan(key, value, &mut writer)? {
					// Reindex has triggered another reindex.
					PlanOutcome::NeedReindex => {
						reindex = true;
					},
					_ => {},
				}
				ops += 1;
			}
			// Collect final changes to value tables
			for c in self.columns.iter() {
				c.complete_plan(&mut writer)?;
			}
			let record_id = writer.record_id();
			let l = writer.drain();

			let bytes = {
				let mut logged_bytes = self.log_queue_bytes.lock();
				let bytes = self.log.end_record(l)?;
				*logged_bytes += bytes;
				self.signal_flush_worker();
				bytes
			};

			{
				// Cleanup the commit overlay.
				let mut overlay = self.commit_overlay.write();
				for (c, key, _) in commit.changeset.iter() {
					let overlay = &mut overlay[*c as usize];
					if let std::collections::hash_map::Entry::Occupied(e) = overlay.entry(key.clone()) {
						if e.get().0 == commit.id {
							e.remove_entry();
						}
					}
				}
			}

			if reindex {
				self.start_reindex(record_id);
			}

			log::debug!(
				target: "parity-db",
				"Processed commit {} (record {}), {} ops, {} bytes written",
				commit.id,
				record_id,
				ops,
				bytes,
			);
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn start_reindex(&self, record_id: u64) {
		self.next_reindex.store(record_id, Ordering::SeqCst);
	}

	fn process_reindex(&self) -> Result<bool> {
		let next_reindex = self.next_reindex.load(Ordering::SeqCst);
		if next_reindex == 0 || next_reindex > self.last_enacted.load(Ordering::SeqCst) {
			return Ok(false)
		}
		// Process any pending reindexes
		for column in self.columns.iter() {
			let (drop_index, batch) = column.reindex(&self.log)?;
			if !batch.is_empty() {
				let mut next_reindex = false;
				let mut writer = self.log.begin_record();
				log::debug!(
					target: "parity-db",
					"Creating reindex record {}",
					writer.record_id(),
				);
				for (key, address) in batch.into_iter() {
					match column.write_reindex_plan(&key, address, &mut writer)? {
						PlanOutcome::NeedReindex => {
							next_reindex = true
						},
						_ => {},
					}
				}
				if let Some(table) = drop_index {
					writer.drop_table(table);
				}
				let record_id = writer.record_id();
				let l = writer.drain();

				let mut logged_bytes = self.log_queue_bytes.lock();
				let bytes = self.log.end_record(l)?;
				log::debug!(
					target: "parity-db",
					"Created reindex record {}, {} bytes",
					record_id,
					bytes,
				);
				*logged_bytes += bytes;
				if next_reindex {
					self.start_reindex(record_id);
				}
				self.signal_flush_worker();
				return Ok(true)
			}
		}
		self.next_reindex.store(0, Ordering::SeqCst);
		Ok(false)
	}

	fn enact_logs(&self, validation_mode: bool) -> Result<bool> {
		let cleared = {
			let _lock = self.enact_mutex.lock();
			let reader = match self.log.read_next(validation_mode) {
				Ok(reader) => reader,
				Err(Error::Corruption(_)) if validation_mode => {
					log::debug!(target: "parity-db", "Bad log header");
					self.log.clear_logs()?;
					return Ok(false);
				}
				Err(e) => return Err(e),
			};
			if let Some(mut reader) = reader {
				log::debug!(
					target: "parity-db",
					"Enacting log {}",
					reader.record_id(),
				);
				if validation_mode {
					// Validate all records before applying anything
					loop {
						match reader.next()? {
							LogAction::BeginRecord(_) => {
								log::debug!(target: "parity-db", "Unexpected log header");
								std::mem::drop(reader);
								self.log.clear_logs()?;
								return Ok(false);
							},
							LogAction::EndRecord => {
								break;
							},
							LogAction::InsertIndex(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col].validate_plan(LogAction::InsertIndex(insertion), &mut reader) {
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_logs()?;
									return Ok(false);
								}
							},
							LogAction::InsertValue(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col].validate_plan(LogAction::InsertValue(insertion), &mut reader) {
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_logs()?;
									return Ok(false);
								}
							},
							LogAction::DropTable(_) => {
								continue;
							}
						}
					}
					reader.reset()?;
					reader.next()?;
				}
				loop {
					match reader.next()? {
						LogAction::BeginRecord(_) => {
							return Err(Error::Corruption("Bad log record".into()));
						},
						LogAction::EndRecord => {
							break;
						},
						LogAction::InsertIndex(insertion) => {
							self.columns[insertion.table.col() as usize]
								.enact_plan(LogAction::InsertIndex(insertion), &mut reader)?;

						},
						LogAction::InsertValue(insertion) => {
							self.columns[insertion.table.col() as usize]
								.enact_plan(LogAction::InsertValue(insertion), &mut reader)?;

						},
						LogAction::DropTable(id) => {
							log::debug!(
								target: "parity-db",
								"Dropping index {}",
								id,
							);
							self.columns[id.col() as usize].drop_index(id)?;
							// Check if there's another reindex on the next iteration
							self.start_reindex(reader.record_id());
						}
					}
				}
				log::debug!(
					target: "parity-db",
					"Enacted log record {}, {} bytes",
					reader.record_id(),
					reader.read_bytes(),
				);
				let record_id = reader.record_id();
				let bytes = reader.read_bytes();
				let cleared = reader.drain();
				self.last_enacted.store(record_id, Ordering::SeqCst);
				Some((record_id, cleared, bytes))
			} else {
				None
			}
		};

		if let Some((record_id, cleared, bytes)) = cleared {
			self.log.end_read(cleared, record_id);
			{
				if !validation_mode {
					let mut queue = self.log_queue_bytes.lock();
					if *queue < bytes {
						log::warn!(
							target: "parity-db",
							"Detected log undeflow record {}, {} bytes, {} queued, reindex = {}",
							record_id,
							bytes,
							*queue,
							self.next_reindex.load(Ordering::SeqCst),
						);
					}
					*queue -= bytes;
					if *queue <= MAX_LOG_QUEUE_BYTES && (*queue + bytes) > MAX_LOG_QUEUE_BYTES {
						self.log_cv.notify_all();
					}
					log::debug!(target: "parity-db", "Log queue size: {} bytes", *queue);
				}
			}
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn flush_logs(&self, min_log_size: u64) -> Result<bool> {
		let (flush_next, read_next) = self.log.flush_one(min_log_size, || {
			for c in self.columns.iter() {
				c.flush()?;
			}
			Ok(())
		})?;
		if read_next {
			self.signal_commit_worker();
		}
		Ok(flush_next)
	}

	fn replay_all_logs(&self) -> Result<()> {
		log::debug!(target: "parity-db", "Replaying database log...");
		// Process the oldest log first
		while self.enact_logs(true)? { }
		// Process intermediate logs
		while self.flush_logs(0)? {
			while self.enact_logs(true)? { }
		}
		// Need one more pass to enact the last log.
		while self.enact_logs(true)? { }
		// Re-read any cached metadata
		for c in self.columns.iter() {
			c.refresh_metadata()?;
		}
		log::debug!(target: "parity-db", "Replay is complete.");
		Ok(())
	}

	fn shutdown(&self) {
		self.shutdown.store(true, Ordering::SeqCst);
		self.signal_flush_worker();
		self.signal_log_worker();
		self.signal_commit_worker();
	}

	fn kill_logs(&self) -> Result<()> {
		log::debug!(target: "parity-db", "Processing leftover commits");
		while self.process_commits(true)? {};
		while self.enact_logs(true)? {};
		self.log.flush_one(0, || Ok(()))?;
		while self.enact_logs(true)? {};
		self.log.flush_one(0, || Ok(()))?;
		while self.enact_logs(true)? {};
		self.log.kill_logs(&self.options);
		if self.collect_stats {
			let mut path = self.options.path.clone();
			path.push("stats.txt");
			match std::fs::File::create(path) {
				Ok(file) => {
					for c in self.columns.iter() {
						c.write_stats(&file);
					}
				}
				Err(e) => log::warn!(target: "parity-db", "Error creating stats file: {:?}", e),
			}
		}
		Ok(())
	}

	fn store_err(&self, result: Result<()>) {
		if let Err(e) = result {
			log::warn!(target: "parity-db", "Background worker error: {}", e);
			let mut err =  self.bg_err.lock();
			if err.is_none() {
				*err = Some(Arc::new(e));
				self.shutdown();
			}
		}
	}
}

pub struct Db {
	inner: Arc<DbInner>,
	commit_thread: Option<std::thread::JoinHandle<()>>,
	flush_thread: Option<std::thread::JoinHandle<()>>,
	log_thread: Option<std::thread::JoinHandle<()>>,
}

impl Db {
	pub fn with_columns(path: &std::path::Path, num_columns: u8) -> Result<Db> {
		let options = Options::with_columns(path, num_columns);

		assert!(options.is_valid());
		Self::open(&options)
	}

	/// Open the database with given
	pub fn open(options: &Options) -> Result<Db> {
		let db = Arc::new(DbInner::open(options)?);
		// This needs to be call before log thread: so first reindexing
		// will run in correct state.
		db.replay_all_logs()?;
		let db_handle = DbHandle { inner: db.clone() };
		db.free_table_id_manager.write().init(db_handle);
		let commit_worker_db = db.clone();
		let commit_thread = std::thread::spawn(move ||
			commit_worker_db.store_err(Self::commit_worker(commit_worker_db.clone()))
		);
		let flush_worker_db = db.clone();
		let flush_thread = std::thread::spawn(move ||
			flush_worker_db.store_err(Self::flush_worker(flush_worker_db.clone()))
		);
		let log_worker_db = db.clone();
		let skip_commit_queue = options.skip_commit_queue;
		let log_thread = std::thread::spawn(move ||
			log_worker_db.store_err(Self::log_worker(log_worker_db.clone(), skip_commit_queue))
		);
		Ok(Db {
			inner: db,
			commit_thread: Some(commit_thread),
			flush_thread: Some(flush_thread),
			log_thread: Some(log_thread),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		self.inner.get(col, key)
	}

	pub fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		self.inner.get_size(col, key)
	}

	pub fn commit_with_non_canonical<I, K>(&self, tx: I, non_canonical: BTreeMap<ColId, FreeIdHandle>) -> Result<()>
		where
			I: IntoIterator<Item=(ColId, K, Option<Value>)>,
			K: AsRef<[u8]>,
	{
		self.inner.commit_with_non_canonical(tx, non_canonical)
	}
	
	pub fn commit<I, K>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item=(ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		self.inner.commit(tx)
	}

	pub fn num_columns(&self) -> u8 {
		self.inner.columns.len() as u8
	}

	fn commit_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				let mut work = db.commit_work.lock();
				while !*work {
					db.commit_worker_cv.wait(&mut work)
				};
				*work = false;
			}

			more_work = db.enact_logs(false)?;
		}
		log::debug!(target: "parity-db", "Commit worker shutdown");
		Ok(())
	}

	fn log_worker(db: Arc<DbInner>, skip_queue: bool) -> Result<()> {
		// Start with pending reindex.
		let mut more_work = db.process_reindex()?;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				let mut work = db.log_work.lock();
				while !*work {
					db.log_worker_cv.wait(&mut work)
				};
				*work = false;
			}

			let more_commits = db.process_commits(skip_queue)?;
			let more_reindex = db.process_reindex()?;
			more_work = more_commits || more_reindex;
		}
		log::debug!(target: "parity-db", "Log worker shutdown");
		Ok(())
	}

	fn flush_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) {
			if !more_work {
				let mut work = db.flush_work.lock();
				while !*work {
					db.flush_worker_cv.wait(&mut work)
				};
				*work = false;
			}
			more_work = db.flush_logs(MIN_LOG_SIZE)?;
		}
		log::debug!(target: "parity-db", "Flush worker shutdown");
		db.flush_logs(0)?;
		Ok(())
	}
	pub fn get_handle(&self, col_id: ColId, single_write: bool) -> Option<crate::no_indexing::FreeIdHandle> {
		self.inner.free_table_id_manager.write().get_handle(col_id, single_write)
	}
	pub fn get_read_only_handle(&self, col_id: ColId, no_write: bool) -> Option<crate::no_indexing::FreeIdHandle> {
		self.inner.free_table_id_manager.write().get_read_only_handle(col_id, no_write)
	}

	#[cfg(test)]
	pub(crate) fn clone_check_table_id_manager(&self, col_id: ColId, size_tier: u8, do_check: bool) -> Option<crate::no_indexing::TableIdManager> {
		self.inner.free_table_id_manager.read().clone_table_id_manager(col_id, size_tier)
			.and_then(|ids| {
				if do_check {
					self.inner.columns[col_id as usize].check_free_list(size_tier, &ids, &self.inner.log.overlays).then(|| ids)
				} else {
					Some(ids)
				}
			})
	}

}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		self.log_thread.take().map(|t| t.join());
		self.flush_thread.take().map(|t| t.join());
		self.commit_thread.take().map(|t| t.join());
		if let Err(e) = self.inner.kill_logs() {
			log::warn!(target: "parity-db", "Shutdown error: {:?}", e);
		}
	}
}

// reference to db for free table handle operation.
#[derive(Clone)]
pub(crate) struct DbHandle {
	inner: Arc<DbInner>,
}

impl DbHandle {
	pub(crate) fn current_free_table_state(&self, col_id: ColId, table_ix: u8) -> (u64, u64) {
		// warning this skip log so only can be use during init.
		self.inner.columns[col_id as usize].current_free_table_state(table_ix)
	}

	pub(crate) fn read_next_free(&self, col_id: ColId, table_ix: u8, free: u64) -> u64 {
		self.inner.columns[col_id as usize].read_next_free_no_indexing(table_ix, free, &*self.inner.log.overlays)
	}

	pub(crate) fn fetch_free_id(&self, handle_id: crate::no_indexing::HandleId, col: ColId, size_tier: u8) -> crate::index::Address {
		self.inner.free_table_id_manager.write().fetch_free_id(handle_id, col, size_tier)
	}

	pub(crate) fn dropped_handle(&self, col_id: ColId, handle_id: crate::no_indexing::HandleId) {
		self.inner.free_table_id_manager.write().dropped_handle(handle_id, col_id)
	}
	pub(crate) fn get_size_tier(&self, col_id: ColId, len: usize) -> u8 {
		let target_tier = self.inner.options.columns[col_id as usize].sizes.iter().position(|t| len <= *t as usize);
		match target_tier {
			Some(tier) => tier as u8,
			None => {
				15
			}
		}
	}
}
