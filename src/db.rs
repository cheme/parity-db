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
use parking_lot::{Mutex, Condvar};
use fs2::FileExt;
use crate::{
	error::{Error, Result},
	column::{ColId, Column},
	log::{Log, LogAction},
	index::PlanOutcome,
	options::Options,
};

// These are disk-backed, so we use u64
const MAX_LOG_QUEUE_BYTES: u64 = 32 * 1024 * 1024;

/// Value is just a vector of bytes. Value sizes up to 4Gb are allowed.
pub type Value = Vec<u8>;

struct DbInner {
	columns: Vec<Column>,
	options: Options,
	shutdown: AtomicBool,
	log: Log,
	log_worker_cv: Condvar,
	log_work: Mutex<bool>,
	commit_worker_cv: Condvar,
	commit_work: Mutex<bool>,
	log_cv: Condvar,
	log_queue_bytes: Mutex<u64>,
	flush_worker_cv: Condvar,
	flush_work: Mutex<bool>,
	enact_mutex: Mutex<()>,
	last_enacted: AtomicU64,
	next_reindex: AtomicU64,
	collect_stats: bool,
	bg_err: Mutex<Option<Arc<Error>>>,
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
		for c in 0 .. options.columns.len() {
			columns.push(Column::open(c as ColId, &options, salt.clone())?);
		}
		log::debug!(target: "parity-db", "Opened db {:?}, salt={:?}", options, salt);
		Ok(DbInner {
			columns,
			options: options.clone(),
			shutdown: std::sync::atomic::AtomicBool::new(false),
			log: Log::open(&options)?,
			log_worker_cv: Condvar::new(),
			log_work: Mutex::new(false),
			commit_worker_cv: Condvar::new(),
			commit_work: Mutex::new(false),
			log_queue_bytes: Mutex::new(0),
			log_cv: Condvar::new(),
			flush_worker_cv: Condvar::new(),
			flush_work: Mutex::new(false),
			enact_mutex: Mutex::new(()),
			next_reindex: AtomicU64::new(1),
			last_enacted: AtomicU64::new(1),
			collect_stats: options.stats,
			bg_err: Mutex::new(None),
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
		// Go into tables and log overlay.
		let log = self.log.overlays();
		self.columns[col as usize].get(&key, log)
	}

	fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		let key = self.columns[col as usize].hash(key);
		// Go into tables and log overlay.
		let log = self.log.overlays();
		self.columns[col as usize].get_size(&key, log)
	}

	// Commit is simply adds the the data to the queue and to the overlay and
	// exits as early as possible.
	fn commit<I, K>(&self, tx: I) -> Result<()>
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

		let changeset = tx.into_iter().map(
			|(c, k, v)| (c, self.columns[c as usize].hash(k.as_ref()), v)
		);
		let mut writer = self.log.begin_record();
		log::debug!(
			target: "parity-db",
			"Processing commit record {}",
			writer.record_id(),
		);
		let mut ops: u64 = 0;
		let mut reindex = false;
		for (c, key, value) in changeset {
			// TODO write_plan passing by value.
			match self.columns[c as usize].write_plan(&key, &value, &mut writer)? {
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
		self.log.sync_appending()?;

		if reindex {
			self.start_reindex(record_id);
			self.signal_log_worker();
		}
		log::debug!(
			target: "parity-db",
			"Processed commit (record {}), {} ops, {} bytes written",
			record_id,
			ops,
			bytes,
		);
		Ok(())
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
				let bytes = self.log.end_record(l)?;

				log::debug!(
					target: "parity-db",
					"Created reindex record {}, {} bytes",
					record_id,
					bytes,
				);
				let mut logged_bytes = self.log_queue_bytes.lock();
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

	fn flush_logs(&self) -> Result<bool> {
		let (flush_next, read_next) = self.log.flush_one(|| {
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
		while self.flush_logs()? {
			while self.enact_logs(true)? { }
		}
		// Need one more pass to enact the last log.
		while self.enact_logs(true)? { }
		// Re-read any cached metadata
		for c in self.columns.iter() {
			c.refresh_metadata()?;
		}
		log::debug!(target: "parity-db", "Done.");
		Ok(())
	}

	fn shutdown(&self) {
		self.shutdown.store(true, Ordering::SeqCst);
		self.signal_flush_worker();
		self.signal_log_worker();
		self.signal_commit_worker();
		self.log.shutdown();
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
		Self::open(&options)
	}

	/// Open the database with given
	pub fn open(options: &Options) -> Result<Db> {
		let db = Arc::new(DbInner::open(options)?);
		// This needs to be call before log thread: so first reindexing
		// will run in correct state.
		db.replay_all_logs()?;
		let commit_worker_db = db.clone();
		let commit_thread = std::thread::spawn(move ||
			commit_worker_db.store_err(Self::commit_worker(commit_worker_db.clone()))
		);
		let flush_worker_db = db.clone();
		let flush_thread = std::thread::spawn(move ||
			flush_worker_db.store_err(Self::flush_worker(flush_worker_db.clone()))
		);
		let log_worker_db = db.clone();
		let log_thread = std::thread::spawn(move ||
			log_worker_db.store_err(Self::log_worker(log_worker_db.clone()))
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
		while !db.shutdown.load(Ordering::SeqCst) {
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

	fn log_worker(db: Arc<DbInner>) -> Result<()> {
		// Start with pending reindex.
		let mut more_work = db.process_reindex()?;
		while !db.shutdown.load(Ordering::SeqCst) {
			if !more_work {
				let mut work = db.log_work.lock();
				while !*work {
					db.log_worker_cv.wait(&mut work)
				};
				*work = false;
			}

			more_work = db.process_reindex()?;
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
			more_work = db.flush_logs()?;
		}
		log::debug!(target: "parity-db", "Flush worker shutdown");
		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		self.log_thread.take().map(|t| t.join());
		self.flush_thread.take().map(|t| t.join());
		self.commit_thread.take().map(|t| t.join());
	}
}
