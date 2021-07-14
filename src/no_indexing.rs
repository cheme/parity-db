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

/// Free table utilities.
///
/// Free table with option `no_indexing` are usable
/// by using directly the table address in u64 be encoded
/// form as key of current api.
/// To query this work as usual, just skipping the indexing
/// key query.
///
/// For updating only, this could run fine, but if new entry
/// are needed, the system do not know new id before logging thread
/// writes log.
/// In some case (external radix tree indexing), the index is needed
/// before even committing.
///
/// Thus a free index management is defined here to allow returning
/// index to clients and keeping a in memory view of the future (post log)
/// indexing state.


use crate::column::ColId;
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::db::DbHandle;
use parking_lot::{Condvar, Mutex};

type HandleId = u64;

/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
///
/// WARNING this struct locks db on drop and shall allways
/// live longer than the db.
struct FreeIdHandle {
	locked: Mutex<Lock>,
	locked_cv: Condvar,
	id: HandleId,
	col: ColId,
	read_only: bool,
	locked_write: bool,
	locked_read: bool,
	need_release: bool,
	is_ready: bool,
	db: DbHandle,
}
	
/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
impl FreeIdHandle {
	/// wait on read write handle if needed.
	/// TODO put in its own struct instead of expecting
	/// it to be call.
	fn ready(&mut self) {
		if self.is_ready {
			return;
		}
		self.is_ready = true;
		if self.locked_read {
			loop {
				let mut lock = self.locked.lock();
				if lock.lock_read(self.id) {
					return;
				}
				self.locked_cv.wait(&mut lock);
			}
		} else if self.locked_write {
			loop {
				let mut first = true;
				let mut lock = self.locked.lock();
				if lock.lock_write(self.id, first) {
					return;
				}
				first = false;
				self.locked_cv.wait(&mut lock);
			}
		}
		unimplemented!();
	}
	/// Get a new id. This synchs with the id manager.
	fn fetch_free_id(&mut self, size_tier: u8) -> u64 {
		unimplemented!()
	}

	/// Collection for this handle.
	fn get_col(&self) -> ColId {
		unimplemented!()
	}

	/// Consume free handle, and but do not release fetch id.
	/// This need to switch off `drop_handle_inner` when handle
	/// is dropped.
	fn commit(mut self) {
		self.need_release = false;
		if self.read_only {
			return;
		}
		unimplemented!()
	}

	/// Release fetched ids, put the handle in a consumed state.
	/// Should never panic (called by drop).
	fn drop_handle_inner(&mut self) {
		if !self.need_release {
			return;
		}
		unimplemented!()
	}
	
	/// Release fetched ids.
	fn drop_handle(self) { }
}

impl Drop for FreeIdHandle {
	fn drop(&mut self) {
		self.drop_handle_inner()
	}
}

struct IdManager {
	columns: Vec<Option<ColIdManager>>,
	db: DbHandle,
}

impl IdManager {
	fn new(db: DbHandle, options: &crate::options::Options) -> Self {
		let mut columns = Vec::with_capacity(options.columns.len());
		for c in options.columns.iter() {
			if c.no_indexing {
				columns.push(Some(ColIdManager::default()));
			} else {
				columns.push(None);
			}
		}
		IdManager { db, columns }
	}
}

// single writer, no reads, prio on write.
// (only if opted in as can be pretty bad).
#[derive(Default)]
struct Lock {
	current_write: Option<HandleId>,
	pending_write: Vec<HandleId>,
	current_reads: Vec<HandleId>,
}

impl Lock {
	fn release(&mut self, id: HandleId) -> bool {
		if self.current_write == Some(id) {
			self.current_write = None;
			return true;
		}
		if self.current_reads.iter().position(|i| i == &id)
				.map(|ix| self.current_reads.remove(ix)).is_some() {
			return true;
		}
		if self.pending_write.iter().position(|i| i == &id)
				.map(|ix| self.pending_write.remove(ix)).is_some() {
			return true;
		}
		false
	}

	fn lock_read(&mut self, id: HandleId) -> bool {
		if self.current_write.is_some() || !self.pending_write.is_empty() {
			return false;
		}
		self.current_reads.push(id);
		true
	}

	fn lock_write(&mut self, id: HandleId, first: bool) -> bool {
		if self.current_write.is_some() || !self.current_reads.is_empty() {
			if first {
				self.pending_write.push(id);
			}
			return false;
		}
		self.pending_write.iter().position(|i| i == &id)
			.map(|ix| self.pending_write.remove(ix));
		self.current_write = Some(id);
		true
	}
}

/// Id management view.
#[derive(Default)]
struct ColIdManager {
	locked: Mutex<Lock>,
	locked_cv: Condvar,
	current_handles: BTreeMap<HandleId, FetchedIds>,
	free_handles: Vec<HandleId>,
}

impl ColIdManager {
	fn release(&mut self, handle_id: HandleId) {
		let mut lock = self.locked.lock();
		if lock.release(handle_id) {
			self.locked_cv.notify_all();
		}
	}
	fn next_handle_id(&mut self, single_write: bool, read_lock: bool) -> HandleId {
		debug_assert!(!(single_write && read_lock));
		let id = if let Some(id) = self.free_handles.pop() {
			id
		} else {
			if let Some((id, _)) = self.current_handles.iter().next_back() {
				*id
			} else {
				1 // 0 is reserved
			}
		};
		if single_write {
		} else if read_lock {
			self.free_handles.push(id);
		}
		id
	}
}

type FetchedIds = Vec<u64>;

impl IdManager {
	// `single_write` to true does opt-in for locking. One must call `ready` afterward.
	pub fn get_handle(&mut self, col: ColId, single_write: bool) -> Option<FreeIdHandle> {
		if let Some(col) = self.columns[col as usize].as_mut() {
			let handle_id = col.next_handle_id(single_write, false);
			unimplemented!()
		} else {
			None
		}
	}

	// `no_write` to true does opt-in for locking. One must call `ready` afterward.
	pub fn get_read_only_handle(&mut self, col: ColId, no_write: bool) -> Option<FreeIdHandle> {
		if let Some(col) = self.columns[col as usize].as_mut() {
			let handle_id = if !no_write {
				0 // unmanaged.
			} else {
				col.next_handle_id(false, true)
			};
			unimplemented!()
		} else {
			None
		}
	}
}
