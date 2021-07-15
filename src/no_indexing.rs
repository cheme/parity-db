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
use std::collections::VecDeque;
use std::sync::Arc;
use crate::db::DbHandle;
use parking_lot::{Condvar, Mutex};

pub(crate) type HandleId = u64;

/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
///
/// WARNING this struct locks db on drop and shall allways
/// live longer than the db.
pub struct FreeIdHandle {
	locked: Arc<(Mutex<Lock>, Condvar)>,
	id: HandleId,
	col: ColId, // TODO useless since handle is for a collection?
	read_only: Option<Vec<Option<u64>>>, // contain filled u64 when read_only
	locked_write: bool,
	locked_read: bool,
	no_need_release: bool,
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
				let mut lock = self.locked.0.lock();
				if lock.lock_read(self.id) {
					return;
				}
				self.locked.1.wait(&mut lock);
			}
		} else if self.locked_write {
			loop {
				let mut first = true;
				let mut lock = self.locked.0.lock();
				if lock.lock_write(self.id, first) {
					return;
				}
				first = false;
				self.locked.1.wait(&mut lock);
			}
		}
		unimplemented!();
	}
	/// Get a new id. This synchs with the id manager.
	fn fetch_free_id(&mut self, size_tier: u8) -> u64 {
		if let Some(filled) = self.read_only.as_mut() {
			let filled = &mut filled[size_tier as usize];
			if filled.is_none() {
				let (current_filled, _next_free) = self.db.current_free_table_state(self.col, size_tier);
				*filled = Some(current_filled)
			}
			let filled = filled.as_mut().unwrap();
			*filled += 1;
			return *filled - 1;
		}
		self.db.fetch_free_id(self.id, self.col, size_tier)
	}

	/// Collection for this handle.
	fn get_col(&self) -> ColId {
		unimplemented!()
	}

	/// Consume free handle, and but do not release fetch id.
	/// This need to switch off `drop_handle_inner` when handle
	/// is dropped.
	fn commit(mut self) {
		self.no_need_release = true;
		if self.read_only.is_some() {
			return;
		}
		unimplemented!()
	}

	/// Release fetched ids, put the handle in a consumed state.
	/// Should never panic (called by drop).
	fn drop_handle_inner(&mut self) {
		if self.no_need_release {
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

pub(crate) struct IdManager {
	columns: Vec<Option<ColIdManager>>,
	db: Option<DbHandle>,
}

impl IdManager {
	pub(crate) fn new(options: &crate::options::Options) -> Self {
		let columns = options.columns.iter()
			.map(|c| c.no_indexing.then(|| ColIdManager::default()))
			.collect();
		IdManager { db: None, columns }
	}
	pub(crate) fn init(&mut self, db: DbHandle) {
		self.db = Some(db);
		for c in self.columns.iter_mut() {
			if let Some(col_manager) = c.as_mut() {
				col_manager.init_state(self.db.as_ref().unwrap());
			}
		}
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
	col_id: ColId,
	locked: Arc<(Mutex<Lock>, Condvar)>,
	current_handles: BTreeMap<HandleId, FetchedIds>,
	free_handles: Vec<HandleId>,
	tables: Vec<TableIdManager>,
}

#[derive(Default)]
struct TableIdManager {
	filled: u64,
	next_free: usize,
	// TODO could be last_filled
	pending_filled: usize,
	pending_next_free: usize,
	// head is first
	free_list: VecDeque<(u64, HandleId)>,
	// TODO first tuple item could be len + offset.
	filled_list: VecDeque<(u64, HandleId)>,
}

impl ColIdManager {
	fn init_state(&mut self, db: &DbHandle) {
		for table_ix in 0..crate::table::SIZE_TIERS {
			let mut table_manager = TableIdManager::default();
			let (filled, next_free) = db.current_free_table_state(self.col_id, table_ix as u8);
			table_manager.free_list.push_back((next_free, 0));
			table_manager.filled_list.push_back((filled, 0));
		}
	}
	fn release(&mut self, handle_id: HandleId) {
		let mut lock = self.locked.0.lock();
		if lock.release(handle_id) {
			self.locked.1.notify_all();
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

impl TableIdManager {
	fn fetch_free_id(&mut self, handle_id: crate::no_indexing::HandleId, db: &DbHandle, col_id: ColId, table_ix: u8) -> u64 {
		if self.free_list[self.pending_next_free].0 != 0 {
			// read from removed list
			if self.pending_next_free == self.free_list.len() {
				let next = db.read_next_free(col_id, table_ix, self.free_list[self.pending_next_free].0);
				self.free_list.push_back((next, 0));
			}
			self.pending_next_free += 1;
			self.free_list[self.pending_next_free - 1].1 = handle_id;
			self.free_list[self.pending_next_free - 1].0
		} else {
			// extend table
			self.filled_list[self.pending_filled].1 = handle_id;
			let result = self.filled_list[self.pending_filled].0;
			self.filled_list.push_back((result + 1, 0));
			self.pending_filled += 1;
			result
		}
	}
}

type FetchedIds = Vec<u64>;

impl IdManager {
	// `single_write` to true does opt-in for locking. One must call `ready` afterward.
	pub fn get_handle(&mut self, col_id: ColId, single_write: bool) -> Option<FreeIdHandle> {
		if let Some(col) = self.columns[col_id as usize].as_mut() {
			let handle_id = col.next_handle_id(single_write, false);
			Some(FreeIdHandle {
				locked: col.locked.clone(),
				id: handle_id,
				col: col_id,
				read_only: None,
				locked_write: single_write,
				locked_read: false,
				no_need_release: false,
				is_ready: false,
				db: self.db.as_ref().unwrap().clone(),
			})
		} else {
			None
		}
	}

	// `no_write` to true does opt-in for locking. One must call `ready` afterward.
	pub fn get_read_only_handle(&mut self, col_id: ColId, no_write: bool) -> Option<FreeIdHandle> {
		if let Some(col) = self.columns[col_id as usize].as_mut() {
			let handle_id = if !no_write {
				0 // unmanaged.
			} else {
				col.next_handle_id(false, true)
			};
			Some(FreeIdHandle {
				locked: col.locked.clone(),
				id: handle_id,
				col: col_id,
				read_only: Some(vec![None; crate::table::SIZE_TIERS]),
				locked_write: false,
				locked_read: no_write,
				no_need_release: false,
				is_ready: !no_write,
				db: self.db.as_ref().unwrap().clone(),
			})
		} else {
			None
		}
	}

	pub(crate) fn fetch_free_id(&mut self, handle_id: crate::no_indexing::HandleId, col: ColId, size_tier: u8) -> u64 {
		if let Some(Some(column)) = self.columns.get_mut(col as usize) {
			if let Some(table) = column.tables.get_mut(size_tier as usize) {
				table.fetch_free_id(handle_id, &self.db.as_ref().unwrap(), col, size_tier)
			} else {
				0
			}
		} else {
			0
		}
	}
}
