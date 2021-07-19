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
///
/// Free index allows multiple write, so non complete adds on commit, in
/// this case empty entry are added (for other write reserved ids).
/// To ensure consistency, on commit:
/// - write on a deleted entry is using an attached next_free info (deleted entry
/// only contains previous_free), and just insert.
/// - write of an empty entry after filled is stored in a temporary free list where
/// we keep head and tail and previous entry is always n - 1, skipping filled entry.
/// - delete of entry writes in a similar temporary free list.
/// - on commit_end first free list is attached, then second free list is attached:
/// keeping index changes far from head (so could be remove from cache when other concurrent
/// handles return).


use crate::column::ColId;
use crate::db::Value;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use crate::db::DbHandle;
use crate::index::Address;
use parking_lot::{Condvar, Mutex};

pub(crate) type HandleId = u64;

/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
///
/// WARNING this struct locks db on drop potentially
/// resulting in deadlocks if misused.
pub struct FreeIdHandle {
	db: DbHandle,
	id: HandleId,
	col: ColId,
	locked: Arc<(Mutex<Lock>, Condvar)>,
	read_only: Option<Vec<Option<u64>>>, // contain filled u64 when read_only
	locked_write: bool,
	locked_read: bool,
	no_need_release: bool,
	is_ready: bool,
}
/*
// TODO something lighter than btreemap
pub(crate) struct FreeIdHandleCommitPayload(BTreeMap<ColId, Vec<ColFreeIdHandleCommitPayload>>);

pub(crate) struct ColFreeIdHandleCommitPayload {
	// For update of removed entry, contains item and next item in free list.
	free_list: Vec<(u64, u64)>,
	// For insert of filled entry every item is considered as deleted
	// with previous element being n - 1.
	// To avoid useless update, this contains items and their next item when
	// they do not use n + 1 as next.
	// TODO consider fusing with free_list after testing
	filled: Vec<(u64, u64)>,
	old_filled: u64,
}

impl ColFreeIdHandleCommitPayload {
	fn next_removed(&self, pos: u64) -> Option<u64> {
		if let Ok(ix) = self.free_list.binary_search_by_key(&pos, |i| i.0) {
			return Some(self.free_list[ix].1);
		}
		if let Ok(ix) = self.filled.binary_search_by_key(&pos, |i| i.0) {
			return Some(self.filled[ix].1);
		}
		if pos >= self.old_filled {
			return Some(pos + 1);
		}
		None
	}
}
*/
/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
impl FreeIdHandle {
	/// wait on read write handle if needed.
	/// TODO put in its own struct instead of expecting
	/// it to be call.
	/// TODO split lock related method from others in this source file
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
				let mut lock = self.locked.0.lock();
				if lock.lock_write(self.id) {
					return;
				}
				self.locked.1.wait(&mut lock);
			}
		}
		self.is_ready = true;
	}
	/// Get a new id. This synchs with the id manager.
	fn fetch_free_id(&mut self, size_tier: u8) -> u64 {
		if let Some(filled) = self.read_only.as_mut() {
			// Read only mode, just return higher ids.
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
		self.col
	}
/*
	/// Consume free handle, but do not release fetch id.
	/// This need to switch off `drop_handle_inner` when handle
	/// is dropped.
	fn commit(mut self) -> bool {
		self.no_need_release = true;
		!self.read_only.is_some()
	}
*/
	/// Release fetched ids, put the handle in a consumed state.
	/// Should never panic (called by drop).
	fn drop_handle_inner(&mut self) {
		if self.no_need_release {
			return;
		}
		self.db.dropped_handle(self.col, self.id);

		let mut lock = self.locked.0.lock();
		lock.release(self.id);
	}
	
	/// Release fetched ids.
	fn drop_handle(self) { }
}

enum ExtendedKey<K> {
	// key from standard column.
	Full(K),
	// 8 bytes u64 index. TODO consider copying in [u8; 8] ?
	U64BE(K),
	// 8 bytes u64 index, and 8 byte next free list index. 
	U64BEOnFree([u8; 16]),
}

impl<K: AsRef<[u8]>> AsRef<[u8]> for ExtendedKey<K> {
	fn as_ref(&self) -> &[u8] {
		match self {
			ExtendedKey::Full(k) => k.as_ref(),
			ExtendedKey::U64BE(k) => k.as_ref(),
			ExtendedKey::U64BEOnFree(k) => k.as_ref(),
		}
	}
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
	pub(crate) fn dropped_handle(&mut self, handle_id: HandleId, col_id: ColId) {
		if let Some(col) = self.columns[col_id as usize].as_mut() {
			col.dropped_handle(handle_id)
		}
	}

	// TODO could be more effitcient if ordered by (col, key). See commit interface.
	pub(crate) fn commit_entry<K>(
		&mut self,
		col: ColId,
		key: impl AsRef<[u8]>,
		handle: HandleId,
	) -> impl AsRef<[u8]>	{
		if let Some(col) = self.columns[col as usize].as_mut() {
			return col.commit_entry(key, handle);
		}
		ExtendedKey::Full(key)
	}

	// To be called after data is commited and handle has been drop to update state.
	pub(crate) fn commit(&mut self) {
	}
/*
	// Can fail if incorrect size key in payload.
	// To be call by db commit when using handle_id.
	pub(crate) fn extract_commit_payload<'a, I, K>(
		&mut self,
		changes: &'a I,
		handles: BTreeMap<ColId, HandleId>,
	) -> Option<FreeIdHandleCommitPayload>
	where
		I: Iterator<Item=&'a (ColId, K, Option<Value>)>,
		K: AsRef<[u8]> + 'a,
	{
		// TODO a non iterator commit could 
		unimplemented!("TODO call db which will remove");
	}

	fn combine_payload<I, K>(
		changes: I,
		payload: FreeIdHandleCommitPayload,
	) -> impl IntoIterator<Item = (ColId, ExtendedKey<K>, Option<Value>)>
	where
		I: IntoIterator<Item=(ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		changes.into_iter().map(move |(col, key, value)| {
			let key = if let Some(ids) = payload.0.get(&col) {
				let address = Address::from_be_slice(&key.as_ref()[0..8]);
				let size_tier = address.size_tier();
				let address = address.as_u64();
				if let Some(next) = ids.get(size_tier as usize)
					.and_then(|t| t.next_removed(address)) {
					let mut buf = [0; 16];
					buf[0..8].copy_from_slice(&address.to_be_bytes()[..]);
					buf[8..16].copy_from_slice(&next.to_be_bytes()[..]);
					ExtendedKey::U64BEOnFree(buf)
				} else {
					ExtendedKey::U64BE(key)
				}
			} else {
				ExtendedKey::Full(key)
			};
			(col, key, value)
		})
	}
*/
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

	fn lock_write(&mut self, id: HandleId) -> bool {
		if self.current_write.is_some() || !self.current_reads.is_empty() {
			self.pending_write.push(id);
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
	current_handles: BTreeMap<HandleId, Vec<FetchedIds>>,
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
	free_list: VecDeque<(u64, HandleState)>,
	// TODO first tuple item could be len + offset.
	filled_list: VecDeque<(u64, HandleState)>,
}

#[derive(PartialEq, Eq)]
enum HandleState {
	Free,
	Used(HandleId),
	Consumed,
}
impl ColIdManager {
	fn init_state(&mut self, db: &DbHandle) {
		for table_ix in 0..crate::table::SIZE_TIERS {
			let mut table_manager = TableIdManager::default();
			let (filled, next_free) = db.current_free_table_state(self.col_id, table_ix as u8);
			table_manager.free_list.push_back((next_free, HandleState::Free));
			table_manager.filled_list.push_back((filled, HandleState::Free));
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
	fn commit_entry<K>(
		&mut self,
		key: K,
		handle: HandleId,
	) -> ExtendedKey<K>
	where
		K: AsRef<[u8]>,
	{
		if self.current_handles.contains_key(&handle) {
			// we do not update fetched ids list (will happen on drop).
			let address = Address::from_be_slice(&key.as_ref()[0..8]);
			let size_tier = address.size_tier();
			if let Some(with_next) = self.tables[size_tier as usize].commit_entry(address, handle) {
				return with_next;
			}
		}
		ExtendedKey::Full(key)
	}
	fn dropped_handle(&mut self, handle_id: HandleId) {
		if let Some(fetched_ids) = self.current_handles.remove(&handle_id) {
			for (table_ix, fetched_id) in fetched_ids.into_iter().enumerate() {
				if fetched_id.len() > 0 {
					self.tables[table_ix].dropped_handle(handle_id, fetched_id);
				}
			}
			// reuse handle
			self.free_handles.push(handle_id);
		}
	}
}

impl TableIdManager {
	fn commit_entry<K>(
		&mut self,
		address: Address,
		handle: HandleId,
	) -> Option<ExtendedKey<K>>
	where
		K: AsRef<[u8]>,
	{
		// TODO ever indexed struct or group search, but this iter seems slow: switch btreemap and
		// radix-tree later?? (but changed dropped_handle a bit).
		if self.filled_list.front().map(|f| address.as_u64() >= f.0).unwrap_or(false) {
			for filled in self.filled_list.iter_mut() {
				if filled.0 > address.as_u64() {
					break;
				}
				if let HandleState::Used(id) = &filled.1 {
					if filled.0 == address.as_u64() {
						return None;
					}
				}
			}
		} else {
			let mut from = None;
			let mut to = None;
			for free in self.free_list.iter_mut() {
				if from.is_none() {
					if let HandleState::Used(id) = &free.1 {
						if free.0 == address.as_u64() {
							debug_assert!(id == &handle);
							from = Some(free.0);
							free.1 = HandleState::Consumed;
						}
					}
				} else {
					match free.1 {
						HandleState::Consumed => (),
						HandleState::Used(_id) => {
							// Note that if keys where resolved ordered we could
							// skip when id is equal to handle and just write with from.
							to = Some(free.0);
						},
						HandleState::Free => {
							to = Some(free.0);
						},
					}
				}
			}
			match (from, to) {
				(Some(from), Some(to)) => {
					let mut buf = [0; 16];
					buf[0..8].copy_from_slice(&from.to_be_bytes()[..]);
					buf[8..16].copy_from_slice(&to.to_be_bytes()[..]);
					return Some(ExtendedKey::U64BEOnFree(buf));
				},
				(None, Some(_))
				| (Some(_), None) => unreachable!("See free list construction"),
				_ => (),
			}
		}
		debug_assert!(false);
		None
	}
	fn dropped_handle(&mut self, handle_id: HandleId, fetched_ids: Vec<FetchedId>) {
		for fetch_id in fetched_ids.into_iter().rev() {
			match fetch_id {
				FetchedId::Filled(ix) => {
					debug_assert!(&self.filled_list[ix].1 != &HandleState::Free);
					if let HandleState::Used(id) = &self.filled_list[ix].1 {
						debug_assert!(id == &handle_id);
						if ix + 1 == self.filled_list.len() {
							self.filled_list.pop_back();
							self.pending_filled -= 1;
						} else {
							self.filled_list[ix].1 = HandleState::Free;
						}
					}
				},
				FetchedId::Free(ix) => {
					debug_assert!(&self.free_list[ix].1 != &HandleState::Free);
					if let HandleState::Used(id) = &self.free_list[ix].1 {
						debug_assert!(id == &handle_id);
						if ix + 1 == self.free_list.len() {
							self.free_list.pop_back();
							self.pending_next_free -= 1;
						} else {
							self.free_list[ix].1 = HandleState::Free;
						}
					}
				},
		// TODO also consider dropping free list that is old (back) and with id 0 as
		// it can be fetch again when needed: should be use with a buffer size and
		// implemented as a maintenance method.
		// -> would need to use offset to avoid updating all registered ix
			}
		}
		/*
		while self.free_list.front().map(|f| f.1 == handle_id).unwrap_or(false) {
			let _ = self.free_list.pop_front();
			self.pending_next_free -= 1;
		}
		for f in self.free_list.iter_mut() {
			if f.1 == handle_id {
				f.1 = 0;
			}
		}

		while self.filled_list.front().map(|f| f.1 == handle_id).unwrap_or(false) {
			let _ = self.filled_list.pop_front();
			self.pending_filled -= 1;
		}
		for f in self.filled_list.iter_mut() {
			if f.1 == handle_id {
				f.1 = 0;
			}
		}
		*/


	}

	fn fetch_free_id(&mut self, handle_id: HandleId, db: &DbHandle, col_id: ColId, table_ix: u8) -> u64 {
		if self.free_list[self.pending_next_free].0 != 0 {
			// read from removed list
			if self.pending_next_free == self.free_list.len() {
				let next = db.read_next_free(col_id, table_ix, self.free_list[self.pending_next_free].0);
				self.free_list.push_back((next, HandleState::Free));
			}
			self.pending_next_free += 1;
			self.free_list[self.pending_next_free - 1].1 = HandleState::Used(handle_id);
			self.free_list[self.pending_next_free - 1].0
		} else {
			// extend table
			self.filled_list[self.pending_filled].1 = HandleState::Used(handle_id);
			let result = self.filled_list[self.pending_filled].0;
			self.filled_list.push_back((result + 1, HandleState::Free));
			self.pending_filled += 1;
			result
		}
	}
}

type FetchedIds = Vec<FetchedId>;

enum FetchedId {
	Filled(usize),
	Free(usize),
}

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
				no_need_release: !no_write,
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
