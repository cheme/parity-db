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
/// - entries after filled are seen as a contigus free list and updated consequentyl.
/// - delete of entry writes in a detached list that is only written after the free list
/// when all is committed.
/// - then detached delete list is written at last fetched id and this management is updated
/// to use this as first free list pointer.


use crate::column::ColId;
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
	pub(crate) id: HandleId,
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
	fn fetch_free_id(&mut self, value_size: usize) -> u64 {
		let size_tier = self.db.get_size_tier(self.col, value_size);
		self.fetch_free_id_size_tier(size_tier)
	}
	/// Get a new id. This synchs with the id manager.
	fn fetch_free_id_size_tier(&mut self, size_tier: u8) -> u64 {
		if let Some(filled) = self.read_only.as_mut() {
			// Read only mode, just return higher ids.
			let filled = &mut filled[size_tier as usize];
			if filled.is_none() {
				let (current_filled, _next_free) = self.db.current_free_table_state(self.col, size_tier);
				*filled = Some(current_filled)
			}
			let filled = filled.as_mut().unwrap();
			*filled += 1;
			return Address::new(*filled - 1, size_tier).as_u64();
		}
		self.db.fetch_free_id(self.id, self.col, size_tier).as_u64()
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
		if lock.release(self.id) {
			self.locked.1.notify_all();
		}
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

	#[cfg(test)]
	pub(crate) fn clone_table_id_manager(&self, col_id: ColId, size_tier: u8) -> Option<crate::no_indexing::TableIdManager> {
		self.columns.get(col_id as usize).and_then(|col| col.as_ref().and_then(|col| col.tables.get(size_tier as usize).cloned()))
	}

	pub(crate) fn new(options: &crate::options::Options) -> Self {
		let columns = options.columns.iter().enumerate()
			.map(|(id, c)| c.no_indexing.then(|| ColIdManager::new(id as u8)))
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
	pub(crate) fn commit_entry(
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

	pub(crate) fn removed_index(
		&mut self,
		col: ColId,
		key: &[u8],
	) {
		if let Some(col) = self.columns[col as usize].as_mut() {
			return col.removed_index(key);
		}
	}
	// To be called after data is commited and handle has been drop to update state.
	pub(crate) fn commit(&mut self) {
		// TODO clean up of back of free.
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
struct ColIdManager {
	col_id: ColId,
	locked: Arc<(Mutex<Lock>, Condvar)>,
	current_handles: BTreeMap<HandleId, ()>,
	free_handles: Vec<HandleId>,
	tables: Vec<TableIdManager>,
}

impl ColIdManager {
	fn new(col_id: ColId) -> Self {
		let mut tables = Vec::with_capacity(16);
		for _ in 0..16 {
			tables.push(TableIdManager::default());
		}
		ColIdManager {
			col_id,
			locked: Default::default(),
			current_handles: Default::default(),
			free_handles: Default::default(),
			tables,
		}
	}
}

#[derive(Default, Clone, Debug)] // TODO clone and debug test only
pub(crate) struct TableIdManager {
	size_tier: u8,
	filled: u64,
	// head is first
	free_list: VecDeque<(u64, HandleState)>,
}

#[derive(PartialEq, Eq, Clone, Debug)] // TODO clone and debug test only
enum HandleState {
	Free,
	Used(HandleId),
	Consumed,
}
impl ColIdManager {
	fn init_state(&mut self, db: &DbHandle) {
		for table_ix in 0..crate::table::SIZE_TIERS {
			let mut table_manager = &mut self.tables[table_ix];
			let (filled, next_free) = db.current_free_table_state(self.col_id, table_ix as u8);
			table_manager.filled = filled;
			table_manager.free_list.push_back((next_free, HandleState::Free));
			table_manager.size_tier = table_ix as u8;
		}
	}
	// TODO unused?? (handle does it)
	fn release(&mut self, handle_id: HandleId) {
		let mut lock = self.locked.0.lock();
		if lock.release(handle_id) {
			self.locked.1.notify_all();
		}
	}
	fn next_handle_id(&mut self, write: bool, single_write: bool, read_lock: bool) -> HandleId {
		if !write && !read_lock {
			return 0;
		}
		debug_assert!(!(single_write && read_lock));
		let id = if let Some(id) = self.free_handles.pop() {
			id
		} else {
			if let Some((id, _)) = self.current_handles.iter().next_back() {
				*id + 1
			} else {
				0
			}
		};
		self.current_handles.insert(id, ());
		//self.current_handles.insert(id, vec![Default::default(); crate::table::SIZE_TIERS]);
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
	pub(crate) fn removed_index(
		&mut self,
		key: &[u8],
	) {
		let address = Address::from_be_slice(&key.as_ref()[0..8]);
		let size_tier = address.size_tier();
		self.tables[size_tier as usize].removed_index(address);
	}

	fn dropped_handle(&mut self, handle_id: HandleId) {
		if let Some(fetched_ids) = self.current_handles.remove(&handle_id) {
			for table_ix in  0 .. crate::table::SIZE_TIERS {
				// TODO can target more precisely by adding fetched size tier
				// or even the fetched ids.
				self.tables[table_ix].dropped_handle(handle_id);
			}
/*			for (table_ix, fetched_id) in fetched_ids.into_iter().enumerate() {
				if fetched_id.len() > 0 {
					self.tables[table_ix].dropped_handle(handle_id, fetched_id);
				}
			}*/
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
		let mut from = None;
		let mut to = None;
		for free in self.free_list.iter_mut() {
			if from.is_none() {
				if let HandleState::Used(id) = &free.1 {
					if free.0 == address.offset() {
						debug_assert!(id == &handle);
						from = Some(Address::new(free.0, self.size_tier));
						free.1 = HandleState::Consumed;
					}
				}
			} else {
				match free.1 {
					HandleState::Consumed => (),
					HandleState::Used(_id) => {
						// Note that if keys where resolved ordered we could
						// skip when id is equal to handle and just write with from.
						to = Some(Address::new(free.0, self.size_tier));
					},
					HandleState::Free => {
						to = Some(Address::new(free.0, self.size_tier));
					},
				}
			}
		}
		match (from, to) {
			(Some(from), Some(to)) => {
				let mut buf = [0; 16];
				buf[0..8].copy_from_slice(&from.as_u64().to_be_bytes()[..]);
				buf[8..16].copy_from_slice(&to.as_u64().to_be_bytes()[..]);
				return Some(ExtendedKey::U64BEOnFree(buf));
			},
			(None, Some(_))
			| (Some(_), None) => unreachable!("See free list construction"),
			_ => (),
		}
		debug_assert!(false);
		None
	}
	fn removed_index(&mut self, address: Address) {
		let offset = address.offset();
		// remove if in free_list TODO double index??
		if let Some(pos) = self.free_list.iter().position(|(a, _)| a == &offset) {
			self.free_list.remove(pos);
		}
		self.free_list.push_front((offset, HandleState::Free));
	}
	//fn dropped_handle(&mut self, handle_id: HandleId, fetched_ids: Vec<usize>) {
	fn dropped_handle(&mut self, handle_id: HandleId) {
		// TODO can have something faster
		for (_, state) in self.free_list.iter_mut() {
			if state == &HandleState::Used(handle_id) {
				*state = HandleState::Free;
			}
		}
		/*
		for ix in fetched_ids.into_iter().rev() {
			debug_assert!(&self.free_list[ix].1 != &HandleState::Free);
			if let HandleState::Used(id) = &self.free_list[ix].1 {
				debug_assert!(id == &handle_id);
				if ix + 1 == self.free_list.len() {
					unreachable!("free list contains a next free (0 if empty).");
				} else {
					self.free_list[ix].1 = HandleState::Free;
				}
			}
			// TODO also consider dropping free list that is old (back) and with id 0 as
			// it can be fetch again when needed: should be use with a buffer size and
			// implemented as a maintenance method.
			// -> would need to use offset to avoid updating all registered ix
		}
		*/
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
		let len = self.free_list.len();
		debug_assert!(len > 0);
		// TODO manage ix of free to avoid this iters.
		for (ix, free) in self.free_list.iter_mut().enumerate() {
			if ix + 1 != len && free.1 == HandleState::Free && free.0 != 0 {
				free.1 = HandleState::Used(handle_id);
				return free.0;
			}
		}

		if let Some(next) = self.free_list.back() {
			if next.0 != 0 {
				// read from removed list
				let next = db.read_next_free(col_id, table_ix, next.0);
				self.free_list.push_back((next, HandleState::Free));
				self.free_list[len - 1].1 = HandleState::Used(handle_id);
				return self.free_list[len - 1].0;
			}
		};
		// extend from filled
		self.free_list.push_front((self.filled, HandleState::Used(handle_id)));
		self.filled += 1;
		self.filled - 1
	}
}

type FetchedIds = Vec<usize>;

impl IdManager {
	// `single_write` to true does opt-in for locking. One must call `ready` afterward.
	pub fn get_handle(&mut self, col_id: ColId, single_write: bool) -> Option<FreeIdHandle> {
		if let Some(col) = self.columns[col_id as usize].as_mut() {
			let handle_id = col.next_handle_id(true, single_write, false);
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
				col.next_handle_id(false, false, true)
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

	pub(crate) fn fetch_free_id(&mut self, handle_id: crate::no_indexing::HandleId, col: ColId, size_tier: u8) -> Address {
		if let Some(Some(column)) = self.columns.get_mut(col as usize) {
			if let Some(table) = column.tables.get_mut(size_tier as usize) {
				let offset = table.fetch_free_id(handle_id, &self.db.as_ref().unwrap(), col, size_tier);
				Address::new(offset, size_tier)
			} else {
				Address::from_u64(0)
			}
		} else {
			Address::from_u64(0)
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::options::{Options, ColumnOptions};
	use std::collections::BTreeMap;
	use super::{FreeIdHandle};

	fn options(name: &str) -> Options {
		env_logger::try_init().ok();
		let mut path: std::path::PathBuf = std::env::temp_dir();
		path.push("parity-db-test");
		path.push(name);
		if path.exists() {
			std::fs::remove_dir_all(&path).unwrap();
		}
		std::fs::create_dir_all(&path).unwrap();

		let mut col_option = ColumnOptions::default();
		col_option.no_indexing = true;
		Options {
			path,
			columns: vec![col_option],
			sync: false,
			stats: false,
		}
	}


	fn prepare_add(
		db: &crate::Db,
		handle: Option<FreeIdHandle>,
		state: &mut BTreeMap<u8, (u64, Option<Vec<u8>>)>,
		writer: &mut BTreeMap<u64, Option<Vec<u8>>>,
		range: (u8, u8),
	) -> FreeIdHandle {
		let mut handle = handle.unwrap_or_else(|| db.get_handle(0, false).unwrap());
		handle.ready();
		for i in range.0 .. range.1 {
			let mut value = b"test_value".to_vec();
			value.push(i);
			let id = handle.fetch_free_id(value.len());
			state.insert(i, (id, Some(value.clone())));
			writer.insert(id, Some(value));
		}
		handle
	}

	fn prepare_remove(
		db: &crate::Db,
		handle: Option<FreeIdHandle>,
		state: &mut BTreeMap<u8, (u64, Option<Vec<u8>>)>,
		writer: &mut BTreeMap<u64, Option<Vec<u8>>>,
		range: (u8, u8),
	) -> FreeIdHandle {
		let mut handle = handle.unwrap_or_else(|| db.get_handle(0, false).unwrap());
		handle.ready();
		for i in range.0 .. range.1 {
			if let Some(id) = state.remove(&i).map(|s| s.0) {
				writer.insert(id, None);
			}
		}
		handle
	}

	fn commit_with_handle(db: &crate::Db, handle: FreeIdHandle, writer: &mut BTreeMap<u64, Option<Vec<u8>>>) {
		let mut handle_map = BTreeMap::new();
		handle_map.insert(0, handle);
		db.commit_with_non_canonical(writer.iter().map(|(id, v)| {
			(0, id.to_be_bytes().to_vec(), v.clone()) 
		}), handle_map);
		writer.clear();
	}

	fn check_state(
		db: &crate::Db,
		state: &BTreeMap<u8, (u64, Option<Vec<u8>>)>,
	) {
		for (_, (id, val)) in state.iter() {
			assert_eq!(val, &db.get(0, &id.to_be_bytes()[..]).unwrap());
		}
	}

	fn wait_log() {
		use std::{thread, time};

		let sleep = time::Duration::from_millis(500);
		thread::sleep(sleep);
	}
	
	#[test]
	fn test_no_locks() {
		let options = options("test_no_lock");
		let db = crate::Db::open(&options).unwrap();
		let mut state = BTreeMap::<u8, (u64, Option<Vec<u8>>)>::new();
		let mut writer = BTreeMap::<u64, Option<Vec<u8>>>::new();
		let handle = prepare_add(&db, None, &mut state, &mut writer, (0, 5));
		commit_with_handle(&db, handle, &mut writer);

		check_state(&db, &state);
		wait_log();
		check_state(&db, &state);

		let handle = prepare_add(&db, None, &mut state, &mut writer, (6, 10));
		let handle = prepare_remove(&db, Some(handle), &mut state, &mut writer, (4, 7));

		commit_with_handle(&db, handle, &mut writer);

		check_state(&db, &state);
		wait_log();
		check_state(&db, &state);

		let handle = prepare_add(&db, None, &mut state, &mut writer, (11, 12));

		commit_with_handle(&db, handle, &mut writer);

		check_state(&db, &state);
		wait_log();
		check_state(&db, &state);
	}

	#[test]
	fn test_no_locks_2() {
		let options = options("test_no_lock_2");
		let db = crate::Db::open(&options).unwrap();
		let mut state = BTreeMap::<u8, (u64, Option<Vec<u8>>)>::new();
		let mut dropped_state = BTreeMap::<u8, (u64, Option<Vec<u8>>)>::new();
		let mut writer = BTreeMap::<u64, Option<Vec<u8>>>::new();
		let handle_1 = prepare_add(&db, None, &mut dropped_state, &mut writer, (50, 55));
		writer.clear();
		let handle_2 = prepare_add(&db, None, &mut state, &mut writer, (0, 5));
		std::mem::drop(handle_1);
		commit_with_handle(&db, handle_2, &mut writer);

		check_state(&db, &state);

		wait_log();
		
		check_state(&db, &state);

		let handle = prepare_add(&db, None, &mut state, &mut writer, (6, 10));
		// 5 and 10
		let handle = prepare_remove(&db, Some(handle), &mut state, &mut writer, (4, 7));

		commit_with_handle(&db, handle, &mut writer);

		check_state(&db, &state);
		wait_log();
		check_state(&db, &state);

		let handle = prepare_add(&db, None, &mut state, &mut writer, (11, 13));

		commit_with_handle(&db, handle, &mut writer);

		check_state(&db, &state);
		wait_log();
		check_state(&db, &state);

		let free_list = db.clone_table_id_manager(0, 0).unwrap();
		panic!("{:?}", free_list);
	}

	#[test]
	fn test_locks() {
		use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
		let mut progress = 0;
		let options = options("test_locks");
		let db = crate::Db::open(&options).unwrap();
		// read lock
		let mut handle_read = db.get_read_only_handle(0, true).unwrap();
		handle_read.ready();
		let mut handle_read_2 = db.get_read_only_handle(0, true).unwrap();
		handle_read_2.ready();
		let mut handle_write = db.get_handle(0, true).unwrap();
		let join = Arc::new(AtomicBool::new(false));
		let join2 = join.clone();
		let write_ready = std::thread::spawn(move || {
			handle_write.ready();
			join2.store(true, Ordering::Relaxed);
		});
		if !join.as_ref().load(Ordering::Relaxed) {
			std::mem::drop(handle_read);
			wait_log();
			progress +=1;
		}
		if !join.load(Ordering::Relaxed) {
			std::mem::drop(handle_read_2);
			progress +=1;
		}
		wait_log();
		assert!(join.load(Ordering::Relaxed));
		assert_eq!(progress, 2);
	}
}
