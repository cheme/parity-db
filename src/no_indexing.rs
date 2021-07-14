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

/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
struct FreeIdHandle<H: FreeIdHandleTrait>(H);
	
/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
trait FreeIdHandleTrait: Sized {
	/// Get a new id. This synchs with the id manager.
	fn fetch_free_id(&mut self, size_tier: u8) -> u64;

	/// Collection for this handle.
	fn get_col(&self) -> ColId;

	/// Consume free handle, and but do not release fetch id.
	/// This need to switch off `drop_handle_inner` when handle
	/// is dropped.
	fn commit(self);

	/// Release fetched ids, put the handle in a consumed state.
	/// Should never panic (called by drop).
	fn drop_handle_inner(&mut self);
	
	/// Release fetched ids.
	fn drop_handle(self) {
		// calls drop from drop implementation.
	}
}

impl<H: FreeIdHandleTrait> Drop for FreeIdHandle<H> {
	fn drop(&mut self) {
		self.0.drop_handle_inner()
	}
}

/// Handle to the index management for querying new ids.
struct Handle;

impl FreeIdHandleTrait for Handle {
	fn fetch_free_id(&mut self, size_tier: u8) -> u64 {
		unimplemented!()
	}

	fn get_col(&self) -> ColId {
		unimplemented!()
	}

	fn commit(self) {
		unimplemented!()
	}

	fn drop_handle_inner(&mut self) {
		unimplemented!()
	}
}


/// Handle to get free id, but do not allow committing.
/// (commit does panic).
struct DummyHandle {
	from: u64,
	col: ColId,
}

impl FreeIdHandleTrait for DummyHandle {
	fn fetch_free_id(&mut self, _size_tier: u8) -> u64 {
		let result = self.from;
		self.from += 1; // TODO check if writing over a size_tier. 
		result
	}

	fn get_col(&self) -> ColId {
		self.col
	}

	fn commit(self) {
		panic!("Dummy handle is not synch.");
	}

	fn drop_handle_inner(&mut self) { }
}

impl DummyHandle {
	/// from should be max 'filled' from first columns (size tier always 0).
	pub fn new(col: ColId, from: u64) -> FreeIdHandle<Self> {
		FreeIdHandle(DummyHandle { from, col })
	}
}

/// Id management view.
struct IdManager;

impl IdManager {
	pub(crate) fn get_handle(&self, col: ColId, lock_until_commit: bool) -> FreeIdHandle<Handle> {
		unimplemented!()
	}
}
