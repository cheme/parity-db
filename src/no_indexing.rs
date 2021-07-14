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
struct FreeIdHandle {
	col: ColId,
	read_only: bool,
}
	
/// Handle to the index management for querying new ids.
/// On drop it releases fetched ids.
impl FreeIdHandle {
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
	fn commit(self) {
		if self.read_only {
			return;
		}
		unimplemented!()
	}

	/// Release fetched ids, put the handle in a consumed state.
	/// Should never panic (called by drop).
	fn drop_handle_inner(&mut self) {
		if self.read_only {
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

/// Id management view.
struct IdManager;

impl IdManager {
	pub(crate) fn get_handle(&self, col: ColId, single_write: bool) -> FreeIdHandle {
		unimplemented!()
	}

	pub(crate) fn get_read_only_handle(&self, col: ColId, no_write: bool) -> FreeIdHandle {
		unimplemented!()
	}
}
