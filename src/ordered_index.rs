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

//! Index stored as radix tree in tables.


use parking_lot::RwLock;
use std::sync::Arc;
use radix_tree::backends::Backend;
use crate::table::{TableId as ValueTableId, ValueTable, Value};
use crate::error::{Error, Result};
use crate::options::{Options, ColumnOptions};
use crate::column::ColId;
use crate::index::Address;
use crate::Key;
use crate::log::LogOverlays;

// Store test encoded 256 node (really unoptimal).
// and keep root_index as a standard index entry,
// by convention first file and first entry.
struct TestBackendInner {
	root_index: u64,
	tables: [ValueTable; 16],
	log: Arc<RwLock<LogOverlays>>,
}

// TODO make it closer to default encoded 256 sizes for reasonable prefix len.
const INDEX_SIZES: [u16; 15] = [96, 128, 192, 256, 320, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 32768];

#[derive(Clone)]
pub(crate) struct TestBackend(Arc<RwLock<TestBackendInner>>);

impl TestBackend {
	pub(crate) fn open(col: ColId, options: &Options, log: &Arc<RwLock<LogOverlays>>) -> Result<Option<Self>> {
		let path = &options.path;
		let options = ColumnOptions {
			preimage: false,
			uniform: false,
			sizes: INDEX_SIZES,
			ref_counted: false,
			compression: crate::compress::CompressionType::NoCompression,
			compression_treshold: 0,
			attach_key: false,
			ordered_indexed: true, // whatever, this is not used for inner tables.
			no_indexing: true,
		};

		let root_index = 0;
		// TODO check ordered_index option, return None otherwhise.
		let mut result = TestBackendInner {
			root_index,
			log: log.clone(),
			tables: [
				Self::open_table(path, col, 0, Some(INDEX_SIZES[0]), &options)?,
				Self::open_table(path, col, 1, Some(INDEX_SIZES[1]), &options)?,
				Self::open_table(path, col, 2, Some(INDEX_SIZES[2]), &options)?,
				Self::open_table(path, col, 3, Some(INDEX_SIZES[3]), &options)?,
				Self::open_table(path, col, 4, Some(INDEX_SIZES[4]), &options)?,
				Self::open_table(path, col, 5, Some(INDEX_SIZES[5]), &options)?,
				Self::open_table(path, col, 6, Some(INDEX_SIZES[6]), &options)?,
				Self::open_table(path, col, 7, Some(INDEX_SIZES[7]), &options)?,
				Self::open_table(path, col, 8, Some(INDEX_SIZES[8]), &options)?,
				Self::open_table(path, col, 9, Some(INDEX_SIZES[9]), &options)?,
				Self::open_table(path, col, 10, Some(INDEX_SIZES[10]), &options)?,
				Self::open_table(path, col, 11, Some(INDEX_SIZES[11]), &options)?,
				Self::open_table(path, col, 12, Some(INDEX_SIZES[12]), &options)?,
				Self::open_table(path, col, 13, Some(INDEX_SIZES[13]), &options)?,
				Self::open_table(path, col, 14, Some(INDEX_SIZES[14]), &options)?,
				Self::open_table(path, col, 15, None, &options)?,
			]
		};
		if result.get(root_index).is_none() {
			// initialize an empty radix tree (collection is considered empty).
			unimplemented!("TODO write empty root and write u64 index in tables: assert gives 0 ix");
		}

		Ok(Some(TestBackend(Arc::new(RwLock::new(result)))))
	}

	fn open_table(
		path: &std::path::Path,
		col: ColId,
		tier: u8,
		entry_size: Option<u16>,
		options: &ColumnOptions,
	) -> Result<ValueTable> {
		let id = ValueTableId::new(col, tier);
		ValueTable::open_index(path, id, entry_size, options)
	}
}


impl TestBackendInner {
	fn remove(&mut self, index: u64) {

		unimplemented!()
	}
	fn get(&self, index: u64) -> Option<Vec<u8>> {
		let address = Address::from_u64(index);
		let size_tier = address.size_tier() as usize;
		let offset = address.offset();
		let log = self.log.read();
		match self.tables[size_tier].get(&Key::default_hash(), offset, &*self.log) {
			Ok(Some((value, false))) => {
				Some(value)
			},
			_ => {
				None // TODO correct error handling
			},
		}
	}
	fn update(&mut self, at: u64, content: Vec<u8>) -> Option<u64> {
		unimplemented!()
	}
	fn insert(&mut self, content: Vec<u8>) -> u64 {
		unimplemented!()
	}
}

// TODO consider lossless backend.
impl Backend for TestBackend {
	type Index = u64;

	fn get_root(&self) -> Option<(Vec<u8>, u64)> {
		let s = self.0.read();
		s.get(s.root_index).map(|v| (v, s.root_index))
	}

	fn get_node(&self, index: Self::Index) -> Option<Vec<u8>> {
		self.0.read().get(index)
	}

	fn update_node(&self, index: Self::Index, node: Vec<u8>) -> Option<Self::Index> {
		 self.0.write().update(index, node)
	}

	fn remove_node(&self, index: Self::Index) {
		self.0.write().remove(index)
	}

	fn set_new_node(&self, node: Vec<u8>) -> Self::Index {
		self.0.write().insert(node)
	}

	fn set_root(&self, root: Self::Index) {
		self.0.write().root_index = root;
	}
}
