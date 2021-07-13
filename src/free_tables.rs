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
pub(crate) struct FreeTables {
	tables: [ValueTable; 16],
}

// TODO make it closer to default encoded 256 sizes for reasonable prefix len.
const INDEX_SIZES: [u16; 15] = [96, 128, 192, 256, 320, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 32768];

impl FreeTables {
	pub(crate) fn open(col: ColId, options: &Options) -> Result<Option<Self>> {
		let path = &options.path;
		let options = ColumnOptions {
			preimage: false,
			uniform: false,
			sizes: INDEX_SIZES,
			ref_counted: false,
			compression: crate::compress::CompressionType::NoCompression,
			compression_treshold: 0,
			attach_key: false,
			free_tables: options.columns[col as usize].free_tables.clone(),
			no_indexing: true,
			// TODO no_rc and no_compress (but should be generic to all tables).
		};

		// TODO check free_tables_index option, return None otherwhise.
		Ok(Some(FreeTables {
			tables: [
				Self::open_table(path, col, 0, &options)?,
				Self::open_table(path, col, 1, &options)?,
				Self::open_table(path, col, 2, &options)?,
				Self::open_table(path, col, 3, &options)?,
				Self::open_table(path, col, 4, &options)?,
				Self::open_table(path, col, 5, &options)?,
				Self::open_table(path, col, 6, &options)?,
				Self::open_table(path, col, 7, &options)?,
				Self::open_table(path, col, 8, &options)?,
				Self::open_table(path, col, 9, &options)?,
				Self::open_table(path, col, 10, &options)?,
				Self::open_table(path, col, 11, &options)?,
				Self::open_table(path, col, 12, &options)?,
				Self::open_table(path, col, 13, &options)?,
				Self::open_table(path, col, 14, &options)?,
				Self::open_table(path, col, 15, &options)?,
			]
		}))
	}

	fn open_table(
		path: &std::path::Path,
		col: ColId,
		tier: u8,
		options: &ColumnOptions,
	) -> Result<ValueTable> {
		let id = ValueTableId::new(col, tier);
		let entry_size = if let Some(sizes) = options.free_tables.as_ref() {
			sizes.get(tier as usize).cloned()
		} else {
			panic!("TODO raise error");
		};
		ValueTable::open_index(path, id, entry_size, options)
	}
}


impl FreeTables {
	fn remove(&mut self, index: u64) {

		unimplemented!()
	}
	pub(crate) fn get(&self, index: u64, log: &RwLock<LogOverlays>) -> Result<Option<Vec<u8>>> {
		let address = Address::from_u64(index);
		let size_tier = address.size_tier() as usize;
		let offset = address.offset();
		Ok(match self.tables[size_tier].get_free(offset, log)? {
			Some((value, false)) => {
				Some(value)
			},
			_ => {
				None
			},
		})
	}
	fn update(&mut self, at: u64, content: Vec<u8>) -> Option<u64> {
		unimplemented!()
	}
	fn insert(&mut self, content: Vec<u8>) -> u64 {
		unimplemented!()
	}
}
