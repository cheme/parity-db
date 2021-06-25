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

mod db;
mod error;
mod index;
mod table;
mod column;
mod log;
mod display;
mod options;
mod stats;
mod compress;

pub use db::Db;
pub use error::{Error, Result};
pub use options::{ColumnOptions, Options};

const KEY_LEN: usize = 32;

#[derive(PartialEq, Eq, Clone)]
pub enum Key { // TODO remove pub??
	Hash([u8; KEY_LEN]),
	// TODO if withkeyref, try bench against &[u8; KEYLEN] (HashRef)
	// but issue with [] passing is probably fixed.
	WithKey(u64, Vec<u8>),
	// TODO consider withkeyref(u64, &'a[u8])
}

impl std::hash::Hash for Key {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		match self {
			Key::Hash(hash) => state.write(&hash[..]),
			// warn don't use for persistence (ne_bytes)
			Key::WithKey(hash, _full) => state.write(&hash.to_ne_bytes()[..]),
		}
	}
}

impl AsRef<[u8]> for Key {
	fn as_ref(&self) -> &[u8] {
		match self {
			Key::Hash(hash) => &hash[..],
			Key::WithKey(_hash, full) => &full[..],
		}
	}
}


impl Key {
	pub fn len(&self) -> usize {
		match self {
			Key::Hash(hash) => hash.len(),
			Key::WithKey(_hash, full) => full.len(),
		}
	}

	pub fn table_slice(&self) -> &[u8] {
		match self {
			Key::Hash(hash) => &hash[6..],
			Key::WithKey(_hash, full) => &full[..],
		}
	}

	pub fn index(&self) -> u64 {
		match self {
			Key::Hash(hash) => {
				use std::convert::TryInto;
				u64::from_be_bytes((hash[0..8]).try_into().unwrap())
			},
			Key::WithKey(hash, _full) => *hash,
		}
	}

	pub fn default_hash() -> Self {
		Key::Hash(Default::default())
	}
}
