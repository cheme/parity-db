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

// On disk data layout for value tables.
//
// Entry 0 (metadata)
// [LAST_REMOVED: 8][FILLED: 8]
// LAST_REMOVED - 64-bit index of removed entries linked list head
// FILLED - highest index filled with live data
//
// Complete entry:
// [SIZE: 2][REFS: 4][KEY: 26][VALUE: SIZE - 30]
// SIZE: 16-bit value size. Sizes up to 0xfffd are allowed.
// This includes size of REFS and KEY
// REF: 31-bit reference counter, first bit reserved to flag an applied compression.
// KEY: lower 26 bytes of the key.
// VALUE: SIZE-30  payload bytes.
//
// Partial entry (first part):
// [MULTIPART: 2][NEXT: 8][REFS: 4][KEY: 26][VALUE]
// MULTIPART - Split entry marker. 0xfffe.
// NEXT - 64-bit index of the entry that holds the next part.
// take all available space in this entry.
// REF: 31-bit reference counter, first bit reserved to flag an applied compression.
// KEY: lower 26 bytes of the key.
// VALUE: The rest of the entry is filled with payload bytes.
//
// Partial entry (continuation):
// [MULTIPART: 2][NEXT: 8][VALUE]
// MULTIPART - Split entry marker. 0xfffe.
// NEXT - 64-bit index of the entry that holds the next part.
// VALUE: The rest of the entry is filled with payload bytes.
//
// Partial entry (last part):
// [SIZE: 2][VALUE: SIZE]
// SIZE: 16-bit size of the remaining payload.
// VALUE: SIZE payload bytes.
//
// Deleted entry
// [TOMBSTONE: 2][NEXT: 8]
// TOMBSTONE - Deleted entry marker. 0xffff
// NEXT - 64-bit index of the next deleted entry.


use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::io::Read;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use crate::{
	Key,
	error::Result,
	column::ColId,
	log::{LogQuery, LogReader, LogWriter, LogOverlays},
	display::hex,
	options::ColumnOptions as Options,
};
use parking_lot::RwLock;

pub const SIZE_TIERS: usize = 16;
pub const SIZE_TIERS_BITS: u8 = 4;
const MAX_ENTRY_SIZE: usize = 0xfffd;
const REFS_SIZE: usize = 4;
const SIZE_SIZE: usize = 2;
const PARTIAL_SIZE: usize = 26;
const INDEX_SIZE: usize = 8;

const TOMBSTONE: &[u8] = &[0xff, 0xff];
const MULTIPART: &[u8] = &[0xff, 0xfe];
const COMPRESSED_MASK: u32 = 0xa0_00_00_00;
// When a rc reach locked ref, it is locked in db.
const LOCKED_REF: u32 = 0x7fff_ffff;

pub type Value = Vec<u8>;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct TableId(u16);

impl TableId {
	pub fn new(col: ColId, size_tier: u8) -> TableId {
		TableId(((col as u16) << 8) | size_tier as u16)
	}

	pub fn from_u16(id: u16) -> TableId {
		TableId(id)
	}

	pub fn col(&self) -> ColId {
		(self.0 >> 8) as ColId
	}

	pub fn size_tier(&self) -> u8 {
		(self.0 & 0xff) as u8
	}

	pub fn file_name(&self) -> String {
		format!("table_{:02}_{}", self.col(), self.size_tier())
	}

	pub fn as_u16(&self) -> u16 {
		self.0
	}
}

impl std::fmt::Display for TableId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "table {:02}_{}", self.col(), self.size_tier())
	}
}

pub struct ValueTable {
	pub id: TableId,
	pub entry_size: u16,
	file: std::fs::File,
	capacity: AtomicU64,
	filled: AtomicU64,
	last_removed: AtomicU64,
	dirty_header: AtomicBool,
	dirty: AtomicBool,
	multipart: bool,
	attach_key: bool,
	no_indexing: bool,
}

#[cfg(target_os = "macos")]
fn disable_read_ahead(file: &std::fs::File) -> Result<()> {
	use std::os::unix::io::AsRawFd;
	if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_RDAHEAD, 0) } != 0 {
		Err(std::io::Error::last_os_error())?
	} else {
		Ok(())
	}
}

#[cfg(not(target_os = "macos"))]
fn disable_read_ahead(_file: &std::fs::File) -> Result<()> {
	Ok(())
}

#[derive(Default, Clone, Copy)]
struct Header([u8; 16]);

impl Header {
	fn last_removed(&self) -> u64 {
		u64::from_le_bytes(self.0[0..INDEX_SIZE].try_into().unwrap())
	}
	fn set_last_removed(&mut self, last_removed: u64) {
		self.0[0..INDEX_SIZE].copy_from_slice(&last_removed.to_le_bytes());
	}
	fn filled(&self) -> u64 {
		u64::from_le_bytes(self.0[INDEX_SIZE..INDEX_SIZE * 2].try_into().unwrap())
	}
	fn set_filled(&mut self, filled: u64) {
		self.0[INDEX_SIZE..INDEX_SIZE * 2].copy_from_slice(&filled.to_le_bytes());
	}
}

struct Entry<B: AsRef<[u8]> + AsMut<[u8]>>(usize, B);
type FullEntry = Entry<[u8; MAX_ENTRY_SIZE]>;
type PartialEntry = Entry<[u8; 10]>;
type PartialKeyEntry = Entry<[u8; 40]>;

impl<B: AsRef<[u8]> + AsMut<[u8]>> Entry<B> {
	#[inline(always)]
	fn new() -> Self {
		Entry(0, unsafe { MaybeUninit::uninit().assume_init() })
	}

	fn set_offset(&mut self, offset: usize) {
		self.0 = offset;
	}

	fn offset(&self) -> usize {
		self.0
	}

	fn write_slice(&mut self, buf: &[u8]) {
		let start = self.0;
		self.0 += buf.len();
		self.1.as_mut()[start..self.0].copy_from_slice(buf);
	}
	fn read_slice(&mut self, size: usize) -> &[u8] {
		let start = self.0;
		self.0 += size;
		&self.1.as_ref()[start..self.0]
	}

	fn is_tombstone(&self) -> bool {
		&self.1.as_ref()[0..SIZE_SIZE] == TOMBSTONE
	}
	fn write_tombstone(&mut self) {
		self.write_slice(&TOMBSTONE);
	}

	fn is_multipart(&self) -> bool {
		&self.1.as_ref()[0..SIZE_SIZE] == MULTIPART
	}
	fn write_multipart(&mut self) {
		self.write_slice(&MULTIPART);
	}

	fn read_size(&mut self) -> u16 {
		u16::from_le_bytes(self.read_slice(SIZE_SIZE).try_into().unwrap())
	}
	fn skip_size(&mut self) {
		self.0 += SIZE_SIZE;
	}
	fn write_size(&mut self, size: u16) {
		self.write_slice(&size.to_le_bytes());
	}

	fn read_next(&mut self) -> u64 {
		u64::from_le_bytes(self.read_slice(INDEX_SIZE).try_into().unwrap())
	}
	fn skip_next(&mut self) {
		self.0 += INDEX_SIZE;
	}
	fn write_next(&mut self, next_index: u64) {
		self.write_slice(&next_index.to_le_bytes());
	}

	fn read_rc(&mut self) -> u32 {
		u32::from_le_bytes(self.read_slice(REFS_SIZE).try_into().unwrap())
	}
	fn skip_rc(&mut self) {
		self.0 += REFS_SIZE;
	}
	fn write_rc(&mut self, rc: u32) {
		self.write_slice(&rc.to_le_bytes());
	}

	fn read_partial(&mut self) -> &[u8] {
		self.read_slice(PARTIAL_SIZE)
	}

	fn read_varint(&mut self) -> u64 {
		let (stored_len, enc_len) = crate::varint_decode(&self.1.as_ref()[self.0..]);
		self.0 += enc_len;
		stored_len
	}

	fn remaining_to(&self, end: usize) -> &[u8] {
		&self.1.as_ref()[self.0..end]
	}

	fn read_remaining_to(&mut self, end: usize) -> &[u8] {
		let start = self.0;
		self.0 = end;
		&self.1.as_ref()[start..self.0]
	}
}

impl ValueTable {
	pub fn open(path: &std::path::Path, id: TableId, entry_size: Option<u16>, options: &Options) -> Result<ValueTable> {
		let (multipart, entry_size) = match entry_size {
			Some(s) => (false, s),
			None => (true, 4096),
		};
		assert!(entry_size >= 64);
		assert!(entry_size <= MAX_ENTRY_SIZE as u16);
		// TODO: posix_fadvise
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let mut file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(path.as_path())?;
		disable_read_ahead(&file)?;
		let mut file_len = file.metadata()?.len();
		if file_len == 0 {
			// Prealocate a single entry that contains metadata
			file.set_len(entry_size as u64)?;
			file_len = entry_size as u64;
		}

		let capacity = file_len / entry_size as u64;
		let mut header = Header::default();
		file.read_exact(&mut header.0)?;
		let last_removed = header.last_removed();
		let mut filled = header.filled();
		if filled == 0 {
			filled = 1;
		}
		log::debug!(target: "parity-db", "Opened value table {} with {} entries", id, filled);
		Ok(ValueTable {
			id,
			entry_size,
			file,
			capacity: AtomicU64::new(capacity),
			filled: AtomicU64::new(filled),
			last_removed: AtomicU64::new(last_removed),
			dirty_header: AtomicBool::new(false),
			dirty: AtomicBool::new(false),
			multipart,
			attach_key: options.attach_key,
			no_indexing: options.no_indexing,
		})
	}

	pub fn value_size(&self) -> u16 {
		if self.attach_key || self.no_indexing {
			// key size in entry size or no key
			self.entry_size - SIZE_SIZE as u16 - REFS_SIZE as u16
		} else {
			self.entry_size - SIZE_SIZE as u16 - REFS_SIZE as u16 - PARTIAL_SIZE as u16
		}
	}

	#[cfg(unix)]
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		Ok(self.file.read_exact_at(buf, offset)?)
	}

	#[cfg(unix)]
	fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		self.dirty.store(true, Ordering::Relaxed);
		Ok(self.file.write_all_at(buf, offset)?)
	}

	#[cfg(windows)]
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.file.seek_read(buf, offset)?;
		Ok(())
	}

	#[cfg(windows)]
	fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.dirty.store(true, Ordering::Relaxed);
		self.file.seek_write(buf, offset)?;
		Ok(())
	}

	fn grow(&self) -> Result<()> {
		let mut capacity = self.capacity.load(Ordering::Relaxed);
		capacity += (256 * 1024) / self.entry_size as u64;
		self.capacity.store(capacity, Ordering::Relaxed);
		self.file.set_len(capacity * self.entry_size as u64)?;
		Ok(())
	}

	// Return if there was content, and if it was compressed.
	pub fn for_parts<Q: LogQuery, F: FnMut(&[u8]) -> bool>(
		&self,
		key: &Key,
		mut index: u64,
		log: &Q,
		mut f: F,
	) -> Result<(bool, bool)> {
		let mut buf = FullEntry::new();

		let mut part = 0;
		let mut compressed = false;
		let entry_size = self.entry_size as usize;
		let mut queried_key_offset = 0;
		let queried_key_len = key.table_slice().len();
		loop {
			let buf = if log.value(self.id, index, &mut buf.1) {
				&mut buf
			} else {
				log::trace!(
					target: "parity-db",
					"{}: Query slot {}",
					self.id,
					index,
				);
				self.read_at(&mut buf.1[0..entry_size], index * self.entry_size as u64)?;
				&mut buf
			};

			buf.set_offset(0);
			let size = buf.read_size();

			if buf.is_tombstone() {
				return Ok((false, false));
			}

			let (content_offset, end_entry, next) = if buf.is_multipart() {
				let next = buf.read_next();
				(buf.offset(), entry_size, next)
			} else {
				(buf.offset(), buf.offset() + size as usize, 0)
			};

			buf.set_offset(content_offset);

			if part == 0 || (self.attach_key && queried_key_offset != queried_key_len) {
				if queried_key_offset == 0 {
					let rc = buf.read_rc();
					compressed = rc & COMPRESSED_MASK > 0;
				}
				let mut key_range = 0..0;
				let matched = if self.attach_key {
					let (matched_len, upper) = if queried_key_offset == 0 {
						// read len
						let stored_len = buf.read_varint();
						let upper = buf.offset() + stored_len as usize;
						if stored_len != queried_key_len as u64 {
							(false, upper)
						} else {
							(true, upper)
						}
					} else {
						let remaining = queried_key_len - queried_key_offset as usize;
						(true, content_offset + remaining)
					};
					key_range.start = buf.offset();
					if upper > entry_size {
						// key over multiple part.
						let upper_key = queried_key_offset + (entry_size - buf.offset()) as usize;
						if !matched_len || &key.table_slice()[queried_key_offset..upper_key] != buf.read_remaining_to(entry_size) {
							key_range.end = buf.offset();
							false
						} else {
							queried_key_offset = upper_key;
							part += 1;
							if next == 0 {
								break;
							}
							index = next;
							continue;
						}
					} else if !matched_len || &key.table_slice()[queried_key_offset..] != buf.read_remaining_to(upper) {
						key_range.end = buf.offset();
						false
					} else {
						queried_key_offset = queried_key_len;
						true
					}
				} else if self.no_indexing {
					true
				} else if key.table_slice() != buf.read_partial() {
					key_range.end = buf.offset();
					false
				} else {
					true
				};
				if !matched {
					log::debug!(
						target: "parity-db",
						"{}: Key mismatch at {}. Expected {}, got {}",
						self.id,
						index,
						hex(key.table_slice()),
						hex(&buf.1[key_range]),
					);
					return Ok((false, false));
				}
				if !f(buf.remaining_to(end_entry)) {
					break;
				};
			} else {
				if !f(buf.remaining_to(end_entry)) {
					break;
				};
			}
			if next == 0 {
				break;
			}
			part += 1;
			index = next;
		}
		Ok((true, compressed))
	}

	pub fn get(&self, key: &Key, index: u64, log: &impl LogQuery) -> Result<Option<(Value, bool)>> {
		let mut result = Vec::new();
		let (success, compressed) = self.for_parts(key, index, log, |buf| {
			result.extend_from_slice(buf);
			true
		})?;
		if success {
			return Ok(Some((result, compressed)));
		}
		Ok(None)
	}

	pub fn size(&self, key: &Key, index: u64, log: &impl LogQuery) -> Result<Option<(u32, bool)>> {
		let mut result = 0;
		let (success, compressed) = self.for_parts(key, index, log, |buf| {
			result += buf.len() as u32;
			true
		})?;
		if success {
			return Ok(Some((result, compressed)));
		}
		Ok(None)
	}

	pub fn has_key_at(&self, index: u64, key: &Key, log: &LogWriter) -> Result<bool> {
		if self.attach_key {
			let (success, _compressed) = self.for_parts(key, index, log, |_| false)?;
			Ok(success)
		} else {
			Ok(match self.partial_key_at(index, log)? {
				Some(existing_key) => &existing_key[..] == key.table_slice(),
				None => false,
			})
		}
	}

	pub fn partial_key_at<Q: LogQuery>(&self, index: u64, log: &Q) -> Result<Option<[u8; PARTIAL_SIZE]>> {
		let mut buf = PartialKeyEntry::new();
		let mut result = [0u8; PARTIAL_SIZE];
		let buf = if log.value(self.id, index, &mut buf.1) {
			&mut buf
		} else {
			self.read_at(&mut buf.1[..], index * self.entry_size as u64)?;
			&mut buf
		};
		if buf.is_tombstone() {
			return Ok(None);
		}
		if buf.is_multipart() {
			buf.skip_size();
			buf.skip_next();
			buf.skip_rc();
			result[..].copy_from_slice(buf.read_partial());
		} else {
			buf.skip_size();
			buf.skip_rc();
			result[..].copy_from_slice(buf.read_partial());
		}
		Ok(Some(result))
	}

	pub fn read_next_free(&self, index: u64, log: &LogWriter) -> Result<u64> {
		let mut buf = PartialEntry::new();
		if log.value(self.id, index, &mut buf.1) {
			buf.skip_size();
			let next = buf.read_next();
			return Ok(next);
		}
		self.read_at(&mut buf.1, index * self.entry_size as u64)?;
		buf.skip_size();
		let next = buf.read_next();
		return Ok(next);
	}

	// TODO remove in favor or std read_next_free
	pub fn read_next_free_no_indexing(&self, index: u64, log: &RwLock<LogOverlays>) -> Result<u64> {
		let mut buf = PartialEntry::new();
		self.read_at(&mut buf.1, index * self.entry_size as u64)?;
		buf.skip_size();
		let next = buf.read_next();
		return Ok(next);
	}

	pub fn read_next_part(&self, index: u64, log: &LogWriter) -> Result<Option<u64>> {
		let mut buf = PartialEntry::new();
		if log.value(self.id, index, &mut buf.1) {
			if buf.is_multipart() {
				buf.skip_size();
				let next = buf.read_next();
				return Ok(Some(next));
			}
			return Ok(None);
		}
		self.read_at(&mut buf.1, index * self.entry_size as u64)?;
		if buf.is_multipart() {
			buf.skip_size();
			let next = buf.read_next();
			return Ok(Some(next));
		}
		return Ok(None);
	}

	pub fn next_free(&self, log: &mut LogWriter) -> Result<u64> {
		let filled = self.filled.load(Ordering::Relaxed);
		let last_removed = self.last_removed.load(Ordering::Relaxed);
		let index = if last_removed != 0 {
			let next_removed = self.read_next_free(last_removed, log)?;
			log::trace!(
				target: "parity-db",
				"{}: Inserting into removed slot {}",
				self.id,
				last_removed,
			);
			self.last_removed.store(next_removed, Ordering::Relaxed);
			last_removed
		} else {
			log::trace!(
				target: "parity-db",
				"{}: Inserting into new slot {}",
				self.id,
				filled,
			);
			self.filled.store(filled + 1, Ordering::Relaxed);
			filled
		};
		self.dirty_header.store(true, Ordering::Relaxed);
		Ok(index)
	}

	fn overwrite_chain(&self, key: &Key, value: &[u8], log: &mut LogWriter, at: Option<u64>, compressed: bool) -> Result<u64> {
		let encoded_key_len = key.encoded_len();
		let mut key_remaining = encoded_key_len;
		let mut remainder = value.len() + REFS_SIZE + if self.attach_key {
			key_remaining
		} else if self.no_indexing {
			0
		} else {
			PARTIAL_SIZE
		}; // Prefix with key and ref counter
		let mut offset = 0;
		let mut start = 0;
		// TODO here assert that when with_key value.len() + key.len() <= self.value size
		assert!(self.multipart || value.len() <= self.value_size() as usize);
		let (mut index, mut follow) = match at {
			Some(index) => (index, true),
			None => (self.next_free(log)?, false)
		};
		loop {
			if start == 0 {
				start = index;
			}

			let mut next_index = 0;
			if follow {
				// check existing link
				match self.read_next_part(index, log)? {
					Some(next) => {
						next_index = next;
					}
					None => {
						follow = false;
					}
				}
			}
			log::trace!(
				target: "parity-db",
				"{}: Writing slot {}: {}",
				self.id,
				index,
				hex(key),
			);
			let mut buf = FullEntry::new();
			let free_space = self.entry_size as usize - SIZE_SIZE;
			let (target_offset, value_len) = if remainder > free_space {
				if !follow {
					next_index = self.next_free(log)?
				}
				buf.write_multipart();
				buf.write_next(next_index);
				(10, free_space - INDEX_SIZE)
			} else {
				buf.write_size(remainder as u16);
				(SIZE_SIZE, remainder)
			};
			let init_offset = buf.offset();
			if offset == 0 && key_remaining > 0 {
				if key_remaining == encoded_key_len {
					// first rc.
					let rc = if compressed {
						1u32 | COMPRESSED_MASK
					} else {
						1u32
					};
					buf.write_rc(rc);
					if self.attach_key {
						let mut buf_len = [0u8; 10];
						let encoded = crate::varint_encode(key.len() as u64, &mut buf_len);
						buf.write_slice(encoded);
						key_remaining -= encoded.len();
					} else if self.no_indexing {
						key_remaining = 0;
					} else {
						buf.write_slice(key.table_slice());
						key_remaining = 0;
					}
				}
				if self.attach_key {
					let mut upper = buf.offset() + key_remaining;
					let key_start = key.table_slice().len() - key_remaining;
					if (self.entry_size as usize) < upper {
						upper = self.entry_size as usize;
						let to_write = upper - buf.offset();
						buf.write_slice(&key.table_slice()[key_start..key_start + to_write]);
						key_remaining -= to_write;
					} else {
						buf.write_slice(&key.table_slice()[key_start..]);
						key_remaining = 0;
					};
				}
			}
			if key_remaining == 0 {
				let written = buf.offset() - init_offset;
				buf.write_slice(&value[offset..offset + value_len - written]);
				offset += value_len - written;
			}
			log.insert_value(self.id, index, buf.1[0..target_offset + value_len].to_vec());
			remainder -= value_len;
			index = next_index;
			if remainder == 0 {
				if index != 0 {
					// End of new entry. Clear the remaining tail and exit
					self.clear_chain(index, log)?;
				}
				break;
			}
		}

		Ok(start)
	}

	fn clear_chain(&self, mut index: u64, log: &mut LogWriter) -> Result<()> {
		loop {
			match self.read_next_part(index, log)? {
				Some(next) => {
					self.clear_slot(index, log)?;
					index = next;
				}
				None => {
					self.clear_slot(index, log)?;
					return Ok(());
				}
			}
		}
	}

	fn clear_slot(&self, index: u64, log: &mut LogWriter) -> Result<()> {
		let last_removed = self.last_removed.load(Ordering::Relaxed);
		log::trace!(
			target: "parity-db",
			"{}: Freeing slot {}",
			self.id,
			index,
		);
		let mut buf = [0u8; 10];
		&buf[0..SIZE_SIZE].copy_from_slice(TOMBSTONE);
		&buf[SIZE_SIZE..SIZE_SIZE + INDEX_SIZE].copy_from_slice(&last_removed.to_le_bytes());

		log.insert_value(self.id, index, buf.to_vec());
		self.last_removed.store(index, Ordering::Relaxed);
		self.dirty_header.store(true, Ordering::Relaxed);
		Ok(())
	}

	pub fn write_insert_plan(&self, key: &Key, value: &[u8], log: &mut LogWriter, compressed: bool) -> Result<u64> {
		self.overwrite_chain(key, value, log, None, compressed)
	}

	pub fn write_replace_plan(&self, index: u64, key: &Key, value: &[u8], log: &mut LogWriter, compressed: bool) -> Result<()> {
		self.overwrite_chain(key, value, log, Some(index), compressed)?;
		Ok(())
	}

	pub fn write_remove_plan(&self, index: u64, log: &mut LogWriter) -> Result<()> {
		if self.multipart {
			self.clear_chain(index, log)?;
		} else {
			self.clear_slot(index, log)?;
		}
		Ok(())
	}

	pub fn write_inc_ref(&self, index: u64, log: &mut LogWriter, compressed: bool) -> Result<()> {
		self.change_ref(index, 1, log, Some(compressed))?;
		Ok(())
	}

	pub fn write_dec_ref(&self, index: u64, log: &mut LogWriter) -> Result<bool> {
		if self.change_ref(index, -1, log, None)? {
			return Ok(true);
		}
		self.write_remove_plan(index, log)?;
		Ok(false)
	}

	pub fn change_ref(&self, index: u64, delta: i32, log: &mut LogWriter, compressed: Option<bool>) -> Result<bool> {
		let mut buf = FullEntry::new();
		let buf = if log.value(self.id, index, &mut buf.1) {
			&mut buf
		} else {
			self.read_at(&mut buf.1[0..self.entry_size as usize], index * self.entry_size as u64)?;
			&mut buf
		};

		let size = buf.read_size();
		if buf.is_tombstone() {
			return Ok(false);
		}

		let size = if buf.is_multipart() {
			buf.skip_next();
			self.entry_size as usize
		} else {
			buf.offset() + size as usize
		};

		let rc_offset = buf.offset();

		let mut counter = buf.read_rc();
		debug_assert!(compressed.map(|compressed| if counter & COMPRESSED_MASK > 0 {
				compressed == true
			} else {
				compressed == false
			}).unwrap_or(true)
		);

		let compressed = counter & COMPRESSED_MASK > 0;
		counter = counter & !COMPRESSED_MASK;
		if delta > 0 {
			if counter >= LOCKED_REF - delta as u32 {
				counter = LOCKED_REF
			} else {
				counter = counter + delta as u32;
			}
		} else {
			if counter != LOCKED_REF {
				counter = counter.saturating_sub(-delta as u32);
				if counter == 0 {
					return Ok(false);
				}
			}
		}
		counter = if compressed {
			counter | COMPRESSED_MASK
		} else {
			counter
		};

		buf.set_offset(rc_offset);
		buf.write_rc(counter);
		// TODO: optimize actual buf size
		log.insert_value(self.id, index, buf.1[0..size].to_vec());
		return Ok(true);
	}

	pub fn write_inner_free_list_remove(&self, index: u64, next: u64, log: &mut LogWriter) -> Result<()> {
		let prev = self.read_next_free(index, log)?;
		let mut buf = PartialEntry::new();
		buf.write_tombstone();
		buf.write_next(prev);
		log.insert_value(self.id, next, buf.1.as_ref().to_vec());
		Ok(())
	}

	pub fn enact_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		while index >= self.capacity.load(Ordering::Relaxed) {
			self.grow()?;
		}
		if index == 0 {
			let mut header = Header::default();
			log.read(&mut header.0)?;
			self.write_at(&header.0, 0)?;
			return Ok(());
		}

		let mut buf = FullEntry::new();
		log.read(&mut buf.1[0..SIZE_SIZE])?;
		if buf.is_tombstone() {
			log.read(&mut buf.1[SIZE_SIZE..SIZE_SIZE + INDEX_SIZE])?;
			self.write_at(&buf.1[0..SIZE_SIZE + INDEX_SIZE], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted tombstone in slot {}", self.id, index);
		} else if buf.is_multipart() {
				let entry_size = self.entry_size as usize;
				log.read(&mut buf.1[SIZE_SIZE..entry_size])?;
				self.write_at(&buf.1[0..entry_size], index * (entry_size as u64))?;
				log::trace!(target: "parity-db", "{}: Enacted multipart in slot {}", self.id, index);
		} else {
			let len = buf.read_size();
			log.read(&mut buf.1[SIZE_SIZE..SIZE_SIZE + len as usize])?;
			self.write_at(&buf.1[0..(SIZE_SIZE + len as usize)], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted {}: {}, {} bytes", self.id, index, hex(&buf.1[6..32]), len);
		}
		Ok(())
	}

	pub fn validate_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		if index == 0 {
			let mut header = Header::default();
			log.read(&mut header.0)?;
			// TODO: sanity check last_removed and filled
			return Ok(());
		}
		let mut buf = FullEntry::new();
		log.read(&mut buf.1[0..SIZE_SIZE])?;
		if buf.is_tombstone() {
			log.read(&mut buf.1[SIZE_SIZE..SIZE_SIZE + INDEX_SIZE])?;
			log::trace!(target: "parity-db", "{}: Validated tombstone in slot {}", self.id, index);
		}
		else if buf.is_multipart() {
			let entry_size = self.entry_size as usize;
			log.read(&mut buf.1[SIZE_SIZE..entry_size])?;
			log::trace!(target: "parity-db", "{}: Validated multipart in slot {}", self.id, index);
		} else {
			// TODO: check len
			let len = buf.read_size();
			log.read(&mut buf.1[SIZE_SIZE..SIZE_SIZE + len as usize])?;
			log::trace!(target: "parity-db", "{}: Validated {}: {}, {} bytes", self.id, index, hex(&buf.1[SIZE_SIZE..32]), len);
		}
		Ok(())
	}

	pub fn refresh_metadata(&self) -> Result<()> {
		let mut header = Header::default();
		self.read_at(&mut header.0, 0)?;
		let last_removed = header.last_removed();
		let mut filled = header.filled();
		if filled == 0 {
			filled = 1;
		}
		self.last_removed.store(last_removed, Ordering::Relaxed);
		self.filled.store(filled, Ordering::Relaxed);
		Ok(())
	}

	pub fn complete_plan(&self, log: &mut LogWriter) -> Result<()> {
		if let Ok(true) = self.dirty_header.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed) {
			// last_removed or filled pointers were modified. Add them to the log
			let mut buf = Header::default();
			let last_removed = self.last_removed.load(Ordering::Relaxed);
			let filled = self.filled.load(Ordering::Relaxed);
			buf.set_last_removed(last_removed);
			buf.set_filled(filled);
			log.insert_value(self.id, 0, buf.0.to_vec());
		}
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		if let Ok(true) = self.dirty.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed) {
			self.file.sync_data()?;
		}
		Ok(())
	}

	pub fn current_free_table_state(&self) -> (u64, u64) {
		(
			self.filled.load(std::sync::atomic::Ordering::Relaxed),
			self.last_removed.load(std::sync::atomic::Ordering::Relaxed),
		)
	}
}

#[cfg(test)]
mod test {
	const ENTRY_SIZE: u16 = 64;
	use super::{ValueTable, TableId, Key, Value};
	use crate::{log::{Log, LogWriter, LogAction}, options::{Options, ColumnOptions}};

	struct TempDir(std::path::PathBuf);

	impl TempDir {
		fn new(name: &'static str) -> TempDir {
			env_logger::try_init().ok();
			let mut path = std::env::temp_dir();
			path.push("parity-db-test");
			path.push("value-table");
			path.push(name);

			if path.exists() {
				std::fs::remove_dir_all(&path).unwrap();
			}
			std::fs::create_dir_all(&path).unwrap();
			TempDir(path)
		}

		fn table(&self, size: Option<u16>, options: &ColumnOptions) -> ValueTable {
			let id = TableId::new(0, 0);
			ValueTable::open(&self.0, id, size, options).unwrap()
		}

		fn log(&self) -> Log {
			let options = Options::with_columns(&self.0, 1);
			Log::open(&options).unwrap()
		}
	}

	impl Drop for TempDir {
		fn drop(&mut self) {
			if self.0.exists() {
				std::fs::remove_dir_all(&self.0).unwrap();
			}
		}
	}

	fn write_ops<F: FnOnce(&mut LogWriter)>(table: &ValueTable, log: &Log, f: F) {
		let mut writer = log.begin_record();
		f(&mut writer);
		let bytes_written = log.end_record(writer.drain()).unwrap();
		// Cycle through 2 log files
		let _ = log.read_next(false);
		log.flush_one(0, || Ok(())).unwrap();
		let _ = log.read_next(false);
		log.flush_one(0, || Ok(())).unwrap();
		let mut reader = log.read_next(false).unwrap().unwrap();
		loop {
			match reader.next().unwrap() {
				LogAction::BeginRecord(_) | LogAction::InsertIndex { .. } | LogAction::DropTable { .. } => {
					panic!("Unexpected log entry");
				},
				LogAction::EndRecord => {
					let bytes_read = reader.read_bytes();
					assert_eq!(bytes_written, bytes_read);
					break;
				},
				LogAction::InsertValue(insertion) => {
					table.enact_plan(insertion.index, &mut reader).unwrap();
				},
			}
		}
	}

	fn key(k: u32, options: &ColumnOptions) -> Key {
		if options.no_indexing {
			crate::column::hash_utils(&(k as u64).to_be_bytes()[..], options.attach_key, false, true, &None)
		} else {
			crate::column::hash_utils(&k.to_le_bytes()[..], options.attach_key, false, false, &None)
		}
	}

	fn value(size: usize) -> Value {
		use rand::RngCore;
		let mut result = Vec::with_capacity(size);
		result.resize(size, 0);
		rand::thread_rng().fill_bytes(&mut result);
		result
	}

	fn attached_key() -> ColumnOptions {
		let mut result = ColumnOptions::default();
		result.attach_key = true;
		result
	}

	fn no_indexing() -> ColumnOptions {
		let mut result = ColumnOptions::default();
		result.no_indexing = true;
		result
	}

	#[test]
	fn insert_simple() {
		insert_simple_inner(&Default::default());
		insert_simple_inner(&attached_key());
		//insert_simple_inner(&no_indexing());
	}
	fn insert_simple_inner(options: &ColumnOptions) {
		let dir = TempDir::new("insert_simple");
		let table = dir.table(Some(ENTRY_SIZE), &options);
		let log = dir.log();

		let key = key(1, options);
		let val = value(19);
		let compressed = true;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key, &val, writer, compressed).unwrap();
			assert_eq!(table.get(&key, 1, writer).unwrap(), Some((val.clone(), compressed)));
		});

		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val, compressed)));
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 2);
	}

	#[test]
	#[should_panic(expected = "assertion failed: entry_size <= MAX_ENTRY_SIZE as u16")]
	fn oversized_into_fixed_panics() {
		let dir = TempDir::new("oversized_into_fixed_panics");
		let _table = dir.table(Some(65534), &Default::default());
	}

	#[test]
	fn remove_simple() {
		remove_simple_inner(&Default::default());
		remove_simple_inner(&attached_key());
		//remove_simple_inner(&no_indexing());
	}
	fn remove_simple_inner(options: &ColumnOptions) {
		let dir = TempDir::new("remove_simple");
		let table = dir.table(Some(ENTRY_SIZE), options);
		let log = dir.log();

		let key1 = key(1, options);
		let key2 = key(2, options);
		let val1 = value(11);
		let val2 = value(21);
		let compressed = false;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		write_ops(&table, &log, |writer| {
			table.write_remove_plan(1, writer).unwrap();
		});

		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), None);
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 1);

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
	}

	#[test]
	fn replace_simple() {
		replace_simple_inner(&Default::default());
		replace_simple_inner(&attached_key());
		//replace_simple_inner(&no_indexing());
	}
	fn replace_simple_inner(options: &ColumnOptions) {
		let dir = TempDir::new("replace_simple");
		let table = dir.table(Some(ENTRY_SIZE), options);
		let log = dir.log();

		let key1 = key(1, options);
		let key2 = key(2, options);
		let key3 = key(2, options);
		let val1 = value(11);
		let val2 = value(21);
		let val3 = value(31);
		let compressed = true;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		write_ops(&table, &log, |writer| {
			table.write_replace_plan(1, &key3, &val3, writer, false).unwrap();
		});

		assert_eq!(table.get(&key3, 1, log.overlays()).unwrap(), Some((val3, false)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
	}

	#[test]
	fn replace_multipart_shorter() {
		replace_multipart_shorter_inner(&Default::default());
		replace_multipart_shorter_inner(&attached_key());
		//replace_multipart_shorter_inner(&no_indexing());
	}
	fn replace_multipart_shorter_inner(options: &ColumnOptions) {
		let dir = TempDir::new("replace_multipart_shorter");
		let table = dir.table(None, options);
		let log = dir.log();

		let key1 = key(1, options);
		let key2 = key(2, options);
		let val1 = value(20000);
		let val2 = value(30);
		let val1s = value(5000);
		let compressed = false;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 7);

		write_ops(&table, &log, |writer| {
			table.write_replace_plan(1, &key1, &val1s, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1s, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 5);
		write_ops(&table, &log, |writer| {
			assert_eq!(table.read_next_free(5, writer).unwrap(), 4);
			assert_eq!(table.read_next_free(4, writer).unwrap(), 3);
			assert_eq!(table.read_next_free(3, writer).unwrap(), 0);
		});
	}

	#[test]
	fn replace_multipart_longer() {
		replace_multipart_longer_inner(&Default::default());
		replace_multipart_longer_inner(&attached_key());
		//replace_multipart_longer_inner(&no_indexing());
	}
	fn replace_multipart_longer_inner(options: &ColumnOptions) {
		let dir = TempDir::new("replace_multipart_longer");
		let table = dir.table(None, options);
		let log = dir.log();

		let key1 = key(1, options);
		let key2 = key(2, options);
		let val1 = value(5000);
		let val2 = value(30);
		let val1l = value(20000);
		let compressed = false;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 4);

		write_ops(&table, &log, |writer| {
			table.write_replace_plan(1, &key1, &val1l, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1l, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 7);
	}

	#[test]
	fn long_key_multipart() {
		let options = attached_key();
		let options = &options;
		let dir = TempDir::new("replace_multipart_shorter");
		let table = dir.table(None, options);
		let log = dir.log();

		let key1 = value(20000);
		let key1 = crate::column::hash_utils(&key1[..], options.attach_key, false, false, &None);
		let key2 = value(16000);
		let key2 = crate::column::hash_utils(&key2[..], options.attach_key, false, false, &None);
		let val1 = value(30);
		let val2 = value(5000);
		let compressed = false;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1, compressed)));
		assert_eq!(table.get(&key2, 6, log.overlays()).unwrap(), Some((val2.clone(), compressed)));

		write_ops(&table, &log, |writer| {
			table.write_replace_plan(1, &key1, &val2, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val2, compressed)));
	}

	#[test]
	fn ref_counting() {
		ref_counting_inner(&Default::default());
		ref_counting_inner(&attached_key());
		//ref_counting_inner(&no_indexing());
	}
	fn ref_counting_inner(options: &ColumnOptions) {
		for compressed in [false, true] {
			let dir = TempDir::new("ref_counting");
			let table = dir.table(None, options);
			let log = dir.log();

			let key = key(1, options);
			let val = value(5000);
			write_ops(&table, &log, |writer| {
				table.write_insert_plan(&key, &val, writer, compressed).unwrap();
				table.write_inc_ref(1, writer, compressed).unwrap();
			});
			assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val.clone(), compressed)));
			write_ops(&table, &log, |writer| {
				table.write_dec_ref(1, writer).unwrap();
			});
			assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val, compressed)));
			write_ops(&table, &log, |writer| {
				table.write_dec_ref(1, writer).unwrap();
			});
			assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), None);
		}
	}

	#[test]
	fn ref_underflow() {
		let options = &ColumnOptions::default();
		let dir = TempDir::new("ref_underflow");
		let table = dir.table(None, &options);
		let log = dir.log();

		let key = key(1, &options);
		let val = value(10);

		let compressed = false;
		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key, &val, writer, compressed).unwrap();
			table.write_inc_ref(1, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val.clone(), compressed)));
		write_ops(&table, &log, |writer| {
			table.write_dec_ref(1, writer).unwrap();
			table.write_dec_ref(1, writer).unwrap();
			table.write_dec_ref(1, writer).unwrap();
		});
		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), None);
	}
}
