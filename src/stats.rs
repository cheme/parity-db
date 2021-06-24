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

/// Database statistics.

use std::sync::atomic::{AtomicU64, AtomicU32, AtomicI64, Ordering};
use std::mem::MaybeUninit;
use std::io::{Read, Write, Cursor};
use crate::{error::Result, column::ColId, table::SIZE_TIERS};

// store up to value of size HISTOGRAM_BUCKETS * 2 ^ HISTOGRAM_BUCKET_BITS,
// that is 32ko
const HISTOGRAM_BUCKETS: usize = 1024;
const HISTOGRAM_BUCKET_BITS: u8 = 5;

pub const TOTAL_SIZE: usize = 4 * HISTOGRAM_BUCKETS + 8 * HISTOGRAM_BUCKETS + 8 * SIZE_TIERS + 8 * 11;

pub struct ColumnStats {
	value_histogram: [AtomicU32; HISTOGRAM_BUCKETS],
	query_histogram: [AtomicU64; SIZE_TIERS], // Per size tier
	oversized: AtomicU64,
	oversized_bytes: AtomicU64,
	total_values: AtomicU64,
	total_bytes: AtomicU64,
	commits: AtomicU64,
	inserted_new: AtomicU64,
	inserted_overwrite: AtomicU64,
	removed_hit: AtomicU64,
	removed_miss: AtomicU64,
	queries_miss: AtomicU64,
	uncompressed_bytes: AtomicU64,
	compression_delta: [AtomicI64; HISTOGRAM_BUCKETS],
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> AtomicU32 {
	let mut buf = [0u8; 4];
	cursor.read_exact(&mut buf).expect("Incorrect stats buffer");
	AtomicU32::new(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> AtomicU64 {
	let mut buf = [0u8; 8];
	cursor.read_exact(&mut buf).expect("Incorrect stats buffer");
	AtomicU64::new(u64::from_le_bytes(buf))
}

fn read_i64(cursor: &mut Cursor<&[u8]>) -> AtomicI64 {
	let mut buf = [0u8; 8];
	cursor.read_exact(&mut buf).expect("Incorrect stats buffer");
	AtomicI64::new(i64::from_le_bytes(buf))
}

fn write_u32(cursor: &mut Cursor<&mut [u8]>, val: &AtomicU32) {
	cursor.write(&val.load(Ordering::Relaxed).to_le_bytes()).expect("Incorrent stats buffer");
}

fn write_u64(cursor: &mut Cursor<&mut [u8]>, val: &AtomicU64) {
	cursor.write(&val.load(Ordering::Relaxed).to_le_bytes()).expect("Incorrent stats buffer");
}

fn write_i64(cursor: &mut Cursor<&mut [u8]>, val: &AtomicI64) {
	cursor.write(&val.load(Ordering::Relaxed).to_le_bytes()).expect("Incorrent stats buffer");
}

fn value_histogram_index(size: u32) -> Option<usize> {
	let bucket = size as usize >> HISTOGRAM_BUCKET_BITS;
	if bucket < HISTOGRAM_BUCKETS {
		Some(bucket)
	} else {
		None
	}
}

impl ColumnStats {
	pub fn from_slice(data: &[u8]) -> ColumnStats {
		let mut cursor = Cursor::new(data);
		let mut value_histogram: [AtomicU32; HISTOGRAM_BUCKETS] = unsafe { MaybeUninit::uninit().assume_init() };
		for n in 0 .. HISTOGRAM_BUCKETS {
			value_histogram[n] = read_u32(&mut cursor);
		}
		let mut query_histogram: [AtomicU64; SIZE_TIERS] = unsafe { MaybeUninit::uninit().assume_init() };
		for n in 0 .. SIZE_TIERS {
			query_histogram[n] = read_u64(&mut cursor);
		}
		let mut stats = ColumnStats {
			value_histogram,
			query_histogram,
			oversized: read_u64(&mut cursor),
			oversized_bytes: read_u64(&mut cursor),
			total_values: read_u64(&mut cursor),
			total_bytes: read_u64(&mut cursor),
			commits: read_u64(&mut cursor),
			inserted_new: read_u64(&mut cursor),
			inserted_overwrite: read_u64(&mut cursor),
			removed_hit: read_u64(&mut cursor),
			removed_miss: read_u64(&mut cursor),
			queries_miss: read_u64(&mut cursor),
			uncompressed_bytes: read_u64(&mut cursor),
			compression_delta: unsafe { MaybeUninit::uninit().assume_init() },
		};
		for n in 0 .. HISTOGRAM_BUCKETS {
			stats.compression_delta[n] = read_i64(&mut cursor);
		}
		stats
	}

	pub fn empty() -> ColumnStats {
		let value_histogram: [AtomicU32; HISTOGRAM_BUCKETS] = unsafe { std::mem::transmute([0u32; HISTOGRAM_BUCKETS]) };
		let query_histogram: [AtomicU64; SIZE_TIERS] = unsafe { std::mem::transmute([0u64; SIZE_TIERS]) };
		ColumnStats {
			value_histogram,
			query_histogram,
			oversized: Default::default(),
			oversized_bytes: Default::default(),
			total_values: Default::default(),
			total_bytes: Default::default(),
			commits: Default::default(),
			inserted_new: Default::default(),
			inserted_overwrite: Default::default(),
			removed_hit: Default::default(),
			removed_miss: Default::default(),
			queries_miss: Default::default(),
			uncompressed_bytes: Default::default(),
			compression_delta: unsafe { std::mem::transmute([0i64; HISTOGRAM_BUCKETS]) },
		}
	}

	pub fn to_slice(&self, data: &mut [u8]) {
		let mut cursor = Cursor::new(data);
		for n in 0 .. HISTOGRAM_BUCKETS {
			write_u32(&mut cursor, &self.value_histogram[n]);
		}
		for n in 0 .. SIZE_TIERS {
			write_u64(&mut cursor, &self.query_histogram[n]);
		}
		write_u64(&mut cursor, &self.oversized);
		write_u64(&mut cursor, &self.oversized_bytes);
		write_u64(&mut cursor, &self.total_values);
		write_u64(&mut cursor, &self.total_bytes);
		write_u64(&mut cursor, &self.commits);
		write_u64(&mut cursor, &self.inserted_new);
		write_u64(&mut cursor, &self.inserted_overwrite);
		write_u64(&mut cursor, &self.removed_hit);
		write_u64(&mut cursor, &self.removed_miss);
		write_u64(&mut cursor, &self.queries_miss);
		write_u64(&mut cursor, &self.uncompressed_bytes);
		for n in 0 .. HISTOGRAM_BUCKETS {
			write_i64(&mut cursor, &self.compression_delta[n]);
		}
	}

	fn write_file(&self, file: &std::fs::File, col: ColId) -> Result<()> {
		let mut writer = std::io::BufWriter::new(file);
		writeln!(writer, "Column {}", col)?;
		writeln!(writer, "Total values: {}", self.total_values.load(Ordering::Relaxed))?;
		writeln!(writer, "Total bytes: {}", self.total_bytes.load(Ordering::Relaxed))?;
		writeln!(writer, "Total oversized values: {}", self.oversized.load(Ordering::Relaxed))?;
		writeln!(writer, "Total oversized bytes: {}", self.oversized_bytes.load(Ordering::Relaxed))?;
		writeln!(writer, "Total commits: {}", self.commits.load(Ordering::Relaxed))?;
		writeln!(writer, "New value insertions: {}", self.inserted_new.load(Ordering::Relaxed))?;
		writeln!(writer, "Existing value insertions: {}", self.inserted_overwrite.load(Ordering::Relaxed))?;
		writeln!(writer, "Removals: {}", self.removed_hit.load(Ordering::Relaxed))?;
		writeln!(writer, "Missed removals: {}", self.removed_miss.load(Ordering::Relaxed))?;
		writeln!(writer, "Uncompressed bytes: {}", self.uncompressed_bytes.load(Ordering::Relaxed))?;
		writeln!(writer, "Compression deltas:")?;
		for i in 0 .. HISTOGRAM_BUCKETS {
			let count = self.value_histogram[i].load(Ordering::Relaxed);
			let delta = self.compression_delta[i].load(Ordering::Relaxed);
			if count != 0 {
				writeln!(writer,
					"    {}-{}: {}",
					i << HISTOGRAM_BUCKET_BITS,
					(((i + 1) << HISTOGRAM_BUCKET_BITS) - 1),
					delta
				)?;
			}
		}
		write!(writer, "Queries per size tier: [")?;
		for i in 0 .. SIZE_TIERS {
			if i == SIZE_TIERS - 1 {
				write!(writer, "{}]\n", self.query_histogram[i].load(Ordering::Relaxed))?;
			} else {
				write!(writer, "{}, ", self.query_histogram[i].load(Ordering::Relaxed))?;
			}
		}
		writeln!(writer, "Missed queries: {}", self.queries_miss.load(Ordering::Relaxed))?;
		writeln!(writer, "Value histogram:")?;
		for i in 0 .. HISTOGRAM_BUCKETS {
			let count = self.value_histogram[i].load(Ordering::Relaxed);
			if count != 0 {
				writeln!(writer,
					"    {}-{}: {}",
					i << HISTOGRAM_BUCKET_BITS,
					(((i + 1) << HISTOGRAM_BUCKET_BITS) - 1),
					count
				)?;
			}
		}
		writeln!(writer, "")?;
		Ok(())
	}

	pub fn write_summary(&self, file: &std::fs::File, col: ColId) {
		let _ = self.write_file(file, col);
	}

	pub fn query_hit(&self, size_tier: u8) {
		self.query_histogram[size_tier as usize].fetch_add(1, Ordering::Relaxed);
	}

	pub fn query_miss(&self) {
		self.queries_miss.fetch_add(1, Ordering::Relaxed);
	}

	pub fn insert(&self, size: u32, compressed: u32) {
		if let Some(index) = value_histogram_index(size) {
			self.value_histogram[index].fetch_add(1, Ordering::Relaxed);
			self.compression_delta[index].fetch_add(size as i64 - compressed as i64, Ordering::Relaxed);
		} else {
			self.oversized.fetch_add(1, Ordering::Relaxed);
			self.oversized_bytes.fetch_add(compressed as u64, Ordering::Relaxed);
		}
		self.total_values.fetch_add(1, Ordering::Relaxed);
		self.total_bytes.fetch_add(compressed as u64, Ordering::Relaxed);
		self.uncompressed_bytes.fetch_add(size as u64, Ordering::Relaxed);
	}

	pub fn remove(&self, size: u32, compressed: u32) {
		if let Some(index) = value_histogram_index(size) {
			self.value_histogram[index].fetch_sub(1, Ordering::Relaxed);
			self.compression_delta[index].fetch_sub(size as i64 - compressed as i64, Ordering::Relaxed);
		} else {
			self.oversized.fetch_sub(1, Ordering::Relaxed);
			self.oversized_bytes.fetch_sub(compressed as u64, Ordering::Relaxed);
		}
		self.total_values.fetch_sub(1, Ordering::Relaxed);
		self.total_bytes.fetch_sub(compressed as u64, Ordering::Relaxed);
		self.uncompressed_bytes.fetch_sub(size as u64, Ordering::Relaxed);
	}

	pub fn insert_val(&self, size: u32, compressed: u32) {
		self.inserted_new.fetch_add(1, Ordering::Relaxed);
		self.insert(size, compressed);
	}

	pub fn remove_val(&self, size: u32, compressed: u32) {
		self.removed_hit.fetch_add(1, Ordering::Relaxed);
		self.remove(size, compressed);
	}

	pub fn remove_miss(&self) {
		self.removed_miss.fetch_add(1, Ordering::Relaxed);
	}

	pub fn replace_val(&self, old: u32, old_compressed: u32, new: u32, new_compressed: u32) {
		self.inserted_overwrite.fetch_add(1, Ordering::Relaxed);
		self.remove(old, old_compressed);
		self.insert(new, new_compressed);
	}

	pub fn commit(&self) {
		self.commits.fetch_add(1, Ordering::Relaxed);
	}
}

#[cfg(test)]
fn set_test_data(set: &mut [u32; HISTOGRAM_BUCKETS]) {
	let data = [
		(32, 36482),
		(64, 157656),
		(96, 317276),
		(128, 29243),
		(160, 19664),
		(192, 14261),
		(224, 9642),
		(256, 5816),
		(288, 4839),
		(320, 3868),
		(352, 3112),
		(384, 2523),
		(416, 29432),
		(448, 2101),
		(480, 2481),
		(512, 8899),
		(544, 8167),
		(576, 357),
		(608, 439),
		(640, 652),
		(672, 574),
		(704, 496),
		(736, 351),
		(768, 160),
		(800, 363),
		(832, 369),
		(864, 378),
		(896, 352),
		(928, 209),
		(960, 220),
		(992, 322),
		(1024, 328),
		(1056, 335),
		(1088, 288),
		(1120, 126),
		(1152, 223),
		(1184, 272),
		(1216, 303),
		(1248, 232),
		(1280, 204),
		(1312, 129),
		(1344, 221),
		(1376, 257),
		(1408, 244),
		(1440, 235),
		(1472, 146),
		(1504, 185),
		(1536, 219),
		(1568, 274),
		(1600, 228),
		(1632, 204),
		(1664, 145),
		(1696, 214),
		(1728, 236),
		(1760, 226),
		(1792, 237),
		(1824, 140),
		(1856, 155),
		(1888, 175),
		(1920, 203),
		(1952, 193),
		(1984, 152),
		(2016, 95),
		(2048, 192),
		(2080, 176),
		(2112, 208),
		(2144, 193),
		(2176, 134),
		(2208, 137),
		(2240, 176),
		(2272, 191),
		(2304, 204),
		(2336, 156),
		(2368, 134),
		(2400, 159),
		(2432, 182),
		(2464, 211),
		(2496, 192),
		(2528, 131),
		(2560, 146),
		(2592, 417),
		(2624, 196),
		(2656, 167),
		(2688, 120),
		(2720, 111),
		(2752, 162),
		(2784, 183),
		(2816, 200),
		(2848, 127),
		(2880, 126),
		(2912, 160),
		(2944, 184),
		(2976, 174),
		(3008, 156),
		(3040, 135),
		(3072, 84),
		(3104, 154),
		(3136, 172),
		(3168, 152),
		(3200, 155),
		(3232, 111),
		(3264, 104),
		(3296, 152),
		(3328, 161),
		(3360, 159),
		(3392, 123),
		(3424, 88),
		(3456, 109),
		(3488, 139),
		(3520, 139),
		(3552, 114),
		(3584, 89),
		(3616, 117),
		(3648, 112),
		(3680, 105),
		(3712, 113),
		(3744, 97),
		(3776, 79),
		(3808, 109),
		(3840, 99),
		(3872, 105),
		(3904, 103),
		(3936, 75),
		(3968, 92),
		(4000, 84),
		(4032, 99),
		(4064, 82),
		(4096, 76),
		(4128, 68),
		(4160, 70),
		(4192, 74),
		(4224, 87),
		(4256, 76),
		(4288, 51),
		(4320, 70),
		(4352, 60),
		(4384, 75),
		(4416, 90),
		(4448, 68),
		(4480, 44),
		(4512, 81),
		(4544, 59),
		(4576, 63),
		(4608, 80),
		(4640, 56),
		(4672, 40),
		(4704, 61),
		(4736, 59),
		(4768, 58),
		(4800, 63),
		(4832, 41),
		(4864, 49),
		(4896, 56),
		(4928, 46),
		(4960, 71),
		(4992, 57),
		(5024, 66),
		(5056, 672),
		(5088, 29),
		(5120, 37),
		(5152, 21),
		(5184, 18),
		(5216, 9),
		(5248, 23),
		(5280, 21),
		(5312, 19),
		(5344, 14),
		(5376, 14),
		(5408, 20),
		(5440, 16),
		(5472, 16),
		(5504, 22),
		(5536, 9),
		(5568, 16),
		(5600, 15),
		(5632, 23),
		(5664, 21),
		(5696, 11),
		(5728, 10),
		(5760, 16),
		(5792, 13),
		(5824, 18),
		(5856, 17),
		(5888, 7),
		(5920, 10),
		(5952, 12),
		(5984, 19),
		(6016, 15),
		(6048, 14),
		(6080, 7),
		(6112, 6),
		(6144, 12),
		(6176, 9),
		(6208, 9),
		(6240, 9),
		(6272, 5),
		(6304, 14),
		(6336, 9),
		(6368, 10),
		(6400, 12),
		(6432, 7),
		(6464, 8),
		(6496, 11),
		(6528, 8),
		(6560, 5),
		(6592, 5),
		(6624, 2),
		(6656, 9),
		(6688, 3),
		(6720, 3),
		(6752, 6),
		(6784, 1),
		(6816, 2),
		(6848, 1),
		(6880, 1),
		(6912, 5),
		(6944, 6),
		(6976, 3),
		(7008, 6),
		(7040, 8),
		(7072, 6),
		(7104, 4),
		(7136, 2),
		(7168, 1),
		(7200, 3),
		(7232, 3),
		(7264, 3),
		(7296, 1),
		(7328, 2),
		(7360, 1),
		(7392, 4),
		(7424, 1),
		(7456, 5),
		(7488, 4),
		(7552, 1),
		(7616, 4),
		(7648, 2),
		(7680, 1),
		(7744, 1),
		(7776, 2),
		(7808, 4),
		(7840, 1),
		(7872, 2),
		(7904, 1),
		(7936, 1),
		(7968, 1),
		(8000, 1),
		(8064, 1),
		(8096, 3),
		(8128, 1),
		(8160, 1),
		(8192, 1),
		(8256, 1),
		(8288, 1),
		(8384, 1),
		(8416, 1),
		(8480, 1),
		(8512, 2),
		(8608, 2),
		(8672, 1),
		(8704, 2),
		(8800, 1),
		(8832, 3),
		(8928, 2),
		(8960, 2),
		(9184, 2),
		(9216, 1),
		(9248, 2),
		(9280, 2),
		(9408, 1),
		(9440, 2),
		(9472, 1),
		(9504, 6),
		(9536, 1),
		(9600, 1),
		(9632, 1),
		(9664, 4),
		(9696, 2),
		(9792, 1),
		(10560, 2),
		(10592, 1),
		(10624, 5),
		(10656, 7),
		(10688, 327),
		(11872, 2),
		(11904, 1),
		(21088, 1),
	];
	for d in data {
		set[d.0 / 32] = d.1;
	}
}

#[cfg(test)]
// 100 byte per additional query penalitiy
const OVERSIZED_ADJUSTMENT_FACTOR: usize = 100;

#[cfg(test)]
fn calculate_oversized_result(nb_oversized: usize, size_oversized: usize, chunk_size: u16) -> usize {
	let chunk_size = chunk_size as usize;
	let avg_size = size_oversized / nb_oversized;
	let rem = avg_size % chunk_size;
	let (avg_nb_chunk, avg_fill) = if rem == 0 {
		((avg_size / chunk_size), 0)
	} else {
		((avg_size / chunk_size) + 1, chunk_size - rem)
	};
	let avg_result = (avg_nb_chunk - 1) * (10 + OVERSIZED_ADJUSTMENT_FACTOR);
	nb_oversized * (avg_result + avg_fill)
}

#[cfg(test)]
fn calculate_result(set: &[u32; HISTOGRAM_BUCKETS], threshold: &[u16; 15]) -> usize {
	let mut result = 0;
	let mut set_ptr = 0;
	for (i, set) in set.iter().enumerate() {
		let size = i * 32;
		while size > threshold[set_ptr] as usize {
			set_ptr += 1;
		}
		if size < threshold[set_ptr] as usize {
			result += (threshold[set_ptr] as usize - size) * *set as usize;
		}
	}
	result
}

#[cfg(test)]
fn calculate_result_interval(ix: Option<usize>, set: &[u32; HISTOGRAM_BUCKETS], threshold: &[u16; 15]) -> usize {
	let mut result = 0;
	let (start, end, col_size) = if let Some(ix) = ix {
		(threshold[ix] / 32, threshold[ix + 1] / 32, threshold[ix + 1] as usize)
	} else {
		(0, threshold[0] / 32, threshold[0] as usize)
	};
	for (i, set) in set[start as usize..end as usize].iter().enumerate() {
		let size = (start as usize + i) * 32;
		if size < col_size {
			result += (col_size - size) * *set as usize;
		} else {
			break;
		}
	}
	result
}


// change new set and return gain if better.
#[cfg(test)]
fn check_step(
	ix: usize,
	right: bool,
	threshold: &[u16; 15],
	new_threshold: &mut [u16; 15],
	set: &[u32; HISTOGRAM_BUCKETS],
	num_step: usize,
) -> usize {


	let (size_interval, limit) = if right {
		let limit = threshold[ix + 1] / 32;
		(limit - (threshold[ix] / 32), limit)
	} else {
		let limit = threshold[ix - 1] / 32;
		((threshold[ix] / 32) - limit, limit)
	};
	let step = (size_interval as usize / num_step) as u16 + 1;
	let mut best_result = 0;
	let mut best_ix = None;
	let old_value = new_threshold[ix];
	let mut set_ix = threshold[ix] / 32;
	while if right { set_ix < limit } else { set_ix > limit } {
		let mut old_weight = 0;
		let mut new_weight = 0;
		if right {
			set_ix = std::cmp::min(limit, set_ix + step); 
			new_threshold[ix] = set_ix * 32;
		} else {
			set_ix = std::cmp::max(limit, set_ix - step); 
			new_threshold[ix] = set_ix * 32;
		}
		if ix > 0 {
			// TODO memoize old weight
			old_weight += calculate_result_interval(Some(ix - 1), set, threshold);
			new_weight += calculate_result_interval(Some(ix - 1), set, new_threshold);
		} else {
			// TODO memoize old weight
			old_weight += calculate_result_interval(None, set, threshold);
			new_weight += calculate_result_interval(None, set, new_threshold);
		}
		if ix < 14 {
			// TODO memoize old weight
			old_weight += calculate_result_interval(Some(ix), set, threshold);
			new_weight += calculate_result_interval(Some(ix), set, new_threshold);
		} else {
			panic!("not here");
		}
		if new_weight < old_weight {
			if old_weight - new_weight > best_result {
				best_result = old_weight - new_weight;
				best_ix = Some(new_threshold[ix]);
			}
		}
	}
	if let Some(best_ix) = best_ix {
		new_threshold[ix] = best_ix;
		best_result
	} else {
		new_threshold[ix] = old_value;
		0
	}
}

#[test]
fn find_best_threshold() {
	use rand::{SeedableRng, RngCore};
	let seed = 0u64;
	let num_step = 33;
	let mut set: [u32; HISTOGRAM_BUCKETS] = [0; HISTOGRAM_BUCKETS];
	set_test_data(&mut set);
	let mut nb_oversized = 2;
	let mut size_oversized = 2303191;
	let mut threshold = [96u16, 128, 192, 256, 320, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 32768];
	let mut threshold_right = [96u16, 128, 192, 256, 320, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 32768];
	let mut threshold_left = [96u16, 128, 192, 256, 320, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 32768];
	let mut unchanged_threshold = [false; 15];
	let mut unchanged_threshold_number = 0;
	let prev_result: usize = calculate_oversized_result(nb_oversized, size_oversized, threshold[14])
		+ calculate_result(&set, &threshold);
	let mut result: usize = prev_result;

	let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);


	let max_iter = 16 * 16; // seems small
	let mut nb_iter = 0;
	while max_iter > nb_iter {
		nb_iter += 1;
		//let mut th = (rng.next_u32() % (15 - unchanged_threshold_number)) as usize;
		let mut th = (rng.next_u32() % (14 - unchanged_threshold_number)) as usize;

		if unchanged_threshold_number > 0 {
			for i in 0..th + 1 {
				if unchanged_threshold[i] {
					th += 1;
				}
			}
		}
		// TODO

		let right = if th < 14 {
			// check right
			check_step(
				th,
				true,
				&threshold,
				&mut threshold_right,
				&set,
				num_step,
			)
		} else { 0 };
		let left = if th > 0 {
			// check left
			check_step(
				th,
				false,
				&threshold,
				&mut threshold_left,
				&set,
				num_step,
			)
		} else { 0 };
		if right > left {
			unchanged_threshold = [false; 15];
			unchanged_threshold_number = 0;
			threshold[th] = threshold_right[th];
			threshold_left[th] = threshold_right[th];
			result -= right;
		} else if left > right {
			unchanged_threshold = [false; 15];
			unchanged_threshold_number = 0;
			threshold[th] = threshold_left[th];
			threshold_right[th] = threshold_left[th];
			result -= left;
		} else {
			unchanged_threshold[th] = true;
			unchanged_threshold_number += 1;
		}
		// this ommit the last one
		if unchanged_threshold_number == 14 {
			// fully converge
			break;
		}
	}
	
	let next_result: usize = calculate_oversized_result(nb_oversized, size_oversized, threshold[14])
		+ calculate_result(&set, &threshold);
	panic!("{:?}", (threshold, prev_result, result, next_result, nb_iter));
}
