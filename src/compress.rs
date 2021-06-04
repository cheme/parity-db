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


//! Compression utility and types.

/// Different compression type
/// allowend and their u8 representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CompressionType {
	NoCompression = 0,
	Lz4 = 1,
	Lz4High = 2,
	Lz4Low = 3,
	Zstd = 4,
	Snappy = 5,
	Snap = 6,
}

/// Compression implementation.
pub(crate) struct Compress {
	inner: Compressor,
	pub treshold: u32,
}

impl Compress {
	pub(crate) fn new(kind: CompressionType, treshold: u32) -> Self {
		Compress {
			inner: kind.into(),
			treshold,
		}
	}
}

enum Compressor {
	NoCompression(NoCompression),
	Lz4(lz4::Lz4),
	Lz4High(lz4::Lz4High),
	Lz4Low(lz4::Lz4Low),
	Zstd(zstd::Zstd),
	Snappy(snappy::Snappy),
	Snap(snap::Snap),
}

impl From<u8> for CompressionType {
	fn from(comp_type: u8) -> Self {
		match comp_type {
			a if a == CompressionType::NoCompression as u8 => CompressionType::NoCompression,
			a if a == CompressionType::Lz4 as u8 => CompressionType::Lz4,
			a if a == CompressionType::Lz4High as u8 => CompressionType::Lz4High,
			a if a == CompressionType::Lz4Low as u8 => CompressionType::Lz4Low,
			a if a == CompressionType::Zstd as u8 => CompressionType::Zstd,
			a if a == CompressionType::Snappy as u8 => CompressionType::Snappy,
			a if a == CompressionType::Snap as u8 => CompressionType::Snap,
			_ => panic!("Unkwown compression."),
		}
	}
}

impl From<CompressionType> for Compressor {
	fn from(comp_type: CompressionType) -> Self {
		match comp_type {
			CompressionType::NoCompression => Compressor::NoCompression(NoCompression),
			CompressionType::Lz4 => Compressor::Lz4(lz4::Lz4::new()),
			CompressionType::Lz4High => Compressor::Lz4High(lz4::Lz4High::new()),
			CompressionType::Lz4Low => Compressor::Lz4Low(lz4::Lz4Low::new()),
			CompressionType::Zstd => Compressor::Zstd(zstd::Zstd::new()),
			CompressionType::Snappy => Compressor::Snappy(snappy::Snappy::new()),
			CompressionType::Snap => Compressor::Snap(snap::Snap::new()),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}
}

impl From<&Compress> for CompressionType {
	fn from(compression: &Compress) -> Self {
		match compression.inner {
			Compressor::NoCompression(_) => CompressionType::NoCompression,
			Compressor::Lz4(_) => CompressionType::Lz4,
			Compressor::Lz4High(_) => CompressionType::Lz4High,
			Compressor::Lz4Low(_) => CompressionType::Lz4Low,
			Compressor::Zstd(_) => CompressionType::Zstd,
			Compressor::Snappy(_) => CompressionType::Snappy,
			Compressor::Snap(_) => CompressionType::Snap,
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}
}

impl Compress {
	pub(crate) fn compress(&self, buf: &[u8]) -> Vec<u8> {
		match &self.inner {
			Compressor::NoCompression(inner) => inner.compress(buf),
			Compressor::Lz4(inner) => inner.compress(buf),
			Compressor::Lz4High(inner) => inner.compress(buf),
			Compressor::Lz4Low(inner) => inner.compress(buf),
			Compressor::Zstd(inner) => inner.compress(buf),
			Compressor::Snappy(inner) => inner.compress(buf),
			Compressor::Snap(inner) => inner.compress(buf),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}

	pub(crate) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
		match &self.inner {
			Compressor::NoCompression(inner) => inner.decompress(buf),
			Compressor::Lz4(inner) => inner.decompress(buf),
			Compressor::Lz4High(inner) => inner.decompress(buf),
			Compressor::Lz4Low(inner) => inner.decompress(buf),
			Compressor::Zstd(inner) => inner.decompress(buf),
			Compressor::Snappy(inner) => inner.decompress(buf),
			Compressor::Snap(inner) => inner.decompress(buf),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}
}

struct NoCompression;

impl NoCompression {
	fn compress(&self, buf: &[u8]) -> Vec<u8> {
		buf.to_vec()
	}

	fn decompress(&self, buf: &[u8]) -> Vec<u8> {
		buf.to_vec()
	}
}

mod lz4 {
	pub(super) type Lz4 = Lz4Inner<DefaultMode>;
	pub(super) type Lz4High = Lz4Inner<HighMode>;
	pub(super) type Lz4Low = Lz4Inner<LowMode>;

	pub(super) trait Lz4Mode {
		const MODE: lz4::block::CompressionMode;
	}

	pub(super) struct DefaultMode;
	pub(super) struct LowMode;
	pub(super) struct HighMode;

	pub(super) struct Lz4Inner<M>(std::marker::PhantomData<M>);

	impl Lz4Mode for DefaultMode {
		const MODE: lz4::block::CompressionMode = lz4::block::CompressionMode::DEFAULT;
	}
	impl Lz4Mode for HighMode {
		const MODE: lz4::block::CompressionMode = lz4::block::CompressionMode::HIGHCOMPRESSION(9);
	}
	impl Lz4Mode for LowMode {
		const MODE: lz4::block::CompressionMode = lz4::block::CompressionMode::FAST(1);
	}

	impl<M: Lz4Mode> Lz4Inner<M> {
		pub(super) fn new() -> Self {
			Lz4Inner(Default::default())
		}

		pub(super) fn compress(&self, buf: &[u8]) -> Vec<u8> {
			lz4::block::compress(buf, Some(M::MODE), true)
				.unwrap()
		}

		pub(super) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
			lz4::block::decompress(buf, None)
				.unwrap()
		}
	}
}

mod zstd {
	pub(super) struct Zstd {
		//write_buffer: RefCell<Vec<u8>>,
	}

	impl Zstd {
		pub(super) fn new() -> Self {
			Zstd {
			}
		}

		pub(super) fn compress(&self, value: &[u8]) -> Vec<u8> {
			//let buf = self.write_buffer.borrow_mut();
			//buf.clear();
			let buf = Vec::new();
			let mut encoder = zstd::Encoder::new(buf, 0).unwrap();
			use std::io::Write;
			encoder.write_all(value).unwrap();
			let buf = encoder.finish().unwrap();
			buf
		}

		pub(super) fn decompress(&self, value: &[u8]) -> Vec<u8> {
			let decoder = zstd::Decoder::with_buffer(value).unwrap();
			let buf = decoder.finish();
			buf.to_vec()
		}
	}
}

mod snappy {
	pub(super) struct Snappy {
	}

	impl Snappy {
		pub(super) fn new() -> Self {
			Snappy {
			}
		}

		pub(super) fn compress(&self, value: &[u8]) -> Vec<u8> {
			snappy::compress(value)
		}

		pub(super) fn decompress(&self, value: &[u8]) -> Vec<u8> {
			snappy::uncompress(value).unwrap()
		}
	}
}

mod snap {
	pub(super) struct Snap {
	}

	impl Snap {
		pub(super) fn new() -> Self {
			Snap {
			}
		}

		pub(super) fn compress(&self, value: &[u8]) -> Vec<u8> {
			use std::io::Write;
			let mut buf = Vec::with_capacity(value.len() << 3);
			let mut encoder = snap::write::FrameEncoder::new(&mut buf);
			encoder.write(value).unwrap();
			std::mem::drop(encoder);
			buf
		}

		pub(super) fn decompress(&self, value: &[u8]) -> Vec<u8> {
			use std::io::Read;
			let mut buf = Vec::with_capacity(value.len());
			let mut decoder = snap::read::FrameDecoder::new(value);
			decoder.read_to_end(&mut buf).unwrap();
			buf
		}
	}
}
