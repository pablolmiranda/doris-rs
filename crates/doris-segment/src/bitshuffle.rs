//! Decoder for the bitshuffle 0.5.1 `bshuf_decompress_lz4` format (scalar algorithm, matching
//! `bshuf_untrans_bit_elem_scal`). The input is a sequence of blocks, each a big-endian u32
//! length followed by an LZ4 block that decompresses to `block_size * elem_size` transposed
//! bytes; a final partial block (rounded down to a multiple of 8 elements) and then any
//! leftover (< 8) elements copied verbatim.
use crate::error::Error;

const TARGET_BLOCK_SIZE_B: usize = 8192;
const BLOCKED_MULT: usize = 8;
const MIN_RECOMMEND_BLOCK: usize = 128;

/// `bshuf_default_block_size`: must stay stable across versions, as upstream notes.
pub fn default_block_size(elem_size: usize) -> usize {
    let block = TARGET_BLOCK_SIZE_B / elem_size;
    let block = (block / BLOCKED_MULT) * BLOCKED_MULT;
    block.max(MIN_RECOMMEND_BLOCK)
}

/// Inverse of `bshuf_compress_lz4(in, out, num_elems, elem_size, 0)`.
pub fn decompress_lz4(input: &[u8], num_elems: usize, elem_size: usize) -> Result<Vec<u8>, Error> {
    let block = default_block_size(elem_size);
    let mut out = Vec::with_capacity(num_elems * elem_size);
    let mut pos = 0usize;
    for _ in 0..num_elems / block {
        pos = decode_block(input, pos, block, elem_size, &mut out)?;
    }
    let last = (num_elems % block) / BLOCKED_MULT * BLOCKED_MULT;
    if last > 0 {
        pos = decode_block(input, pos, last, elem_size, &mut out)?;
    }
    let leftover = (num_elems % BLOCKED_MULT) * elem_size;
    let tail = input
        .get(pos..pos + leftover)
        .ok_or_else(|| Error::Corrupt("bitshuffle leftover bytes past end".into()))?;
    out.extend_from_slice(tail);
    Ok(out)
}

fn decode_block(
    input: &[u8],
    pos: usize,
    size: usize,
    elem_size: usize,
    out: &mut Vec<u8>,
) -> Result<usize, Error> {
    let hdr = input
        .get(pos..pos + 4)
        .ok_or_else(|| Error::Corrupt("bitshuffle block header past end".into()))?;
    let nbytes = u32::from_be_bytes(hdr.try_into().expect("4 bytes")) as usize;
    let comp = input
        .get(pos + 4..pos + 4 + nbytes)
        .ok_or_else(|| Error::Corrupt("bitshuffle block body past end".into()))?;
    let want = size * elem_size;
    let tmp = lz4_flex::block::decompress(comp, want)
        .map_err(|e| Error::Decompress(format!("lz4 block: {e}")))?;
    if tmp.len() != want {
        return Err(Error::Decompress(format!(
            "bitshuffle block decompressed to {} bytes, want {want}",
            tmp.len()
        )));
    }
    out.extend_from_slice(&untrans_bit_elem(&tmp, size, elem_size));
    Ok(pos + 4 + nbytes)
}

/// `bshuf_untrans_bit_elem_scal`: `size` must be a multiple of 8.
pub fn untrans_bit_elem(input: &[u8], size: usize, elem_size: usize) -> Vec<u8> {
    debug_assert_eq!(size % 8, 0);
    let tmp = trans_byte_bitrow(input, size, elem_size);
    shuffle_bit_eightelem(&tmp, size, elem_size)
}

fn trans_byte_bitrow(input: &[u8], size: usize, elem_size: usize) -> Vec<u8> {
    let nbyte_row = size / 8;
    let mut out = vec![0u8; size * elem_size];
    for jj in 0..elem_size {
        for ii in 0..nbyte_row {
            for kk in 0..8 {
                out[ii * 8 * elem_size + jj * 8 + kk] = input[(jj * 8 + kk) * nbyte_row + ii];
            }
        }
    }
    out
}

/// Transpose an 8x8 bit matrix held in a u64 (little-endian byte order).
fn trans_bit_8x8(mut x: u64) -> u64 {
    let mut t = (x ^ (x >> 7)) & 0x00AA00AA00AA00AA;
    x = x ^ t ^ (t << 7);
    t = (x ^ (x >> 14)) & 0x0000CCCC0000CCCC;
    x = x ^ t ^ (t << 14);
    t = (x ^ (x >> 28)) & 0x00000000F0F0F0F0;
    x ^ t ^ (t << 28)
}

fn shuffle_bit_eightelem(input: &[u8], size: usize, elem_size: usize) -> Vec<u8> {
    let nbyte = elem_size * size;
    let mut out = vec![0u8; nbyte];
    let mut jj = 0;
    while jj < 8 * elem_size {
        let mut ii = 0;
        while ii + 8 * elem_size - 1 < nbyte {
            let mut x =
                u64::from_le_bytes(input[ii + jj..ii + jj + 8].try_into().expect("8 bytes"));
            x = trans_bit_8x8(x);
            for kk in 0..8 {
                out[ii + jj / 8 + kk * elem_size] = x as u8;
                x >>= 8;
            }
            ii += 8 * elem_size;
        }
        jj += 8;
    }
    out
}
