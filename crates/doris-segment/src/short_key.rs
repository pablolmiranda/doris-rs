//! The short key index page (`ShortKeyIndexBuilder::finalize`): the body is every key, then a
//! varint32 start offset per key; `ShortKeyFooterPB` in the page footer gives the counts.
use crate::error::Error;
use crate::footer::SegmentFile;
use crate::page::read_page;
use crate::pb::segment_v2::CompressionTypePb;
use crate::varint::read_varint32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortKeys {
    pub num_items: u32,
    /// One encoded short key per block of `num_rows_per_block` rows.
    pub key_bytes: Vec<Vec<u8>>,
    pub num_rows_per_block: u32,
    pub num_segment_rows: u32,
}

/// The segment's short key page (`SegmentFooterPB.short_key_index_page`).
pub fn read_short_keys(file: &SegmentFile) -> Result<ShortKeys, Error> {
    let ptr = file
        .footer
        .short_key_index_page
        .ok_or_else(|| Error::Corrupt("segment without a short key index page".into()))?;
    let page = read_page(file, &ptr, CompressionTypePb::NoCompression)?;
    let f = page
        .footer
        .short_key_page_footer
        .ok_or_else(|| Error::Corrupt("short key page without its footer".into()))?;
    let (num_items, key_len) = (f.num_items.unwrap_or(0), f.key_bytes.unwrap_or(0) as usize);
    // ShortKeyIndexDecoder::parse
    if page.body.len() != key_len + f.offset_bytes.unwrap_or(0) as usize {
        return Err(Error::Corrupt("short key page size differs from its footer".into()));
    }
    let (keys, offsets) = page.body.split_at(key_len);
    let mut pos = 0;
    let mut starts = Vec::with_capacity((num_items as usize).min(offsets.len()));
    for _ in 0..num_items {
        let o = read_varint32(offsets, &mut pos)? as usize;
        if o > key_len {
            return Err(Error::Corrupt("short key offset past the keys".into()));
        }
        starts.push(o);
    }
    if pos != offsets.len() {
        return Err(Error::Corrupt("bytes left after the short key offsets".into()));
    }
    starts.push(key_len);
    let mut key_bytes = Vec::with_capacity(num_items as usize);
    for w in starts.windows(2) {
        if w[0] > w[1] {
            return Err(Error::Corrupt("short key offsets out of order".into()));
        }
        key_bytes.push(keys[w[0]..w[1]].to_vec());
    }
    Ok(ShortKeys {
        num_items,
        key_bytes,
        num_rows_per_block: f.num_rows_per_block.unwrap_or(0),
        num_segment_rows: f.num_segment_rows.unwrap_or(0),
    })
}
