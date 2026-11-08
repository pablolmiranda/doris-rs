//! The zone map index of a column (`ZoneMapIndexWriter`): a segment `ZoneMapPB` in the column
//! meta, and one `ZoneMapPB` per data page, serialized into an IndexedColumn of binary plain
//! (V1) pages with an ordinal index.
use prost::Message;

use crate::binary::decode_plain_v1;
use crate::column::column_meta;
use crate::error::Error;
use crate::footer::SegmentFile;
use crate::ordinal::index_entries;
use crate::page::read_page;
use crate::pb::segment_v2::{ColumnIndexTypePb, CompressionTypePb, ZoneMapPb};

#[derive(Debug, Clone, PartialEq)]
pub struct ZoneMaps {
    pub segment: ZoneMapPb,
    pub pages: Vec<ZoneMapPb>,
}

pub fn read_zone_maps(file: &SegmentFile, unique_id: u32) -> Result<ZoneMaps, Error> {
    let meta = column_meta(file, unique_id)?;
    let index = meta
        .indexes
        .iter()
        .find(|i| i.r#type == Some(ColumnIndexTypePb::ZoneMapIndex as i32))
        .and_then(|i| i.zone_map_index.as_ref())
        .ok_or_else(|| Error::Corrupt(format!("column {unique_id} has no zone map index")))?;
    let segment = index
        .segment_zone_map
        .clone()
        .ok_or_else(|| Error::Corrupt(format!("column {unique_id}: no segment zone map")))?;
    let column = index
        .page_zone_maps
        .as_ref()
        .ok_or_else(|| Error::Corrupt(format!("column {unique_id}: no page zone maps")))?;
    let codec = CompressionTypePb::try_from(column.compression.unwrap_or(0)).unwrap_or(CompressionTypePb::NoCompression);
    let root = column
        .ordinal_index_meta
        .as_ref()
        .ok_or_else(|| Error::Corrupt(format!("column {unique_id}: page zone maps without an ordinal index")))?;
    let root_ptr = root
        .root_page
        .ok_or_else(|| Error::Corrupt(format!("column {unique_id}: page zone maps without a root page")))?;
    let data_pages = if root.is_root_data_page.unwrap_or(false) {
        vec![root_ptr]
    } else {
        index_entries(file, &root_ptr, codec)?.into_iter().map(|(_, p)| p).collect()
    };
    let mut pages = Vec::new();
    for ptr in data_pages {
        let page = read_page(file, &ptr, codec)?;
        for value in decode_plain_v1(&page.body)? {
            pages.push(ZoneMapPb::decode(value)?);
        }
    }
    if pages.len() as i64 != column.num_values.unwrap_or(0) {
        return Err(Error::Corrupt(format!("column {unique_id}: page zone map count differs from num_values")));
    }
    Ok(ZoneMaps { segment, pages })
}
