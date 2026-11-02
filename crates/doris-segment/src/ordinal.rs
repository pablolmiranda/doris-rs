//! Ordinal index: maps a column's data pages to their first row ordinal.
//! `OrdinalIndexPB.root_page` is either the single data page itself or an index page whose body
//! is `num_entries` × (`varint32 len`, key = big-endian u64 ordinal, `varint64 offset`,
//! `varint32 size`), as written by `IndexPageBuilder::add` and `PagePointer::encode_to`.
use crate::error::Error;
use crate::footer::SegmentFile;
use crate::page::read_page;
use crate::pb::segment_v2::{ColumnIndexTypePb, ColumnMetaPb, PagePointerPb};
use crate::varint::{read_varint32, read_varint64};

/// Pointers to all data pages of a column, in ordinal order.
pub fn data_pages(file: &SegmentFile, meta: &ColumnMetaPb) -> Result<Vec<PagePointerPb>, Error> {
    let unique_id = meta.unique_id.unwrap_or(0);
    let root = meta
        .indexes
        .iter()
        .find(|idx| idx.r#type == Some(ColumnIndexTypePb::OrdinalIndex as i32))
        .and_then(|idx| idx.ordinal_index.as_ref())
        .and_then(|oi| oi.root_page.as_ref())
        .ok_or(Error::NoOrdinalIndex(unique_id))?;
    let root_ptr = root.root_page.ok_or(Error::NoOrdinalIndex(unique_id))?;
    if root.is_root_data_page.unwrap_or(false) {
        return Ok(vec![root_ptr]);
    }
    let page = read_page(file, &root_ptr, meta.compression())?;
    let n = page
        .footer
        .index_page_footer
        .as_ref()
        .and_then(|f| f.num_entries)
        .ok_or_else(|| Error::Corrupt("ordinal index page without num_entries".into()))?
        as usize;
    let body = &page.body;
    let mut pos = 0usize;
    let mut pages = Vec::with_capacity(n);
    for _ in 0..n {
        let key_len = read_varint32(body, &mut pos)? as usize;
        pos += key_len;
        let offset = read_varint64(body, &mut pos)?;
        let size = read_varint32(body, &mut pos)?;
        pages.push(PagePointerPb { offset, size });
    }
    Ok(pages)
}
