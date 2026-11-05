//! Binary columns (CHAR, VARCHAR, STRING) in the V2 storage format.
//!
//! The default encoding is DICT_ENCODING (`BinaryDictPageBuilder`). Each column has one
//! dictionary page, a BinaryPlainPage of its distinct values, and data pages whose values begin
//! with a u32 LE mode:
//! - DICT (5): a bit-shuffled INT page of codewords into the dictionary;
//! - PLAIN (2): a BinaryPlainPage of the values themselves, written once the dictionary fills;
//! - PLAIN_V2 (8): the V3-format variant, not supported here.
//!
//! BinaryPlainPage (V1) is `values | offsets (u32 LE each) | num_elems (u32 LE)`: value `i`
//! spans `offsets[i]..offsets[i + 1]` and the last one ends where the offsets begin.
//! CHAR(N) is stored zero-padded to N bytes, dictionary included; the reference reads it back
//! cut at the first NUL (`BinaryPlainPageCharStripPreDecoder`, `strnlen`), and so does this.
use crate::column::{decode_bitshuffle_values, field_type, read_data_pages};
use crate::error::Error;
use crate::footer::SegmentFile;
use crate::page::read_page;
use crate::pb::segment_v2::{ColumnMetaPb, EncodingTypePb, PageTypePb};
use crate::values::ColumnValues;

const MODE_HEADER: usize = 4;

/// Shape of one dictionary-encoded data page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictPageMode {
    /// Codewords into the dictionary.
    Dict,
    /// The values themselves (written once the dictionary filled).
    Plain,
}

/// Split a BinaryPlainPage (V1) into its values, validating every offset against the page.
pub fn decode_plain_v1(page: &[u8]) -> Result<Vec<&[u8]>, Error> {
    if page.len() < 4 {
        return Err(Error::Corrupt("plain page shorter than its trailer".into()));
    }
    let n = u32::from_le_bytes(page[page.len() - 4..].try_into().expect("4 bytes")) as usize;
    let trailer = n
        .checked_add(1)
        .and_then(|x| x.checked_mul(4))
        .filter(|t| *t <= page.len())
        .ok_or_else(|| {
            Error::Corrupt(format!(
                "plain page of {} bytes cannot hold {n} offsets",
                page.len()
            ))
        })?;
    let offsets_pos = page.len() - trailer;
    let offset = |i: usize| {
        let at = offsets_pos + 4 * i;
        u32::from_le_bytes(page[at..at + 4].try_into().expect("4 bytes")) as usize
    };
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let start = offset(i);
        let end = if i + 1 < n {
            offset(i + 1)
        } else {
            offsets_pos
        };
        if start > end || end > offsets_pos {
            return Err(Error::Corrupt(format!(
                "plain page value {i} spans {start}..{end}, outside 0..{offsets_pos}"
            )));
        }
        out.push(&page[start..end]);
    }
    Ok(out)
}

/// Read a column's dictionary page into its words (CHAR words still zero-padded).
pub fn read_dictionary(file: &SegmentFile, meta: &ColumnMetaPb) -> Result<Vec<Vec<u8>>, Error> {
    let unique_id = meta.unique_id.unwrap_or(0);
    let ptr = meta.dict_page.ok_or_else(|| {
        Error::Corrupt(format!(
            "column {unique_id}: dictionary encoding without a dictionary page"
        ))
    })?;
    let page = read_page(file, &ptr, meta.compression())?;
    if page.footer.r#type != Some(PageTypePb::DictionaryPage as i32) {
        return Err(Error::Corrupt(format!(
            "column {unique_id}: dictionary pointer leads to a page of type {:?}",
            page.footer.r#type
        )));
    }
    let encoding = page
        .footer
        .dict_page_footer
        .as_ref()
        .and_then(|f| f.encoding)
        .unwrap_or(EncodingTypePb::UnknownEncoding as i32);
    if encoding != EncodingTypePb::PlainEncoding as i32 {
        return Err(Error::UnsupportedEncoding {
            column: unique_id,
            encoding,
        });
    }
    Ok(decode_plain_v1(&page.body)?
        .into_iter()
        .map(<[u8]>::to_vec)
        .collect())
}

/// Decode the values of one dictionary-encoded data page (its null bitmap already removed).
pub fn decode_dict_data_page(
    values: &[u8],
    non_null: usize,
    dict: &[Vec<u8>],
) -> Result<(DictPageMode, Vec<Vec<u8>>), Error> {
    if values.len() < MODE_HEADER {
        return Err(Error::Corrupt(
            "dictionary data page shorter than its mode header".into(),
        ));
    }
    let mode = u32::from_le_bytes(values[..4].try_into().expect("4 bytes"));
    let payload = &values[MODE_HEADER..];
    if mode == EncodingTypePb::DictEncoding as u32 {
        let codes = decode_bitshuffle_values(payload, non_null, 4)?;
        let mut out = Vec::with_capacity(non_null);
        for c in codes.chunks_exact(4) {
            let code = i32::from_le_bytes(c.try_into().expect("4 bytes"));
            let word = usize::try_from(code)
                .ok()
                .and_then(|i| dict.get(i))
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "dictionary codeword {code} outside a dictionary of {}",
                        dict.len()
                    ))
                })?;
            out.push(word.clone());
        }
        Ok((DictPageMode::Dict, out))
    } else if mode == EncodingTypePb::PlainEncoding as u32 {
        let vals = decode_plain_v1(payload)?;
        if vals.len() != non_null {
            return Err(Error::Corrupt(format!(
                "plain page holds {} values but the null bitmap implies {non_null}",
                vals.len()
            )));
        }
        Ok((
            DictPageMode::Plain,
            vals.into_iter().map(<[u8]>::to_vec).collect(),
        ))
    } else {
        Err(Error::UnsupportedDictPageMode(mode))
    }
}

/// Decode every row of a CHAR, VARCHAR or STRING column.
pub(crate) fn read_binary_column(
    file: &SegmentFile,
    meta: &ColumnMetaPb,
) -> Result<ColumnValues, Error> {
    let unique_id = meta.unique_id.unwrap_or(0);
    let plain = match meta.encoding() {
        EncodingTypePb::DictEncoding => false,
        EncodingTypePb::PlainEncoding => true,
        _ => {
            return Err(Error::UnsupportedEncoding {
                column: unique_id,
                encoding: meta.encoding.unwrap_or(0),
            })
        }
    };
    let is_char = meta.r#type == Some(field_type::CHAR);
    let dict = if plain { Vec::new() } else { read_dictionary(file, meta)? };
    let mut out = Vec::new();
    for page in read_data_pages(file, meta)? {
        let values = if plain {
            // BinaryPlainPageDecoder (V1): the page is the values, offsets and count
            let v: Vec<Vec<u8>> = decode_plain_v1(&page.values)?.into_iter().map(<[u8]>::to_vec).collect();
            if v.len() != page.non_null() {
                return Err(Error::Corrupt(format!(
                    "plain page holds {} values but the null bitmap implies {}",
                    v.len(),
                    page.non_null()
                )));
            }
            v
        } else {
            decode_dict_data_page(&page.values, page.non_null(), &dict)?.1
        };
        let mut values = values.into_iter();
        for &is_null in &page.nulls {
            if is_null {
                out.push(None);
                continue;
            }
            let mut v = values
                .next()
                .expect("value count checked against the null bitmap");
            if is_char {
                if let Some(nul) = v.iter().position(|b| *b == 0) {
                    v.truncate(nul);
                }
            }
            out.push(Some(v));
        }
    }
    Ok(ColumnValues::Bytes(out))
}
