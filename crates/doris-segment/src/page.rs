//! Page framing as written by `PageIO::compress_and_write_page`:
//! `body | PageFooterPB | footer_size: u32 LE | crc32c(body + footer + footer_size): u32 LE`.
//! The body is compressed with the column's codec when that saved space; a reader detects it
//! because the stored body length differs from `PageFooterPB.uncompressed_size`.
use std::io::Read;

use prost::Message;

use crate::error::Error;
use crate::footer::SegmentFile;
use crate::pb::segment_v2::{CompressionTypePb, PageFooterPb, PagePointerPb};

#[derive(Debug, Clone)]
pub struct Page {
    pub footer: PageFooterPb,
    pub body: Vec<u8>,
}

/// Verify, split, and (if needed) decompress one raw page.
pub fn decode_page(raw: &[u8], codec: CompressionTypePb) -> Result<Page, Error> {
    if raw.len() < 8 {
        return Err(Error::PageTooSmall { size: raw.len() });
    }
    let (content, crc_bytes) = raw.split_at(raw.len() - 4);
    let expected = u32::from_le_bytes(crc_bytes.try_into().expect("4 bytes"));
    let actual = crc32c::crc32c(content);
    if expected != actual {
        return Err(Error::PageChecksumMismatch { expected, actual });
    }
    let footer_size =
        u32::from_le_bytes(content[content.len() - 4..].try_into().expect("4 bytes")) as usize;
    if footer_size > content.len() - 4 {
        return Err(Error::Corrupt(format!(
            "page footer size {footer_size} exceeds page"
        )));
    }
    let body_end = content.len() - 4 - footer_size;
    let footer = PageFooterPb::decode(&content[body_end..content.len() - 4])?;
    let stored = &content[..body_end];
    let declared = footer
        .uncompressed_size
        .map_or(stored.len(), |n| n as usize);
    let body = if stored.len() == declared {
        stored.to_vec()
    } else {
        let out = decompress(codec, stored)?;
        if out.len() != declared {
            return Err(Error::PageSizeMismatch {
                expected: declared,
                actual: out.len(),
            });
        }
        out
    };
    Ok(Page { footer, body })
}

/// Read and decode the page a pointer refers to.
pub fn read_page(
    file: &SegmentFile,
    ptr: &PagePointerPb,
    codec: CompressionTypePb,
) -> Result<Page, Error> {
    let start = ptr.offset as usize;
    let end = start + ptr.size as usize;
    if end > file.bytes.len() || start > end {
        return Err(Error::PageOutOfBounds {
            offset: ptr.offset,
            size: ptr.size,
            len: file.bytes.len(),
        });
    }
    decode_page(&file.bytes[start..end], codec)
}

fn decompress(codec: CompressionTypePb, input: &[u8]) -> Result<Vec<u8>, Error> {
    match codec {
        CompressionTypePb::Lz4f => {
            let mut out = Vec::new();
            lz4_flex::frame::FrameDecoder::new(input)
                .read_to_end(&mut out)
                .map_err(|e| Error::Decompress(format!("lz4 frame: {e}")))?;
            Ok(out)
        }
        CompressionTypePb::Zstd => {
            zstd::decode_all(input).map_err(|e| Error::Decompress(format!("zstd: {e}")))
        }
        CompressionTypePb::NoCompression => Err(Error::Corrupt(
            "page body length differs from uncompressed_size but codec is NO_COMPRESSION".into(),
        )),
        other => Err(Error::UnsupportedCompression(other as i32)),
    }
}
