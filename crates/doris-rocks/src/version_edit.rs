//! Version edits (`db/version_edit.cc`): the records of a manifest. Each edit applies to one
//! column family (`column_family`, 0 when absent) and may add or drop it, add or delete table
//! files, and set the database-wide numbers. Encoding follows `VersionEdit::EncodeTo`, decoding
//! `VersionEdit::DecodeFrom`.

use crate::coding::{put_fixed64, put_length_prefixed, put_varint32, put_varint64, Reader};

const COMPARATOR: u32 = 1;
const LOG_NUMBER: u32 = 2;
const NEXT_FILE_NUMBER: u32 = 3;
const LAST_SEQUENCE: u32 = 4;
const COMPACT_POINTER: u32 = 5;
const DELETED_FILE: u32 = 6;
const NEW_FILE: u32 = 7;
const PREV_LOG_NUMBER: u32 = 9;
const MIN_LOG_NUMBER_TO_KEEP: u32 = 10;
const NEW_FILE2: u32 = 100;
const NEW_FILE3: u32 = 102;
const NEW_FILE4: u32 = 103;
const COLUMN_FAMILY: u32 = 200;
const COLUMN_FAMILY_ADD: u32 = 201;
const COLUMN_FAMILY_DROP: u32 = 202;
const MAX_COLUMN_FAMILY: u32 = 203;

// CustomTag, for kNewFile4's fields
const TERMINATE: u32 = 1;
const NEED_COMPACTION: u32 = 2;
const MIN_LOG_NUMBER_TO_KEEP_HACK: u32 = 3;
const PATH_ID: u32 = 65;
const NON_SAFE_IGNORE_MASK: u32 = 1 << 6;

/// A table file added at a level. `smallest` and `largest` are encoded internal keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewFile {
    pub level: u32,
    pub number: u64,
    pub path_id: u32,
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub smallest_seqno: u64,
    pub largest_seqno: u64,
    pub marked_for_compaction: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    pub comparator: Option<String>,
    pub log_number: Option<u64>,
    pub prev_log_number: Option<u64>,
    pub next_file_number: Option<u64>,
    pub last_sequence: Option<u64>,
    pub max_column_family: Option<u32>,
    /// Encoded, as 5.14.2 does, only inside the first new file's custom fields.
    pub min_log_number_to_keep: Option<u64>,
    pub column_family: u32,
    pub column_family_add: Option<String>,
    pub column_family_drop: bool,
    /// (level, file number)
    pub deleted_files: Vec<(u32, u64)>,
    pub new_files: Vec<NewFile>,
}

impl VersionEdit {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let tag64 = |out: &mut Vec<u8>, tag: u32, v: u64| {
            put_varint32(out, tag);
            put_varint64(out, v);
        };
        if let Some(c) = &self.comparator {
            put_varint32(&mut out, COMPARATOR);
            put_length_prefixed(&mut out, c.as_bytes());
        }
        if let Some(v) = self.log_number {
            tag64(&mut out, LOG_NUMBER, v);
        }
        if let Some(v) = self.prev_log_number {
            tag64(&mut out, PREV_LOG_NUMBER, v);
        }
        if let Some(v) = self.next_file_number {
            tag64(&mut out, NEXT_FILE_NUMBER, v);
        }
        if let Some(v) = self.last_sequence {
            tag64(&mut out, LAST_SEQUENCE, v);
        }
        if let Some(v) = self.max_column_family {
            put_varint32(&mut out, MAX_COLUMN_FAMILY);
            put_varint32(&mut out, v);
        }
        for (level, number) in &self.deleted_files {
            put_varint32(&mut out, DELETED_FILE);
            put_varint32(&mut out, *level);
            put_varint64(&mut out, *number);
        }
        let mut min_log_written = false;
        for f in &self.new_files {
            let custom = f.marked_for_compaction || self.min_log_number_to_keep.is_some();
            if custom {
                put_varint32(&mut out, NEW_FILE4);
            } else if f.path_id == 0 {
                put_varint32(&mut out, NEW_FILE2);
            } else {
                put_varint32(&mut out, NEW_FILE3);
            }
            put_varint32(&mut out, f.level);
            put_varint64(&mut out, f.number);
            if f.path_id != 0 && !custom {
                put_varint32(&mut out, f.path_id);
            }
            put_varint64(&mut out, f.size);
            put_length_prefixed(&mut out, &f.smallest);
            put_length_prefixed(&mut out, &f.largest);
            put_varint64(&mut out, f.smallest_seqno);
            put_varint64(&mut out, f.largest_seqno);
            if custom {
                if f.path_id != 0 {
                    put_varint32(&mut out, PATH_ID);
                    put_length_prefixed(&mut out, &[f.path_id as u8]);
                }
                if f.marked_for_compaction {
                    put_varint32(&mut out, NEED_COMPACTION);
                    put_length_prefixed(&mut out, &[1]);
                }
                if let (Some(v), false) = (self.min_log_number_to_keep, min_log_written) {
                    put_varint32(&mut out, MIN_LOG_NUMBER_TO_KEEP_HACK);
                    let mut b = Vec::new();
                    put_fixed64(&mut b, v);
                    put_length_prefixed(&mut out, &b);
                    min_log_written = true;
                }
                put_varint32(&mut out, TERMINATE);
            }
        }
        if self.column_family != 0 {
            put_varint32(&mut out, COLUMN_FAMILY);
            put_varint32(&mut out, self.column_family);
        }
        if let Some(name) = &self.column_family_add {
            put_varint32(&mut out, COLUMN_FAMILY_ADD);
            put_length_prefixed(&mut out, name.as_bytes());
        }
        if self.column_family_drop {
            put_varint32(&mut out, COLUMN_FAMILY_DROP);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<VersionEdit, String> {
        let mut r = Reader::new(bytes);
        let mut e = VersionEdit::default();
        let bad = |what: &str| format!("version edit: bad {what}");
        let utf8 = |b: &[u8], what: &str| String::from_utf8(b.to_vec()).map_err(|_| bad(what));
        while !r.is_empty() {
            let tag = r.varint32().ok_or_else(|| bad("tag"))?;
            match tag {
                COMPARATOR => e.comparator = Some(utf8(r.length_prefixed().ok_or_else(|| bad("comparator"))?, "comparator")?),
                LOG_NUMBER => e.log_number = Some(r.varint64().ok_or_else(|| bad("log number"))?),
                PREV_LOG_NUMBER => e.prev_log_number = Some(r.varint64().ok_or_else(|| bad("previous log number"))?),
                NEXT_FILE_NUMBER => e.next_file_number = Some(r.varint64().ok_or_else(|| bad("next file number"))?),
                LAST_SEQUENCE => e.last_sequence = Some(r.varint64().ok_or_else(|| bad("last sequence"))?),
                MAX_COLUMN_FAMILY => e.max_column_family = Some(r.varint32().ok_or_else(|| bad("max column family"))?),
                MIN_LOG_NUMBER_TO_KEEP => {
                    e.min_log_number_to_keep = Some(r.varint64().ok_or_else(|| bad("min log number to keep"))?)
                }
                COMPACT_POINTER => {
                    r.varint32().ok_or_else(|| bad("compaction pointer"))?;
                    r.length_prefixed().ok_or_else(|| bad("compaction pointer"))?;
                }
                DELETED_FILE => {
                    let level = r.varint32().ok_or_else(|| bad("deleted file"))?;
                    let number = r.varint64().ok_or_else(|| bad("deleted file"))?;
                    e.deleted_files.push((level, number));
                }
                NEW_FILE | NEW_FILE2 | NEW_FILE3 | NEW_FILE4 => {
                    let level = r.varint32().ok_or_else(|| bad("new file"))?;
                    let number = r.varint64().ok_or_else(|| bad("new file"))?;
                    let path_id = if tag == NEW_FILE3 { r.varint32().ok_or_else(|| bad("new file"))? } else { 0 };
                    let size = r.varint64().ok_or_else(|| bad("new file"))?;
                    let smallest = r.length_prefixed().ok_or_else(|| bad("new file"))?.to_vec();
                    let largest = r.length_prefixed().ok_or_else(|| bad("new file"))?.to_vec();
                    let (smallest_seqno, largest_seqno) = if tag == NEW_FILE {
                        (0, 0)
                    } else {
                        (r.varint64().ok_or_else(|| bad("new file"))?, r.varint64().ok_or_else(|| bad("new file"))?)
                    };
                    let mut f = NewFile {
                        level,
                        number,
                        path_id,
                        size,
                        smallest,
                        largest,
                        smallest_seqno,
                        largest_seqno,
                        marked_for_compaction: false,
                    };
                    if tag == NEW_FILE4 {
                        loop {
                            let custom = r.varint32().ok_or_else(|| bad("new file field"))?;
                            if custom == TERMINATE {
                                break;
                            }
                            let field = r.length_prefixed().ok_or_else(|| bad("new file field"))?;
                            match custom {
                                PATH_ID => f.path_id = u32::from(*field.first().ok_or_else(|| bad("path id"))?),
                                NEED_COMPACTION => f.marked_for_compaction = field.first() == Some(&1),
                                MIN_LOG_NUMBER_TO_KEEP_HACK => {
                                    e.min_log_number_to_keep =
                                        Some(Reader::new(field).fixed64().ok_or_else(|| bad("min log number to keep"))?)
                                }
                                other if other & NON_SAFE_IGNORE_MASK != 0 => {
                                    return Err(format!("version edit: new file field {other} must be understood"))
                                }
                                _ => {}
                            }
                        }
                    }
                    e.new_files.push(f);
                }
                COLUMN_FAMILY => e.column_family = r.varint32().ok_or_else(|| bad("column family"))?,
                COLUMN_FAMILY_ADD => {
                    e.column_family_add =
                        Some(utf8(r.length_prefixed().ok_or_else(|| bad("column family name"))?, "column family name")?)
                }
                COLUMN_FAMILY_DROP => e.column_family_drop = true,
                other => return Err(format!("version edit: unknown tag {other}")),
            }
        }
        Ok(e)
    }
}
