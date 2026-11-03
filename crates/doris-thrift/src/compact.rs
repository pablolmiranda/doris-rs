//! `TCompactProtocol` output that writes field headers the way the Java FE and the C++ BE do.
//!
//! A field header uses the one-byte delta form when the field id is 1 to 15 above the previous
//! one. The `thrift` crate (0.25) stops at 14 and writes a delta of 15 in the long form, which
//! every reader accepts but which is not the reference's bytes. This protocol keeps its own field
//! id bookkeeping for headers and passes everything else to the crate's protocol.
use thrift::protocol::{
    TCompactOutputProtocol, TFieldIdentifier, TListIdentifier, TMapIdentifier, TMessageIdentifier,
    TOutputProtocol, TSetIdentifier, TStructIdentifier, TType,
};
use thrift::transport::TWriteTransport;

pub struct CompactOutputProtocol<T: TWriteTransport> {
    inner: TCompactOutputProtocol<T>,
    last_field_id: i16,
    /// Previous structs' last field ids, restored as each nested struct ends.
    field_ids: Vec<i16>,
    /// A bool field's id: bool fields carry their value in the header's type nibble.
    pending_bool: Option<i16>,
}

impl<T: TWriteTransport> CompactOutputProtocol<T> {
    pub fn new(transport: T) -> Self {
        Self {
            inner: TCompactOutputProtocol::new(transport),
            last_field_id: 0,
            field_ids: Vec::new(),
            pending_bool: None,
        }
    }

    fn write_field_header(&mut self, type_nibble: u8, field_id: i16) -> thrift::Result<()> {
        let delta = i32::from(field_id) - i32::from(self.last_field_id);
        if (1..=15).contains(&delta) {
            self.inner.write_byte(((delta as u8) << 4) | type_nibble)?;
        } else {
            self.inner.write_byte(type_nibble)?;
            self.inner.write_i16(field_id)?;
        }
        self.last_field_id = field_id;
        Ok(())
    }
}

/// Compact protocol type nibble of a non-bool field.
fn type_nibble(t: TType) -> thrift::Result<u8> {
    Ok(match t {
        TType::I08 => 0x03,
        TType::I16 => 0x04,
        TType::I32 => 0x05,
        TType::I64 => 0x06,
        TType::Double => 0x07,
        TType::String => 0x08,
        TType::List => 0x09,
        TType::Set => 0x0A,
        TType::Map => 0x0B,
        TType::Struct => 0x0C,
        TType::Uuid => 0x0D,
        other => {
            return Err(thrift::Error::Protocol(thrift::ProtocolError::new(
                thrift::ProtocolErrorKind::InvalidData,
                format!("no compact field type for {other:?}"),
            )))
        }
    })
}

impl<T: TWriteTransport> TOutputProtocol for CompactOutputProtocol<T> {
    fn write_message_begin(&mut self, identifier: &TMessageIdentifier) -> thrift::Result<()> {
        self.inner.write_message_begin(identifier)
    }
    fn write_message_end(&mut self) -> thrift::Result<()> {
        self.inner.write_message_end()
    }
    fn write_struct_begin(&mut self, identifier: &TStructIdentifier) -> thrift::Result<()> {
        self.field_ids.push(self.last_field_id);
        self.last_field_id = 0;
        self.inner.write_struct_begin(identifier)
    }
    fn write_struct_end(&mut self) -> thrift::Result<()> {
        self.last_field_id = self.field_ids.pop().unwrap_or(0);
        self.inner.write_struct_end()
    }
    fn write_field_begin(&mut self, identifier: &TFieldIdentifier) -> thrift::Result<()> {
        let id = identifier.id.unwrap_or(0);
        if identifier.field_type == TType::Bool {
            self.pending_bool = Some(id);
            Ok(())
        } else {
            self.write_field_header(type_nibble(identifier.field_type)?, id)
        }
    }
    fn write_field_end(&mut self) -> thrift::Result<()> {
        Ok(())
    }
    fn write_field_stop(&mut self) -> thrift::Result<()> {
        self.inner.write_field_stop()
    }
    fn write_bool(&mut self, b: bool) -> thrift::Result<()> {
        match self.pending_bool.take() {
            Some(id) => self.write_field_header(if b { 0x01 } else { 0x02 }, id),
            None => self.inner.write_bool(b),
        }
    }
    fn write_bytes(&mut self, b: &[u8]) -> thrift::Result<()> {
        self.inner.write_bytes(b)
    }
    fn write_i8(&mut self, i: i8) -> thrift::Result<()> {
        self.inner.write_i8(i)
    }
    fn write_i16(&mut self, i: i16) -> thrift::Result<()> {
        self.inner.write_i16(i)
    }
    fn write_i32(&mut self, i: i32) -> thrift::Result<()> {
        self.inner.write_i32(i)
    }
    fn write_i64(&mut self, i: i64) -> thrift::Result<()> {
        self.inner.write_i64(i)
    }
    fn write_double(&mut self, d: f64) -> thrift::Result<()> {
        self.inner.write_double(d)
    }
    fn write_uuid(&mut self, uuid: &uuid::Uuid) -> thrift::Result<()> {
        self.inner.write_uuid(uuid)
    }
    fn write_string(&mut self, s: &str) -> thrift::Result<()> {
        self.inner.write_string(s)
    }
    fn write_list_begin(&mut self, identifier: &TListIdentifier) -> thrift::Result<()> {
        self.inner.write_list_begin(identifier)
    }
    fn write_list_end(&mut self) -> thrift::Result<()> {
        self.inner.write_list_end()
    }
    fn write_set_begin(&mut self, identifier: &TSetIdentifier) -> thrift::Result<()> {
        self.inner.write_set_begin(identifier)
    }
    fn write_set_end(&mut self) -> thrift::Result<()> {
        self.inner.write_set_end()
    }
    fn write_map_begin(&mut self, identifier: &TMapIdentifier) -> thrift::Result<()> {
        self.inner.write_map_begin(identifier)
    }
    fn write_map_end(&mut self) -> thrift::Result<()> {
        self.inner.write_map_end()
    }
    fn flush(&mut self) -> thrift::Result<()> {
        self.inner.flush()
    }
    fn write_byte(&mut self, b: u8) -> thrift::Result<()> {
        self.inner.write_byte(b)
    }
}
