//! `map`, `struct`, `named_struct` and `element_at` on a MAP or STRUCT (`function_map.cpp`,
//! `function_struct.cpp`, `function_array_element.h`; research `2026-11-12-m6-s7b6-arrays.md`
//! §2). A MAP keeps its entries in order; a repeated key keeps only its last occurrence
//! (`ColumnMap::deduplicate_keys`).
use std::cmp::Ordering;

use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::eval::{coerce, compare};
use crate::value::ColType;
use crate::ExecError;

pub fn is_map_function(name: &str) -> bool {
    matches!(name, "map" | "struct" | "named_struct")
}

/// Whether doris-rs runs this constructor, by its signature.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    match name {
        "map" => to.ptype == T::MAP && args.len() % 2 == 0,
        "struct" => to.ptype == T::STRUCT && args.len() == to.children.len(),
        "named_struct" => to.ptype == T::STRUCT && args.len() == 2 * to.children.len() && args.iter().step_by(2).all(text),
        _ => false,
    }
}

/// A MAP's entries with each repeated key (NULLs equal, floats by their bits) kept only at its
/// last occurrence.
pub fn dedup(entries: Vec<(Datum, Datum)>) -> Vec<(Datum, Datum)> {
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| crate::set_op::row_key(std::slice::from_ref(k))).collect();
    entries
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !keys[i + 1..].contains(&keys[*i]))
        .map(|(_, e)| e)
        .collect()
}

/// The constructors' values, one per row; never NULL.
pub fn eval(name: &str, types: &[ColType], to: &ColType, args: &[&[Datum]], rows: usize) -> Result<Vec<Datum>, ExecError> {
    (0..rows)
        .map(|r| match name {
            "map" => {
                let (kt, vt) = to.map_types().unwrap_or_default();
                let entries = (0..args.len() / 2)
                    .map(|i| {
                        Ok((
                            coerce(args[2 * i][r].clone(), &types[2 * i], &kt)?,
                            coerce(args[2 * i + 1][r].clone(), &types[2 * i + 1], &vt)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, ExecError>>()?;
                Ok(Datum::Map(dedup(entries)))
            }
            _ => {
                // named_struct's values follow their names
                let (first, step) = if name == "named_struct" { (1, 2) } else { (0, 1) };
                let fields = to
                    .children
                    .iter()
                    .enumerate()
                    .map(|(i, ft)| {
                        let at = first + i * step;
                        coerce(args[at][r].clone(), &types[at], ft)
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Datum::Struct(fields))
            }
        })
        .collect()
}

/// `element_at(map, key)`: the value of the first entry whose key equals the probe (a NULL
/// probe finds a NULL key); NULL when none does or the map is NULL.
pub fn map_element(maps: &[Datum], mt: &ColType, probes: &[Datum], pt: &ColType, to: &ColType) -> Result<Vec<Datum>, ExecError> {
    let (kt, vt) = mt.map_types().unwrap_or_default();
    maps.iter()
        .zip(probes)
        .map(|(m, p)| {
            let Datum::Map(m) = m else { return Ok(Datum::Null) };
            let hit = m.iter().find(|(k, _)| match (k, p) {
                (Datum::Null, Datum::Null) => true,
                (Datum::Null, _) | (_, Datum::Null) => false,
                _ => compare(k, &kt, p, pt) == Some(Ordering::Equal),
            });
            hit.map_or(Ok(Datum::Null), |(_, v)| coerce(v.clone(), &vt, to))
        })
        .collect()
}

/// `element_at(struct, name)`: the field named so, case-insensitively; an unknown name fails as
/// C++ does (`function_array_element.h:193-200`).
pub fn struct_element(structs: &[Datum], st: &ColType, names: &[Datum], to: &ColType) -> Result<Vec<Datum>, ExecError> {
    let fields = st.fields().unwrap_or_default();
    structs
        .iter()
        .zip(names)
        .map(|(s, n)| {
            let name = match n {
                Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                _ => return Ok(Datum::Null),
            };
            let Some(i) = fields.iter().position(|(f, _)| f.eq_ignore_ascii_case(&name)) else {
                return Err(ExecError::Coded {
                    code: doris_thrift::gen::status::TStatusCode::RUNTIME_ERROR.0,
                    name: "RUNTIME_ERROR",
                    msg: format!("Element not found for function element_at: name {name} not found in {}.", struct_name(st)),
                });
            };
            match s {
                Datum::Struct(v) => coerce(v.get(i).cloned().unwrap_or(Datum::Null), &fields[i].1, to),
                _ => Ok(Datum::Null),
            }
        })
        .collect()
}

/// `DataTypeStruct::get_name`: `Struct(name:type, ..)`, a nullable field's type as `Nullable(..)`.
fn struct_name(t: &ColType) -> String {
    let f: Vec<String> = t
        .fields()
        .unwrap_or_default()
        .iter()
        .map(|(n, ft)| {
            if ft.nullable {
                format!("{n}:Nullable({})", ft.type_name())
            } else {
                format!("{n}:{}", ft.type_name())
            }
        })
        .collect();
    format!("Struct({})", f.join(", "))
}
