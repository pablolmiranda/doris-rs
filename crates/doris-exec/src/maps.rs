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
        // a nested key: lookups by it would not compare as C++'s compare_at
        "map" => to.map_types().is_some_and(|(k, _)| !k.is_nested()) && args.len().is_multiple_of(2),
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

/// `IDataType::get_name` with nullability: a nullable part as `Nullable(..)`.
fn cpp_name(t: &ColType) -> String {
    let part = |c: &ColType| if c.nullable { format!("Nullable({})", cpp_name(c)) } else { cpp_name(c) };
    match t.ptype {
        T::ARRAY => format!("Array({})", t.element().map(|e| part(&e)).unwrap_or_default()),
        T::MAP => t
            .map_types()
            .map_or_else(String::new, |(k, v)| format!("Map({}, {})", part(&k), part(&v))),
        T::STRUCT => {
            let f: Vec<String> = t.fields().unwrap_or_default().iter().map(|(n, c)| format!("{n}:{}", part(c))).collect();
            format!("Struct({})", f.join(", "))
        }
        _ => t.type_name(),
    }
}

/// `DataTypeStruct::get_name`.
fn struct_name(t: &ColType) -> String {
    cpp_name(t)
}

/// `element_at(struct, name | index)` at node `i`, checked as C++ works out its return type
/// (`function_array_element.h:84-100,176-200`): the constant name or index must name a field,
/// and the call's type must be that field's.
pub fn check_struct_element(nodes: &[doris_thrift::gen::exprs::TExprNode], i: usize) -> Result<(), ExecError> {
    use doris_thrift::gen::exprs::TExprNodeType as N;
    let ty = |at: usize| nodes.get(at).and_then(|n| ColType::from_type_desc("", &n.type_, true));
    let first = i + 1;
    let second = crate::aggregate::subtree_end(nodes, first)?;
    let (Some(st), Some(to)) = (ty(first), ty(i)) else {
        return Err(ExecError::BadRequest("element_at without its types".into()));
    };
    let fields = st.fields().unwrap_or_default();
    let runtime = |msg: String| ExecError::Coded {
        code: doris_thrift::gen::status::TStatusCode::RUNTIME_ERROR.0,
        name: "RUNTIME_ERROR",
        msg,
    };
    let key = &nodes[second];
    let at = match (key.node_type, key.string_literal.as_ref(), key.int_literal.as_ref()) {
        (N::STRING_LITERAL, Some(s), _) => fields.iter().position(|(f, _)| f.eq_ignore_ascii_case(&s.value)).ok_or_else(|| {
            runtime(format!(
                "Element not found for function element_at: name {} not found in {}.",
                s.value,
                struct_name(&st)
            ))
        })?,
        (N::INT_LITERAL, _, Some(v)) => {
            let limit = fields.len() as i64 + 1;
            if v.value < 1 || v.value >= limit {
                return Err(runtime(format!(
                    "Index out of bound for function element_at: index {} should base from 1 and less than {limit}.",
                    v.value
                )));
            }
            (v.value - 1) as usize
        }
        _ => {
            return Err(ExecError::Unsupported {
                what: "element_at on a STRUCT by a name that is not a constant".into(),
            })
        }
    };
    let (name, ft) = &fields[at];
    let same = ft.ptype == to.ptype && ft.scale == to.scale && (!ft.is_nested() || ft.children == to.children);
    if !same {
        return Err(ExecError::Unsupported {
            what: format!("element_at of field {name} ({}) as {}", ft.type_name(), to.type_name()),
        });
    }
    Ok(())
}
