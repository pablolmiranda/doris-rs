# Changes applied to the compiler output

`regen.sh` runs Thrift 0.24.0 (`--gen rs --allow-64bit-consts -strict`, the flags upstream uses
for its C++ and Java targets) over `idl/`, then `postprocess.py` applies these changes. Each one
checks the shape it expects and stops regeneration if the compiler output differs.

1. **Module paths.** The compiler writes modules for the crate root (`use crate::types;`); they
   live under `src/gen/`, so every `use crate::<module>;` becomes `use crate::gen::<module>;`.
2. **`parquet.rs`, `FileMetaData.column_orders`.** For a list of unions the generator pushes
   `Box::new(elem)` into a `Vec<ColumnOrder>`, which does not compile; the patch pushes `elem`.
   Exactly one occurrence is expected.
3. **Per-method structs.** Each service method's `<Service><Method>Args` and
   `<Service><Method>Result` structs, their fields and their inherent methods are private; they
   become `pub` so `gen::methods` (and callers) can decode and encode call and reply bodies.
4. **`runtime_profile.rs`, `TRuntimeProfileNode.info_strings`.** Its values are `Vec<u8>`, not
   `String`: the BE writes raw key bytes into profile info strings (a scan's key range reads
   `ScanKeys:ScanKey=[null : \xff]`), and the generated `String` rejects bytes that are not
   UTF-8. Found by the Epic 1 traffic corpus; no other string field carried such bytes there.

`postprocess.py` also writes `src/gen/mod.rs` (one `pub mod` per IDL file, generated-code lints
allowed) and `src/gen/methods.rs`, the table of every method: the wire name each processor
matches on and the `*Args`/`*Result` structs its process function uses.
