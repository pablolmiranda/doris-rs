//! `regexp` / `rlike` (`FunctionRegexpLike`, `like.cpp`).
//!
//! C++ first tries five literal classifiers. A constant pattern they don't fit goes to Hyperscan;
//! a per-row pattern goes to RE2. Either falls back to Boost when RE2 rejects the pattern and
//! `enable_extended_regex` is on. doris-rs matches the RE2 and Hyperscan paths with `regex`, behind
//! a gate over `regex-syntax`'s AST that accepts only what the three engines read alike and rewrites
//! the Perl classes to their ASCII forms. The Boost path uses `fancy-regex` behind a narrower gate.
//! Everything else is refused, naming it.
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::OnceLock;

use doris_segment::writer::Datum;
use regex::Regex as TextRegex;
use regex_syntax::ast::{self, Ast};

use crate::ExecError;

/// The query options `regexp` reads (`RuntimeState`: `enable_extended_regex`, `enable_hyperscan_fallback`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegexOptions {
    pub extended: bool,
    pub hyperscan_fallback: bool,
}

impl Default for RegexOptions {
    fn default() -> Self {
        RegexOptions {
            extended: false,
            hyperscan_fallback: true,
        }
    }
}

impl RegexOptions {
    /// From a query's or load's options (`runtime_state.h`: extended only when set and true).
    pub fn from_query_options(o: Option<&doris_thrift::gen::palo_internal_service::TQueryOptions>) -> RegexOptions {
        RegexOptions {
            extended: o.and_then(|o| o.enable_extended_regex) == Some(true),
            hyperscan_fallback: o.and_then(|o| o.enable_hyperscan_fallback).unwrap_or(true),
        }
    }
}

thread_local! {
    static OPTIONS: Cell<RegexOptions> = Cell::new(RegexOptions::default());
}

/// Runs `f` with `o` as this thread's regexp options.
pub fn with_regex_options<R>(o: RegexOptions, f: impl FnOnce() -> R) -> R {
    struct Restore(RegexOptions);
    impl Drop for Restore {
        fn drop(&mut self) {
            OPTIONS.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(OPTIONS.with(|c| c.replace(o)));
    f()
}

/// This thread's regexp options.
pub fn options() -> RegexOptions {
    OPTIONS.with(Cell::get)
}

/// A pattern C++'s classifiers turn into a literal match (`like.cpp:41-54`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Class {
    AllPass,
    Equals(Vec<u8>),
    StartsWith(Vec<u8>),
    EndsWith(Vec<u8>),
    Substring(Vec<u8>),
}

struct Classifiers {
    allpass: TextRegex,
    equals: TextRegex,
    starts: TextRegex,
    ends: TextRegex,
    substring: TextRegex,
}

fn classifiers() -> &'static Classifiers {
    static C: OnceLock<Classifiers> = OnceLock::new();
    C.get_or_init(|| {
        const LIT: &str = r"([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)";
        // RE2::FullMatch: anchored at both ends
        let full = |p: String| TextRegex::new(&format!("^(?:{p})$")).expect("classifier");
        Classifiers {
            allpass: full(r"(\.\*)+".into()),
            equals: full(format!(r"\^{LIT}\$")),
            starts: full(format!(r"\^{LIT}(?:\.\*)*")),
            ends: full(format!(r"(?:\.\*)*{LIT}\$")),
            substring: full(format!(r"(?:\.\*)*{LIT}(?:\.\*)*")),
        }
    })
}

/// The literal class of a pattern, in `FunctionRegexpLike::open`'s order; `None` when the pattern
/// needs a regex engine (or is not UTF-8, which the UTF-8 classifiers never match).
pub fn classify(p: &[u8]) -> Option<Class> {
    let p = std::str::from_utf8(p).ok()?;
    let c = classifiers();
    let lit = |re: &TextRegex| re.captures(p).map(|m| m.get(1).map_or(vec![], |g| g.as_str().as_bytes().to_vec()));
    if c.allpass.is_match(p) {
        return Some(Class::AllPass);
    }
    if let Some(l) = lit(&c.equals) {
        return Some(Class::Equals(l));
    }
    if let Some(l) = lit(&c.starts) {
        return Some(Class::StartsWith(l));
    }
    if let Some(l) = lit(&c.ends) {
        return Some(Class::EndsWith(l));
    }
    lit(&c.substring).map(Class::Substring)
}

/// Which engine's syntax the gate checks against.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// a constant pattern: Hyperscan (PCRE syntax, ASCII Perl classes, `\s` unverified)
    Hyperscan,
    /// a per-row pattern, or Hyperscan's fallback: RE2
    Re2,
}

/// What the gate makes of a pattern.
enum Gate {
    /// a `regex` pattern with the same meaning; whether it holds a `$` (Hyperscan's `$` also
    /// matches before a final newline); the largest bounded repetition count
    Ok { pattern: String, dollar: bool, max_count: u32 },
    /// RE2 rejects it: with RE2's error text when it is known
    Re2Rejects(Option<String>),
    /// outside what doris-rs can match exactly
    Refuse(String),
}

fn refuse(what: impl Into<String>) -> ExecError {
    ExecError::Unsupported { what: what.into() }
}

struct Walk<'p> {
    pattern: &'p str,
    /// the product of the counted repetitions enclosing the current node (RE2's RepetitionWalker)
    nesting: u64,
    mode: Mode,
    edits: Vec<(usize, usize, String)>,
    dollar: bool,
    max_count: u32,
    /// the first RE2 error found, as (offset, text)
    re2: Option<(usize, String)>,
    refused: Option<String>,
}

const DIGIT: &str = "0-9";
const WORD: &str = "0-9A-Za-z_";
// RE2's \s: no \v
const SPACE: &str = r"\t\n\f\r ";

impl Walk<'_> {
    fn refuse(&mut self, what: &str) {
        self.refused.get_or_insert_with(|| what.to_string());
    }

    fn perl(&mut self, p: &ast::ClassPerl) -> Option<&'static str> {
        match p.kind {
            ast::ClassPerlKind::Digit => Some(DIGIT),
            ast::ClassPerlKind::Word => Some(WORD),
            ast::ClassPerlKind::Space if self.mode == Mode::Re2 => Some(SPACE),
            ast::ClassPerlKind::Space => {
                self.refuse(r"\s in a constant regexp pattern");
                None
            }
        }
    }

    fn literal(&mut self, l: &ast::Literal) {
        use ast::LiteralKind as K;
        use ast::SpecialLiteralKind as S;
        let ok = match &l.kind {
            K::Verbatim | K::Meta => true,
            K::Superfluous => l.c.is_ascii_punctuation(),
            K::HexFixed(ast::HexLiteralKind::X) | K::HexBrace(ast::HexLiteralKind::X) => true,
            K::Special(S::Tab | S::LineFeed | S::CarriageReturn | S::FormFeed) => true,
            // Hyperscan's \v is PCRE's vertical-whitespace class; RE2's is VT alone
            K::Special(S::VerticalTab) => self.mode == Mode::Re2,
            _ => false,
        };
        if !ok {
            self.refuse(&format!("the regexp escape {}", l.c.escape_default()));
        }
    }

    fn set_item(&mut self, i: &ast::ClassSetItem) {
        use ast::ClassSetItem as I;
        match i {
            I::Empty(_) | I::Ascii(_) => {}
            I::Literal(l) => self.literal(l),
            I::Range(r) => {
                self.literal(&r.start);
                self.literal(&r.end);
            }
            I::Perl(p) if !p.negated => {
                if let Some(set) = self.perl(p) {
                    self.edits.push((p.span.start.offset, p.span.end.offset, set.to_string()));
                }
            }
            I::Union(u) => u.items.iter().for_each(|i| self.set_item(i)),
            _ => self.refuse("a regexp class inside a class"),
        }
    }

    fn walk(&mut self, a: &Ast) {
        match a {
            Ast::Empty(_) | Ast::Dot(_) => {}
            Ast::Literal(l) => self.literal(l),
            Ast::Flags(_) => self.refuse("regexp flags"),
            Ast::Assertion(x) => {
                use ast::AssertionKind as K;
                let s = &x.span;
                match x.kind {
                    K::StartLine | K::StartText | K::EndText => {}
                    K::EndLine => self.dollar = true,
                    K::WordBoundary => self.edits.push((s.start.offset, s.end.offset, r"(?-u:\b)".into())),
                    K::NotWordBoundary => self.edits.push((s.start.offset, s.end.offset, r"(?-u:\B)".into())),
                    _ => self.refuse("a regexp word-boundary form"),
                }
            }
            Ast::ClassUnicode(_) => self.refuse(r"\p in a regexp"),
            Ast::ClassPerl(p) => {
                if let Some(set) = self.perl(p) {
                    let neg = if p.negated { "^" } else { "" };
                    self.edits.push((p.span.start.offset, p.span.end.offset, format!("[{neg}{set}]")));
                }
            }
            Ast::ClassBracketed(c) => match &c.kind {
                ast::ClassSet::Item(i) => self.set_item(i),
                ast::ClassSet::BinaryOp(_) => self.refuse("a regexp class operation"),
            },
            Ast::Repetition(r) => {
                // regex-syntax reads `a**`, `a*+`, `x{2}{3}` as nested repetitions; RE2 rejects
                // them, and Boost reads `*+` as possessive
                if matches!(*r.ast, Ast::Repetition(_)) {
                    self.refuse("a regexp quantifier on a quantifier");
                }
                let op = &self.pattern[r.op.span.start.offset..r.op.span.end.offset];
                // regex-syntax allows `{ 2 }`; RE2 and Hyperscan read it as literal text
                if op.chars().any(char::is_whitespace) {
                    self.refuse("whitespace inside a regexp repetition count");
                }
                let outer = self.nesting;
                if let ast::RepetitionKind::Range(range) = &r.op.kind {
                    let n = match *range {
                        ast::RepetitionRange::Exactly(n) | ast::RepetitionRange::AtLeast(n) => n,
                        ast::RepetitionRange::Bounded(a, b) => a.max(b),
                    };
                    if n > 1000 {
                        // RE2's kMaxRepeat: `bad repetition operator`, text not verified
                        self.re2.get_or_insert((r.op.span.start.offset, String::new()));
                    }
                    self.max_count = self.max_count.max(n);
                    // RE2 rejects nested counted repetitions whose product passes kMaxRepeat
                    self.nesting = self.nesting.saturating_mul(u64::from(n.max(1)));
                    if self.nesting > 1000 {
                        self.refuse("nested regexp repetitions past 1000");
                    }
                }
                self.walk(&r.ast);
                self.nesting = outer;
            }
            Ast::Group(g) => {
                match &g.kind {
                    ast::GroupKind::CaptureIndex(_) => {}
                    ast::GroupKind::CaptureName { starts_with_p: true, .. } => {}
                    // RE2 2021 reads only `(?P<name>`
                    ast::GroupKind::CaptureName { starts_with_p: false, .. } => {
                        if self.mode == Mode::Hyperscan {
                            self.refuse("a regexp named group (?<name>");
                        }
                        self.re2.get_or_insert((g.span.start.offset, "invalid perl operator: (?<".into()));
                    }
                    ast::GroupKind::NonCapturing(f) if f.items.is_empty() => {}
                    ast::GroupKind::NonCapturing(_) => self.refuse("regexp flags"),
                }
                self.walk(&g.ast);
            }
            Ast::Alternation(x) => x.asts.iter().for_each(|a| self.walk(a)),
            Ast::Concat(x) => x.asts.iter().for_each(|a| self.walk(a)),
        }
    }
}

fn gate(p: &str, mode: Mode) -> Gate {
    let parsed = match ast::parse::Parser::new().parse(p) {
        Ok(a) => a,
        Err(e) => {
            use ast::ErrorKind as K;
            return match e.kind() {
                // RE2's ParsePerlFlags: the bad operator is `(?` and the character after it
                K::UnsupportedLookAround => {
                    let at = e.span().start.offset;
                    let op: String = p[at..].chars().take(3).collect();
                    Gate::Re2Rejects(Some(format!("invalid perl operator: {op}")))
                }
                K::GroupUnclosed | K::GroupUnopened | K::ClassUnclosed | K::RepetitionMissing | K::UnsupportedBackreference => Gate::Re2Rejects(None),
                _ => Gate::Refuse(format!("the regexp pattern {p}")),
            };
        }
    };
    let mut w = Walk {
        pattern: p,
        nesting: 1,
        mode,
        edits: vec![],
        dollar: false,
        max_count: 0,
        re2: None,
        refused: None,
    };
    w.walk(&parsed);
    if let Some(what) = w.refused {
        return Gate::Refuse(what);
    }
    if let Some((_, text)) = w.re2 {
        return Gate::Re2Rejects((!text.is_empty()).then_some(text));
    }
    let mut pattern = p.to_string();
    w.edits.sort_by_key(|e| std::cmp::Reverse(e.0));
    for (start, end, with) in w.edits {
        pattern.replace_range(start..end, &with);
    }
    Gate::Ok {
        pattern,
        dollar: w.dollar,
        max_count: w.max_count,
    }
}

/// A compiled pattern.
enum Matcher {
    Lit(Class),
    /// RE2 or Hyperscan semantics; `hs_dollar`: a Hyperscan pattern with a `$`
    Re {
        re: regex::bytes::Regex,
        hs_dollar: bool,
    },
    /// Boost; `anchors`: the pattern has `^` or `$`, which Boost reads per line
    Boost {
        re: fancy_regex::Regex,
        anchors: bool,
    },
}

fn build(pattern: &str) -> Result<regex::bytes::Regex, ExecError> {
    // RE2 with dot_nl, Hyperscan with HS_FLAG_DOTALL; UTF-8 in both
    regex::bytes::RegexBuilder::new(pattern)
        .dot_matches_new_line(true)
        .unicode(true)
        .build()
        .map_err(|_| refuse(format!("the regexp pattern {pattern}")))
}

fn invalid(p: &str, error: &str, hint: bool) -> ExecError {
    let hint = if hint {
        ". If you need advanced regex features, try setting enable_extended_regex=true"
    } else {
        ""
    };
    ExecError::Internal(format!("Invalid regex expression: {p}. Error: {error}{hint}"))
}

/// Whether a group with an unbounded quantifier inside is itself repeated without bound:
/// Boost's backtracking budget decides those, and doris-rs cannot tell where it runs out.
fn nested_unbounded(p: &str) -> bool {
    let b = p.as_bytes();
    // per open group: whether an unbounded quantifier appeared inside it
    let mut stack: Vec<bool> = vec![false];
    let unbounded_at = |i: usize| -> bool {
        match b.get(i) {
            Some(b'*' | b'+') => true,
            Some(b'{') => b[i..].iter().position(|&c| c == b'}').is_some_and(|e| b[i..i + e].ends_with(b",")),
            _ => false,
        }
    };
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 1,
            b'[' => {
                i += 1;
                while i < b.len() && b[i] != b']' {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'(' => stack.push(false),
            b')' => {
                let inner = stack.pop().unwrap_or(false);
                if inner && unbounded_at(i + 1) {
                    return true;
                }
                if let Some(top) = stack.last_mut() {
                    *top |= inner;
                }
            }
            _ if unbounded_at(i) => {
                if let Some(top) = stack.last_mut() {
                    *top = true;
                }
            }
            _ => {}
        }
        i += 1;
    }
    false
}

/// A quantifier right after a quantifier (`*+`, `++`, `{2}*`: possessive or nested in Boost), or
/// whitespace inside a `{…}` count; a lazy `?` after a quantifier is one operator.
fn stacked_or_spaced(p: &str) -> bool {
    let b = p.as_bytes();
    let mut i = 0;
    let mut after_quantifier = false;
    while i < b.len() {
        match b[i] {
            b'\\' => {
                i += 2;
                after_quantifier = false;
                continue;
            }
            b'[' => {
                i += 1;
                while i < b.len() && b[i] != b']' {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
                after_quantifier = false;
            }
            b'*' | b'+' if after_quantifier => return true,
            b'{' => {
                let end = b[i..].iter().position(|&c| c == b'}').map(|e| i + e);
                if let Some(e) = end.filter(|&e| b[i + 1..e].iter().all(|c| c.is_ascii_digit() || *c == b',' || c.is_ascii_whitespace())) {
                    if after_quantifier || b[i + 1..e].iter().any(u8::is_ascii_whitespace) {
                        return true;
                    }
                    i = e;
                    after_quantifier = true;
                    i += 1;
                    continue;
                }
                after_quantifier = false;
            }
            b'*' | b'+' => after_quantifier = true,
            // `x??` and `x*?` are lazy, one operator; a `?` after anything else is a quantifier
            b'?' => after_quantifier = !after_quantifier,
            _ => after_quantifier = false,
        }
        i += 1;
    }
    false
}

/// RE2 rejected `p` and extended regex is on: Boost.
fn boost(p: &str) -> Result<Matcher, ExecError> {
    if !p.is_ascii() {
        return Err(refuse("a Boost regexp with non-ASCII text"));
    }
    if let Err(e) = ast::parse::Parser::new().parse(p) {
        if *e.kind() == ast::ErrorKind::GroupUnclosed {
            let error = format!("Unmatched marking parenthesis ( or \\(.  The error occurred while parsing the regular expression: '{p}>>>HERE>>>'.");
            return Err(invalid(p, &error, false));
        }
    }
    if stacked_or_spaced(p) {
        return Err(refuse("a Boost regexp with a possessive quantifier or a spaced count"));
    }
    if nested_unbounded(p) {
        return Err(refuse("a Boost regexp with a nested unbounded repetition"));
    }
    // Boost's Perl syntax: `.` matches a newline by default
    let re = fancy_regex::Regex::new(&format!("(?s){p}")).map_err(|_| refuse(format!("the Boost regexp {p}")))?;
    Ok(Matcher::Boost {
        re,
        anchors: p.contains(['^', '$']),
    })
}

fn compile(p: &[u8], constant: bool, o: RegexOptions) -> Result<Matcher, ExecError> {
    if let Some(c) = classify(p) {
        return Ok(Matcher::Lit(c));
    }
    let Ok(text) = std::str::from_utf8(p) else {
        return Err(refuse("a regexp pattern that is not UTF-8"));
    };
    let mut mode = Mode::Re2;
    if constant {
        if text.contains('\0') {
            // Hyperscan reads the pattern as a C string
            return Err(refuse("a NUL in a constant regexp pattern"));
        }
        if !o.hyperscan_fallback {
            return Err(refuse("regexp without enable_hyperscan_fallback"));
        }
        mode = Mode::Hyperscan;
    }
    match gate(text, mode) {
        // is_hyperscan_regexp_expensive: a count past 50 skips Hyperscan for RE2
        Gate::Ok { max_count, .. } if mode == Mode::Hyperscan && max_count > 50 => compile_re2(text, o),
        Gate::Ok { pattern, dollar, .. } => Ok(Matcher::Re {
            re: build(&pattern)?,
            hs_dollar: mode == Mode::Hyperscan && dollar,
        }),
        Gate::Re2Rejects(_) if o.extended => boost(text),
        Gate::Re2Rejects(Some(error)) => Err(invalid(text, &error, true)),
        Gate::Re2Rejects(None) => Err(refuse(format!("the RE2 error for {text}"))),
        Gate::Refuse(what) => Err(refuse(what)),
    }
}

fn compile_re2(text: &str, o: RegexOptions) -> Result<Matcher, ExecError> {
    match gate(text, Mode::Re2) {
        Gate::Ok { pattern, .. } => Ok(Matcher::Re {
            re: build(&pattern)?,
            hs_dollar: false,
        }),
        Gate::Re2Rejects(_) if o.extended => boost(text),
        Gate::Re2Rejects(Some(error)) => Err(invalid(text, &error, true)),
        Gate::Re2Rejects(None) => Err(refuse(format!("the RE2 error for {text}"))),
        Gate::Refuse(what) => Err(refuse(what)),
    }
}

impl Matcher {
    fn is_match(&self, v: &[u8]) -> Result<bool, ExecError> {
        Ok(match self {
            Matcher::Lit(Class::AllPass) => true,
            Matcher::Lit(Class::Equals(l)) => v == l.as_slice(),
            Matcher::Lit(Class::StartsWith(l)) => v.starts_with(l),
            Matcher::Lit(Class::EndsWith(l)) => v.ends_with(l),
            Matcher::Lit(Class::Substring(l)) => l.is_empty() || v.windows(l.len()).any(|w| w == l.as_slice()),
            Matcher::Re { re, hs_dollar } => {
                if *hs_dollar && v.ends_with(b"\n") {
                    return Err(refuse("a constant regexp with $ over a value ending in a newline"));
                }
                re.is_match(v)
            }
            Matcher::Boost { re, anchors } => {
                if !v.is_ascii() {
                    return Err(refuse("a Boost regexp over non-ASCII text"));
                }
                if *anchors && v.contains(&b'\n') {
                    return Err(refuse("a Boost regexp anchor over multi-line text"));
                }
                let text = std::str::from_utf8(v).expect("ascii");
                re.is_match(text).map_err(|_| refuse("a Boost regexp past its backtracking budget"))?
            }
        })
    }
}

fn bytes(d: &Datum) -> Option<&[u8]> {
    match d {
        Datum::Bytes(b) => Some(b),
        _ => None,
    }
}

/// `value REGEXP pattern` over a block. A constant pattern is compiled once (Hyperscan's path); a
/// per-row one row by row, NULL values included, as C++ compiles every row's nested pattern. A
/// NULL value or pattern gives NULL.
pub fn regexp_column(values: &[Datum], patterns: &[Datum], constant: bool, o: RegexOptions) -> Result<Vec<Datum>, ExecError> {
    let mut cache: HashMap<&[u8], Matcher> = HashMap::new();
    let mut out = Vec::with_capacity(values.len());
    for (v, p) in values.iter().zip(patterns) {
        // a NULL pattern's nested value is "", a substring class: nothing to compile
        let Some(p) = bytes(p) else {
            out.push(Datum::Null);
            continue;
        };
        if !cache.contains_key(p) {
            cache.insert(p, compile(p, constant, o)?);
        }
        out.push(match bytes(v) {
            Some(v) => Datum::Bool(cache[p].is_match(v)?),
            None => Datum::Null,
        });
    }
    Ok(out)
}
