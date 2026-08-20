# ADR 567 - OCTET_LENGTH, OVERLAY, TRANSLATE, and the LENGTH alias

**Status:** Proposed 2026-08-20.

## Context

The scalar registry holds forty-three functions; measured against the core
standard's string set, three remain missing — `OCTET_LENGTH`, `OVERLAY`,
`TRANSLATE` — plus `LENGTH`, the spelling nearly every arriving user tries
before `CHAR_LENGTH`. All four reuse existing registry shapes
(`StringToInteger` for the length, `REPLACE`'s three-string shape for
`TRANSLATE`, `SUBSTRING`'s literal-bound grammar convention for `OVERLAY`), so
per the registry's cost model this batch is validation of amortization, not
new surface.

## Decision

### 1. OCTET_LENGTH counts UTF-8 bytes; LENGTH aliases CHAR_LENGTH

`OCTET_LENGTH(text)` returns the UTF-8 byte count of the value —
distinguishable from `CHAR_LENGTH` on any multibyte string, which is the
acceptance control. `LENGTH` joins `CHARACTER_LENGTH` as an alias of
`CHAR_LENGTH`: character semantics, the Postgres convention.

### 2. OVERLAY follows SUBSTRING's literal-bound grammar doctrine

`OVERLAY(x PLACING y FROM i [FOR n])` lowers with `i` (and `n`) required to be
plan-time literals, range-checked in the frontend exactly as `SUBSTRING`'s
`FROM`/`FOR` are (`i >= 1`, `n >= 0`), traveling as literal arguments. The
semantics are the standard's own defining identity —
`SUBSTRING(x FROM 1 FOR i-1) || y || SUBSTRING(x FROM i+n)` with `n`
defaulting to `CHAR_LENGTH(y)` — implemented directly, character-positioned,
clamping past the end the way the substring composition naturally does.

### 3. TRANSLATE is the three-argument character map

`TRANSLATE(text, from_chars, to_chars)` maps each character of `text` found in
`from_chars` to the character at the same position in `to_chars`, deletes it
when `to_chars` is shorter, and copies unmatched characters through — the
de-facto convention (Postgres and peers). The standard's own
`TRANSLATE ... USING` names a translation catalog object this engine does not
have; that spelling stays rejected rather than silently reinterpreted.

All three propagate NULL and cannot fail at runtime; bad `OVERLAY` bounds are
bind-time rejections, never execution errors.

## Acceptance

- `OCTET_LENGTH` vs `CHAR_LENGTH` on a multibyte string: different numbers,
  both correct; equal on pure ASCII.
- `LENGTH` and `CHAR_LENGTH` are one function: same result, same plan.
- `OVERLAY` at the start, middle, end, past the end, with and without `FOR`,
  with `FOR 0` (pure insertion), and with multibyte characters on both sides
  of the position.
- Non-literal or out-of-range `FROM`/`FOR` are rejected at plan time with the
  SUBSTRING-style message.
- `TRANSLATE` with mapping, deletion (shorter `to_chars`), pass-through, and
  multibyte characters in all three arguments; `TRANSLATE ... USING` stays
  rejected.
- NULL in any argument yields NULL for all three.
- Rune parity: `octet_length`, `overlay`, `translate` reach the same
  identifiers.

## Guardrails

Signature reuse is asserted the way the registry's cost model expects: this
batch adds no new `ScalarFunctionSignature` variant except OVERLAY's
string-string-bounds shape if none exists; a second new shape is the smell the
model watches for.

## Risks and deferrals

`SUBSTR` shorthand stays rejected (existing decision). Byte-positioned OVERLAY
and binary-string OCTET_LENGTH wait for a binary type. `TRANSLATE ... USING`
waits for translation objects that will likely never exist.

## Forward

With these, the registry's core-standard string set is closed; the remaining
core gaps are structural, not scalar — `IN (SELECT ...)` membership and
quantified comparisons — which belong to the join machinery, not this
registry.
