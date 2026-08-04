# ADR 305 Results: SUBSTRING Expression Support

## Outcome

Implemented SQL `SUBSTRING` expression support and re-measured q22:

```text
q22 dev    = pass@execute
q22 strict = pass@execute
```

The measured result exceeded the minimum acceptance bar. q22 moved past the ADR 304
`unsupported_substring@plan` wall and reached execute in both modes at `sf=0.01`.

## Scope Delivered

- Lowered `SUBSTRING(expr FROM start [FOR length])` and `SUBSTRING(expr, start, length)`.
- Kept bounds literal-only for this cut.
- Rejected `SUBSTR` shorthand, missing start, non-literal bounds, `FROM <= 0`, and negative `FOR`.
- Allowed `FOR 0`, which returns the empty string.
- Added `Expr::Substring` and `BoundExprNode::Substring`.
- Bound only `Utf8` input and `Utf8` output.
- Executed through Arrow's character-counted `substring_by_char` kernel, with SQL's 1-based `FROM`
  converted to Arrow's 0-based start.
- Kept SUBSTRING out of predicate bytecode compilation; it is a residual Toron expression.

## Verification Notes

Focused behavior tests pin:

- FROM/FOR and comma-form lowering.
- Literal-bound rejection.
- 1-based character offsets.
- multibyte UTF-8 character slicing.
- `FOR 0` empty-string behavior.
- NULL preservation.
- non-UTF8 bind rejection.

q22 harness coverage was updated from the old substring wall to `pass@execute` in both modes.
The `unsupported_substring` telemetry classifier remains for old rows and targeted regression probes.

## Follow-On

No q22 reachability blocker was exposed by this cut. The next frontier should be selected from a fresh
matrix read rather than inferred from the old q22 wall.
