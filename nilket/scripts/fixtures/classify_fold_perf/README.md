# classify_fold_perf comparison fixtures

The Rune port of `classify_fold_perf.py` is checked against the original on
this fixture, because no perf capture or node profile is in the repository.
`perf` here stands in for the real tool and emits a fixed sample stream, so
both implementations see identical input. This compares one implementation
against the other; it says nothing about real perf output.

Run both from this directory's parent:

```sh
cd nilket/scripts
PATH=fixtures/classify_fold_perf:$PATH python3 classify_fold_perf.py \
  q20 2165-2168 2185-2199 \
  fixtures/classify_fold_perf/q20_run1.data \
  fixtures/classify_fold_perf/q20_run2.data
PATH=fixtures/classify_fold_perf:$PATH rnx run classify_fold_perf.rn \
  q20 2165-2168 2185-2199 \
  fixtures/classify_fold_perf/q20_run1.data \
  fixtures/classify_fold_perf/q20_run2.data
```

Both must print `expected.out` byte for byte. Failure behaviour is compared
too: a node profile with no reconciled stage, and a missing file, must each
exit 1 from both.

The sample stream deliberately contains `½` in two places, and the two are
not treated alike.

In a period field, `50½000` is not a number to either implementation:
Python's `isdigit` rejects it and so does the port's ASCII digit test, so
that sample is dropped whole.

In a source line, `aggregate.rs:21½0` is not dropped. Both stop the digit
scan at the `½` and read line 21: Python because `\d+` matches only the
prefix, the port because its scan ends at the first character outside `0` to
`9`. Line 21 is in neither the key-preparation nor the state-update range,
so the sample lands in the unattributed bucket, which is why the expected
output shows it there.

Together they are what separates an ASCII digit test from Rune's wider
`is_numeric`, which would take `50½000` as a number and fail to parse it.
The port fails this fixture if it uses the wider predicate.
