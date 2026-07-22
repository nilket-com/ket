
# Polariton Observations
---

 - Tabs need title bar with x button logic.
 - Multiple panes pick up typing.
 - Needs click to change pane focus.
 - :pn is missing
 - Cursor is displayed on multiple panes which is a confusing false indicator of pane focus.


 - Triggered by running create table sql on sqlite memory:

❯ polariton

thread '<unnamed>' (808754) panicked at crates/libaryon/src/engine/session.rs:3336:18:
view content must match its buffer content
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
[1]    808716 IOT instruction (core dumped)  polariton
