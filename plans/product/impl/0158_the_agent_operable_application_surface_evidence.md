# PRODUCT-0158 evidence: the driver against the live application

Runs on 2026-09-07 with a debug build of the polariton binary against the
local Nilket deployment, all through `polariton describe` and `polariton
drive`. Timings are wall clock from `time`.

## Describe: the Trends showcase

`polariton describe uat/plot_trends_showcase.rn` opened the application
headless, attached the local workspace, ran the source, and reported four
figures in under a second: line, stacked-area, scatter, and bubble, each with
its incarnation token, pane, view name, family, plot-area size, series table,
hidden sets, and inspector rows with provenance tiers. The scatter reported
`vector`; the bubble reported `series: absent` with the reason that it
identifies no series, which is what its published manifest says too.

## Drive: the Trends showcase

A seven-command script: describe, hide series 1 of the line figure by
incarnation, the same hide again, isolate series 99, restore an unknown token,
resize the line figure to 360x360, publish.

| command | outcome | evidence |
|---|---|---|
| hide 1 | applied | `state.hidden = [1]` |
| hide 1 again | applied | `state.hidden = [1]`, no second change |
| isolate 99 | refused | `unknown_series`, `series_count: 3` |
| restore token 777 | refused | `unknown_figure` |
| resize 360x360 | applied | the pane reported `366.4 x 366`, the cell grid's nearest size |
| publish | applied | four figures; the line figure's `series.json` carries `initially_hidden: [1]` and its viewbox is the resized pane |

The whole drive ran in 0.7 s.

## Drive: the PRODUCT-0157 gate-10 fixtures (gate 8)

`uat/plot_fixtures_showcase.commands.json`, checked in beside the showcase,
addresses each figure by view name: describe; hide `Base` in the percent
stack; hide `Plain` in the hostile-label line; resize the raster scatter to
360x360; hide `Atlas` in it; describe; publish to
`plot_fixtures_showcase_driven`.

| figure | before | after |
|---|---|---|
| fixture-raster-scatter | 942.4 x 432, `render_mode: downsampled` | 366.4 x 366, `render_mode: rasterized` |
| fixture-percent-area | hidden `[]` | hidden `[0]` |
| fixture-hostile-label | hidden `[]` | hidden `[1]` |

The published package's manifest lists the raster figure as `inert`, reason
`raster`, and the other two as `actionable`; each `series.json` carries the
hidden set above. The drive, including the 1,157,625-point scatter's
preparation twice, ran in 13.3 s in a debug build. Every describe reported
`loaded.hidden` empty and `live.hidden` as the state after the commands so
far.

Resizing the raster pane moved the divider it shares with the two panes above
it, as a pointer drag would, so those panes reported new sizes in the second
describe (174.4 x 498 and 184 x 498). The receipt says so; a script that wants
them larger resizes them too.

## What the driver found on the way

- The run's completion signal is the host panels, not the scheduler: the
  scheduler goes idle when the last relation's artifact lands, while the
  million-point kernel is still being built on a task. The headless run waits
  until no prepared panel is still running.
- A Rune view is a one-cell composite, so the render mode has to be read
  through the composite's per-cell plot areas, the same areas coverage uses.
  The grid kernel now derives those areas once for both.
- The window's runtime is implicit for everything `update` spawns; the
  headless application owns one and drives every task stream on it,
  collecting outputs through a channel, as the window's executor does. A
  blocking future inside a task holds a worker, never the application.

## Tests

Nine `product_0158` tests in the binary: the CLI's two subcommands; describe
against the inspector's rows and against the published `series.json`;
absence and render mode on fixtures; command and refusal JSON shapes; a
commands parse error refusing before the application opens; and three against
host panels the test fills itself: the driver's sequences against the
legend's own `HostPlotState` with equal hidden sets and equal revision bumps,
a stale incarnation refused with the current one after a rerun, and the menu's
publish and the driver's publish producing the same identity digest and
byte-identical files. The in-head completion the harness uses installs
panels without their named-view attachment, so those three address figures by
incarnation; the view form is exercised by the fixture drive above.

## Review corrections, same cut

Three findings from review, each with a regression test:

- `describe --session <id>` was specified and not built. The driver now
  opens any startup target through the window's own resolver, so a saved
  session opens as `polariton --session <id>` would; the test creates a
  session in an isolated store, opens it headless, fills its panels, and
  finds `loaded.hidden` empty with real pane sizes.
- A resize the pane already satisfied waited its full deadline for a change
  that never came, then reported refusal. The pane resize now says whether
  any boundary had to move; a satisfied request is complete at once.
- The loaded snapshot reported zero sizes because it was assumed rather than
  read. It is now captured from the panes at load, kept across every later
  describe, and asserted equal to the initial live state.

One coverage limit stands: the saved-session test opens a real store but
fills its panels through the test helper, so a complete saved-source run
against the workspace is not exercised by a test. Creating a saved session
with a source in it is a window action this record does not put on the
surface, so that run is an operator check: open a session on a showcase in
the window with the showcase source pane focused - the describe runs the
focused buffer, as Run does - close it, and `polariton describe --session
<id>`. Record the session id and the receipt path with the result. Passing
means the receipt names all three views with real sizes, every
`loaded.hidden` empty, and the scatter's resolved render mode.
