# Specification: High-Fidelity Hybrid Diffing (diffsplit)

## Objective
Implement a "Meld-Killer" diffing system in Baryon that combines the structural intelligence of a Unified Abstract Syntax Tree (UAST) with the surgical precision of the Histogram diff algorithm. The system provides side-by-side diff panes, synchronized scrolling, and high-DPI Bezier "bridge" connections using the Kitty Graphics Protocol.

## 1. Hybrid Diff Engine (`src/engine/diff.rs`)

### 1.1 Structural Anchoring (UAST)
The algorithm uses the `UastRegistry` to perform high-level structural matching. 
- **Language Agnostic:** By targeting unified `SemanticKind` nodes (`RelationalTable`, `RelationalRow`, `Token`), the diff works across all supported languages (Rust, Python, TOML, CSV, etc.).
- **Move Detection:** The engine performs bottom-up similarity scoring (Jaccard Index) on parent nodes. If a function (`RelationalTable`) moves from the top of the file to the bottom, the AST matcher pairs them, allowing the UI to draw a connecting bridge across the entire document.
- **Match Threshold:** A forgiving threshold (currently `0.1`) ensures that even heavily refactored functions are correctly anchored.

### 1.2 Targeted Precision (Histogram)
Once two top-level structures are paired by the AST, the engine switches to the **Histogram Diff** algorithm (via the `imara-diff` crate) to compare their contents.
- **Human Readable:** Histogram diffing (the Git default) is superior to standard Myers for code, as it anchors on unique lines to avoid the "mismatched brace" problem.
- **Hybrid Handoff:** Recursing into the AST stops once a structural match is made, handing off the line-level work to the high-performance Histogram engine.

### 1.3 Super-Targeted Precision (Semantic Nodes)
Within the lines matched by the Histogram diff, the engine will perform a third, fine-grained layer of analysis using red/green semantic nodes (tokens/keywords/identifiers).
- **Intra-Line Granularity:** Instead of treating a modified line as a single monolithic block, the semantic tokens (red-green nodes) are diffed to pinpoint exact character-level insertions, deletions, or modifications within that line.
- **Semantic Awareness:** Ensures that only actual semantic changes are highlighted, potentially ignoring pure whitespace or cosmetic formatting differences, improving code review precision.
- **Adaptive Fallback:** If semantic resolution fails or is too noisy, the engine seamlessly falls back to the Histogram line-diff bounds.

### 1.4 Performance Optimization
- **Linear Pass:** Absolute line and byte coordinates for all nodes are pre-computed in a single linear O(N) pass using a `LineIndex` (binary search for byte-to-line mapping).
- **Zero-Length Anchors:** Pure additions and deletions are mapped to single-line "anchor points" on the opposing pane to ensure the bridge splines converge correctly.

## 2. Visual Representation

### 2.1 The Gutter Bridge (`src/ui/diff.rs`)
- **Horizontal Space:** The vertical pane divider is widened to 4 columns (`SPLIT_DIVIDER_CELLS = 4`) to provide a canvas for the bridge.
- **High-DPI Splines:** Using the Kitty Graphics Protocol, Baryon renders an `RgbaImage` containing cubic Bezier curves that fluidly connect the corresponding line ranges between the left and right panes.
- **S-Curve Geometry:** Cubic Bezier points are calculated with horizontal tangents at the pane boundaries, creating smooth, "liquid" transitions for moved or modified code blocks.

### 2.2 Background Highlighting (`src/ui/draw.rs`)
- **Semantic Colors:** Added (Green), Removed (Red), and Modified (Blue) blocks are rendered with semi-transparent background overlays.
- **Coordinate Sync:** The UI utilizes `ScreenRow` and `PaneRow` unit types to ensure perfectly aligned highlighting across different coordinate spaces.
- **Intra-line Highlights:** For super-targeted precision, exact ranges of semantic tokens within a modified line use a higher opacity or distinct brightened color background, clearly differentiating the specific token changes from the rest of the modified line block.

## 3. Interaction Model

### 3.1 Commands
- `:diffsplit <file>` / `:ds <file>`: Vertically split the current pane and initiate a diff against the specified file.
- **Async Responsiveness:** Diffs are initially calculated using a fast line-based fallback and then automatically recomputed using the full hybrid AST model as soon as the asynchronous semantic parser completes the tree graft.

### 3.2 Synchronized Scrolling
- **Linked Viewports:** Scrolling one pane in a diff pair automatically adjusts the partner pane's scroll position based on the calculated `DiffBlock` mapping, keeping the corresponding lines visually aligned.
