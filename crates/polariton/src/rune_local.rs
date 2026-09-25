//! Rune sources that bring their own data, run in the window on rnx.
//!
//! A source that declares a top-level `main` item is a local program: it
//! runs here, not in Toron, with rnx's batteries (`http`, `json`, `text`,
//! `time`, and also `fs`, `path`, `process`, `io` and `env` — the whole
//! `rnx::server` context, not an HTTP-only surface) beside Polariton's own
//! `workspace` and `viz`. Its entry keeps the existing contract,
//! `main(workspace, viz)`, and may be `async`.
//!
//! The mode is read from the parsed source, never guessed from a failed view
//! compilation: a view source is statements, so it has no `main` of its own.
//!
//! `viz.frame(name, rows)` turns the script's rows into a plottable cell. Its
//! rules are deliberate, because a frame is where fetched text becomes data:
//! columns are the union of the rows' keys in name order; a missing key or
//! `()` is null, never zero; integers and floats together widen to float;
//! unsigned integers are integers when they fit in i64; any other mix of
//! types, an all-null column, or an unsupported value (including an unsigned
//! integer beyond i64) is refused with the column and row named.
//!
//! `viz.frame_typed(name, rows, schema)` states types instead of inferring
//! them: `schema` maps column names to `"date"`, `"float"`, `"int"`,
//! `"string"` or `"bool"`. A date is never guessed from text that looks like
//! one: only a `"date"` column converts, from strict `YYYY-MM-DD` strings, and
//! any other value in it is refused by row. A declared column may be all
//! null (a sparse fetch); an undeclared one may not, since nothing says its
//! type. Unnamed columns are inferred.

use crate::result_artifact::ResultArtifact;
use crate::rune_host::{RuneCapabilities, RuneCell, RuneExecution, RuneViz, Surface, decode_return};
use polars::prelude::{Column, DataFrame};
use rune::ast::Spanned;
use rune::runtime::{Object, Value, VmResult};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::oneshot;

/// Instructions one local execution may spend. rnx refuses zero and
/// `usize::MAX`; this bounds CPU-only loops, which cancellation cannot stop.
pub(crate) const BUDGET: usize = 500_000_000;

/// The execution mode a source declares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
	/// No top-level `main`: the existing view/presentation path.
	Views,
	/// A top-level `main` taking `(workspace, viz)`.
	Local,
	/// A top-level `main` with the wrong arity: refused, with this count.
	LocalArity(usize),
	/// The source does not parse, so its mode is unknown. It stays on the
	/// existing path, which owns its diagnostics; the error is kept so an
	/// admission refusal can name it instead of a missing connection.
	Unparsed(String),
}

impl Mode {
	/// Whether this source runs in the window. Never true for a source whose
	/// mode is unknown: a failed parse is not permission to run locally.
	pub(crate) fn is_local(&self) -> bool {
		matches!(self, Mode::Local | Mode::LocalArity(_))
	}
}

/// Classify a source by its parsed items.
pub(crate) fn mode(source: &str) -> Mode {
	let file = match rune::parse::parse_all::<rune::ast::File>(
		source,
		rune::SourceId::empty(),
		false,
	) {
		Ok(file) => file,
		// A view source is statements, which are not file items. It is
		// unparsed only if it fails as the body the view compiler wraps it in.
		Err(error) => {
			let body = format!("pub fn main(workspace) {{\n{source}\n}}\n");
			return match rune::parse::parse_all::<rune::ast::File>(
				&body,
				rune::SourceId::empty(),
				false,
			) {
				Ok(_) => Mode::Views,
				Err(_) => Mode::Unparsed(error.to_string()),
			};
		}
	};
	for (item, _) in &file.items {
		if let rune::ast::Item::Fn(function) = item
			&& source.get(function.name.span().range()) == Some("main")
		{
			return match function.args.len() {
				2 => Mode::Local,
				count => Mode::LocalArity(count),
			};
		}
	}
	Mode::Views
}

/// Compile and run one local source on the calling thread, which must not be
/// a runtime worker: the invocation is created, driven and closed here.
/// Resolving or dropping `cancel`'s sender abandons the run and closes it,
/// revoking its HTTP requests; the result is then `Err`.
pub(crate) fn execute(
	name: &str,
	source: &str,
	capabilities: RuneCapabilities,
	cancel: oneshot::Receiver<()>,
) -> Result<RuneExecution, String> {
	execute_with(name, source, capabilities, cancel, extensions)
}

/// `execute` over a caller-chosen extension set; rnx builds it twice, for
/// compilation and for the invocation, so it is a constructor, not a value.
fn execute_with(
	name: &str,
	source: &str,
	capabilities: RuneCapabilities,
	cancel: oneshot::Receiver<()>,
	extensions: fn() -> rnx::Extensions,
) -> Result<RuneExecution, String> {
	if let Mode::LocalArity(count) = mode(source) {
		return Err(format!(
			"main takes {count} argument{}; a local source's main takes (workspace, viz)",
			if count == 1 { "" } else { "s" }
		));
	}
	// Presentation calls gain their original spans before compilation; the
	// compiled text is the instrumented one, and every VM diagnostic is
	// mapped back to the author's buffer through `locator`.
	#[cfg(feature = "toron")]
	let instrumented = crate::rune_instrument::instrument(
		source,
		&crate::rune_instrument::Vocabulary::from_registry(&["view"]),
	)
	.map_err(|error| locate(name, source, error.span.start, &error.message))?;
	#[cfg(feature = "toron")]
	let (compiled, locator) = (
		instrumented.source.as_str(),
		Locator {
			name,
			original: source,
			generated: instrumented.source.as_str(),
			map: Some(&instrumented),
		},
	);
	#[cfg(not(feature = "toron"))]
	let (compiled, locator) = (
		source,
		Locator {
			name,
			original: source,
			generated: source,
		},
	);
	let program = rnx::server::Program::compile_source(name, compiled, extensions())
		.map_err(|failure| locator.describe(&failure))?;
	let surface = Surface::local(capabilities, name, source);
	let arguments = vec![
		rune::to_value(surface.workspace.clone()).map_err(|error| error.to_string())?,
		rune::to_value(surface.viz.clone()).map_err(|error| error.to_string())?,
	];
	let mut invocation = program
		.prepare_with(extensions(), "main", arguments, BUDGET)
		.map_err(|failure| locator.describe(&failure))?;
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|error| format!("Rune runtime could not start: {error}"))?;
	let outcome = runtime.block_on(async {
		tokio::select! {
			result = invocation.run() => Some(result),
			_ = cancel => None,
		}
	});
	// The run future is dropped above; close retires what it still owned.
	let closed = invocation.close();
	surface.finish(settle(outcome, closed, &locator))
}

/// Cleanup failure is state loss and outranks every outcome, cancellation
/// included; then cancellation; then the script's own result.
fn settle(
	outcome: Option<Result<Value, rnx::server::Failure>>,
	closed: Result<(), rnx::server::Failure>,
	locator: &Locator<'_>,
) -> Result<Option<String>, String> {
	match (outcome, closed) {
		(_, Err(failure)) if failure.category() == "cleanup" => Err(locator.describe(&failure)),
		(None, _) => Err(CANCELLED.to_owned()),
		(Some(Ok(value)), _) => decode_return(value),
		(Some(Err(failure)), _) => Err(locator.describe(&failure)),
	}
}

/// The error a cancelled run settles with.
pub(crate) const CANCELLED: &str = "execution cancelled";

fn extensions() -> rnx::Extensions {
	let extensions = rnx::Extensions::none().with("polariton", |module| {
		crate::rune_host::register(module)?;
		Ok(Vec::new())
	});
	// The same presentation vocabulary a view source uses, plus the local
	// `view(name, body)` carrier and the method guard the instrumentation
	// calls. Registered under the name the instrumented calls address.
	#[cfg(feature = "toron")]
	let extensions = extensions.with("polariton_presentation", |module| {
		crate::rune_presentation::install_vocabulary(module)?;
		crate::rune_presentation::install_local_views(module)?;
		Ok(Vec::new())
	});
	extensions
}

/// Where a local run's diagnostics point: the author's original buffer. The
/// compiled text may be instrumented; its positions are mapped back.
struct Locator<'a> {
	name: &'a str,
	original: &'a str,
	generated: &'a str,
	#[cfg(feature = "toron")]
	map: Option<&'a crate::rune_instrument::Instrumented>,
}

impl Locator<'_> {
	fn describe(&self, failure: &rnx::server::Failure) -> String {
		let Some((line, column)) = failure.position() else {
			return failure.message().to_owned();
		};
		let generated = byte_offset(self.generated, line, column);
		#[cfg(feature = "toron")]
		let original = match self.map {
			Some(map) => map.original_offset(generated),
			None => generated,
		};
		#[cfg(not(feature = "toron"))]
		let original = generated;
		locate(self.name, self.original, original, failure.message())
	}
}

/// The byte offset of a one-based line and character column.
fn byte_offset(text: &str, line: usize, column: usize) -> usize {
	let start = text
		.split_inclusive('\n')
		.take(line.saturating_sub(1))
		.map(str::len)
		.sum::<usize>();
	let rest = &text[start.min(text.len())..];
	start
		+ rest
			.char_indices()
			.nth(column.saturating_sub(1))
			.map_or(rest.len(), |(offset, _)| offset)
}

/// `name:line:column: message` with the source line, for a byte offset in
/// the original text.
pub(crate) fn locate(name: &str, source: &str, offset: usize, message: &str) -> String {
	let offset = offset.min(source.len());
	let before = &source[..offset];
	let line = before.matches('\n').count() + 1;
	let line_start = before.rfind('\n').map_or(0, |index| index + 1);
	let column = source[line_start..offset].chars().count() + 1;
	let excerpt = source[line_start..].lines().next().unwrap_or("");
	format!("{name}:{line}:{column}: {message}\n    {excerpt}")
}

/// Why `views` refuses outside a local run.
pub(crate) const VIEWS_LOCAL_ONLY: &str = "views([...]) on a frame is available in a local source, one that declares `pub fn main(workspace, viz)`";

/// `frame.views([view("name", figure([...])), ...])`: decode each entry
/// through the shared presentation vocabulary and queue the set for this
/// run's batch. A name is any nonempty string, unique across the run.
#[cfg(feature = "toron")]
pub(crate) fn cell_views(
	cell: &RuneCell,
	list: &rune::runtime::Vec,
) -> VmResult<Result<(), String>> {
	let Some(queue) = &cell.views else {
		return VmResult::Ok(Err(VIEWS_LOCAL_ONLY.to_owned()));
	};
	let Ok(mut queue) = queue.lock() else {
		return VmResult::Ok(Err("View queue was poisoned".to_owned()));
	};
	let mut views = Vec::with_capacity(list.len());
	let mut here = std::collections::HashSet::new();
	for value in list.iter() {
		let (name, span, intent) = match crate::rune_presentation::decode_local_view(value) {
			Ok(decoded) => decoded,
			Err(failure) => {
				return VmResult::Ok(Err(locate(
					&queue.name,
					&queue.source,
					failure.span.start as usize,
					&failure.message,
				)));
			}
		};
		if queue.names.contains(&name) || !here.insert(name.clone()) {
			return VmResult::Ok(Err(locate(
				&queue.name,
				&queue.source,
				span.start as usize,
				&format!("view \"{name}\" is already defined in this run; view names are unique"),
			)));
		}
		views.push((name, span, intent));
	}
	let mut order = queue.names.len();
	let views = views
		.into_iter()
		.map(|(name, span, intent)| {
			queue.names.insert(name.clone());
			order += 1;
			crate::rune_relation_envelope::SpannedRuneViewIntent {
				intent,
				span,
				identity: crate::rune_view::LocalViewIdentity::Named(name),
				output_order: order - 1,
			}
		})
		.collect();
	let (source_name, source) = (queue.name.clone(), Arc::clone(&queue.source));
	let sequence = queue
		.sequence
		.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	queue.sets.push(crate::rune_host::LocalViewSet {
		order: sequence,
		artifact: Arc::clone(&cell.artifact),
		views,
		source_name,
		source,
	});
	VmResult::Ok(Ok(()))
}

/// Without the `toron` feature there is no presentation vocabulary to decode.
#[cfg(not(feature = "toron"))]
pub(crate) fn cell_views(
	cell: &RuneCell,
	_list: &rune::runtime::Vec,
) -> VmResult<Result<(), String>> {
	VmResult::Ok(Err(match cell.views {
		None => VIEWS_LOCAL_ONLY.to_owned(),
		Some(_) => "views([...]) needs Polariton's toron feature, which this build lacks".to_owned(),
	}))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
	Int,
	Float,
	Bool,
	Text,
	Date,
}

impl Kind {
	fn name(self) -> &'static str {
		match self {
			Kind::Int => "integer",
			Kind::Float => "float",
			Kind::Bool => "bool",
			Kind::Text => "string",
			Kind::Date => "date",
		}
	}

	fn declared(name: &str) -> Option<Self> {
		Some(match name {
			"int" => Kind::Int,
			"float" => Kind::Float,
			"bool" => Kind::Bool,
			"string" => Kind::Text,
			"date" => Kind::Date,
			_ => return None,
		})
	}
}

/// Days since 1970-01-01 for a strict `YYYY-MM-DD`, or `None`.
fn civil_days(text: &str) -> Option<i32> {
	let bytes = text.as_bytes();
	if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
		return None;
	}
	let digits = |range: std::ops::Range<usize>| -> Option<i64> {
		let part = &text[range];
		part.bytes().all(|b| b.is_ascii_digit()).then(|| part.parse().ok())?
	};
	let (year, month, day) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
	let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
	let length = match month {
		1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
		4 | 6 | 9 | 11 => 30,
		2 if leap => 29,
		2 => 28,
		_ => return None,
	};
	if day < 1 || day > length {
		return None;
	}
	// Howard Hinnant's days_from_civil.
	let y = if month <= 2 { year - 1 } else { year };
	let era = y.div_euclid(400);
	let yoe = y - era * 400;
	let mp = (month + 9) % 12;
	let doy = (153 * mp + 2) / 5 + day - 1;
	let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	i32::try_from(era * 146_097 + doe - 719_468).ok()
}

enum Cell {
	Null,
	Int(i64),
	Float(f64),
	Bool(bool),
	Text(String),
}

fn cell(value: &Value) -> Option<Cell> {
	if value.into_unit().is_ok() {
		return Some(Cell::Null);
	}
	if let Ok(v) = value.as_signed() {
		return Some(Cell::Int(v));
	}
	// Lengths and indexes are unsigned; beyond i64 there is no honest column.
	if let Ok(v) = value.as_unsigned() {
		return i64::try_from(v).ok().map(Cell::Int);
	}
	if let Ok(v) = value.as_float() {
		return Some(Cell::Float(v));
	}
	if let Ok(v) = value.as_bool() {
		return Some(Cell::Bool(v));
	}
	if let Ok(v) = value.borrow_string_ref() {
		return Some(Cell::Text(v.to_string()));
	}
	None
}

/// `viz.frame(name, rows)`: see the module documentation for the rules.
pub(crate) fn viz_frame(
	viz: &RuneViz,
	name: String,
	// Borrowed: building a frame must not consume the script's rows.
	rows: &rune::runtime::Vec,
) -> VmResult<Result<RuneCell, String>> {
	VmResult::Ok(frame(&rows[..]).map(|data| RuneCell {
		name,
		artifact: ResultArtifact::from_polars(Arc::new(data)),
		receipts: Arc::clone(&viz.receipts),
		views: viz.views.clone(),
	}))
}

/// One declared column: every non-null cell must be of the declared kind
/// (an integer may fill a float column), or the row is named. A declared
/// column may be entirely null, since its type does not need inferring.
fn typed_column(name: &str, kind: Kind, cells: &[Cell]) -> Result<Column, String> {
	let refuse = |row: usize, cell: &Cell| {
		let found = match cell {
			Cell::Null => "null",
			Cell::Int(_) => "integer",
			Cell::Float(_) => "float",
			Cell::Bool(_) => "bool",
			Cell::Text(_) => "string",
		};
		format!("frame column `{name}` is declared {} but row {row} is {found}", kind.name())
	};
	macro_rules! collect {
		($pattern:pat => $value:expr) => {{
			let mut values = Vec::with_capacity(cells.len());
			for (row, cell) in cells.iter().enumerate() {
				values.push(match cell {
					Cell::Null => None,
					$pattern => Some($value),
					other => return Err(refuse(row, other)),
				});
			}
			values
		}};
	}
	let column = match kind {
		Kind::Int => Column::new(name.into(), collect!(Cell::Int(v) => *v)),
		Kind::Float => {
			let mut values = Vec::with_capacity(cells.len());
			for (row, cell) in cells.iter().enumerate() {
				values.push(match cell {
					Cell::Null => None,
					Cell::Float(v) => Some(*v),
					Cell::Int(v) => Some(*v as f64),
					other => return Err(refuse(row, other)),
				});
			}
			Column::new(name.into(), values)
		}
		Kind::Bool => Column::new(name.into(), collect!(Cell::Bool(v) => *v)),
		Kind::Text => Column::new(name.into(), collect!(Cell::Text(v) => v.clone())),
		Kind::Date => {
			let mut days = Vec::with_capacity(cells.len());
			for (row, cell) in cells.iter().enumerate() {
				days.push(match cell {
					Cell::Null => None,
					Cell::Text(text) => {
						Some(civil_days(text).ok_or_else(|| {
							format!(
								"frame column `{name}` is declared date but row {row} is \"{text}\", not YYYY-MM-DD"
							)
						})?)
					}
					other => return Err(refuse(row, other)),
				});
			}
			Column::new(name.into(), days)
				.cast(&polars::prelude::DataType::Date)
				.map_err(|error| format!("frame column `{name}` could not become a date: {error}"))?
		}
	};
	Ok(column)
}

/// `viz.frame_typed(name, rows, schema)`: see the module documentation.
pub(crate) fn viz_frame_typed(
	viz: &RuneViz,
	name: String,
	rows: &rune::runtime::Vec,
	schema: &Object,
) -> VmResult<Result<RuneCell, String>> {
	let mut declared = BTreeMap::new();
	for (column, kind) in schema.iter() {
		let kind = match kind.borrow_string_ref() {
			Ok(kind) => kind.to_string(),
			Err(_) => {
				return VmResult::Ok(Err(format!(
					"frame schema for `{column}` is {}, not a type name",
					kind.type_info()
				)));
			}
		};
		match Kind::declared(&kind) {
			Some(kind) => {
				declared.insert(column.to_string(), kind);
			}
			None => {
				return VmResult::Ok(Err(format!(
					"frame schema gives `{column}` the type \"{kind}\"; use date, float, int, string or bool"
				)));
			}
		}
	}
	VmResult::Ok(frame_with(&rows[..], &declared).map(|data| RuneCell {
		name,
		artifact: ResultArtifact::from_polars(Arc::new(data)),
		receipts: Arc::clone(&viz.receipts),
		views: viz.views.clone(),
	}))
}

fn frame(rows: &[Value]) -> Result<DataFrame, String> {
	frame_with(rows, &BTreeMap::new())
}

fn frame_with(rows: &[Value], declared: &BTreeMap<String, Kind>) -> Result<DataFrame, String> {
	if rows.is_empty() {
		return Err("frame needs at least one row".to_owned());
	}
	let mut objects = Vec::with_capacity(rows.len());
	for (index, row) in rows.iter().enumerate() {
		let object = row
			.borrow_ref::<Object>()
			.map_err(|_| format!("frame row {index} is {}, not an object", row.type_info()))?;
		objects.push(object);
	}
	let names: BTreeSet<String> = objects
		.iter()
		.flat_map(|object| object.keys().map(|key| key.to_string()))
		.collect();
	if let Some(missing) = declared.keys().find(|column| !names.contains(*column)) {
		return Err(format!("frame schema names `{missing}`, which no row has"));
	}
	let mut columns = Vec::with_capacity(names.len());
	for name in &names {
		let mut cells = Vec::with_capacity(objects.len());
		let mut kinds: BTreeMap<&'static str, (Kind, usize)> = BTreeMap::new();
		for (index, object) in objects.iter().enumerate() {
			let value = match object.get(name.as_str()) {
				None => Cell::Null,
				Some(value) => cell(value).ok_or_else(|| {
					format!(
						"frame column `{name}` row {index} is {}, which a frame cannot hold",
						value.type_info()
					)
				})?,
			};
			let kind = match &value {
				Cell::Null => None,
				Cell::Int(_) => Some(Kind::Int),
				Cell::Float(_) => Some(Kind::Float),
				Cell::Bool(_) => Some(Kind::Bool),
				Cell::Text(_) => Some(Kind::Text),
			};
			if let Some(kind) = kind {
				kinds.entry(kind.name()).or_insert((kind, index));
			}
			cells.push(value);
		}
		if let Some(kind) = declared.get(name) {
			columns.push(typed_column(name, *kind, &cells)?);
			continue;
		}
		// Integers widen to float before conflicts are looked for, so an
		// int/float pair is never reported as the mix.
		if let Some(int_at) = kinds.get("integer").map(|(_, at)| *at)
			&& let Some((_, float_at)) = kinds.get_mut("float")
		{
			*float_at = (*float_at).min(int_at);
			kinds.remove("integer");
		}
		let kinds: Vec<(Kind, usize)> = kinds.into_values().collect();
		let kind = match kinds.as_slice() {
			[] => {
				return Err(format!(
					"frame column `{name}` has no values; its type cannot be inferred"
				));
			}
			[(kind, _)] => *kind,
			[(first, at), (second, other), ..] => {
				return Err(format!(
					"frame column `{name}` mixes {} (row {at}) and {} (row {other})",
					first.name(),
					second.name()
				));
			}
		};
		let column = match kind {
			Kind::Int => Column::new(
				name.as_str().into(),
				cells
					.iter()
					.map(|c| match c {
						Cell::Int(v) => Some(*v),
						_ => None,
					})
					.collect::<Vec<_>>(),
			),
			Kind::Float => Column::new(
				name.as_str().into(),
				cells
					.iter()
					.map(|c| match c {
						Cell::Float(v) => Some(*v),
						Cell::Int(v) => Some(*v as f64),
						_ => None,
					})
					.collect::<Vec<_>>(),
			),
			Kind::Bool => Column::new(
				name.as_str().into(),
				cells
					.iter()
					.map(|c| match c {
						Cell::Bool(v) => Some(*v),
						_ => None,
					})
					.collect::<Vec<_>>(),
			),
			Kind::Text => Column::new(
				name.as_str().into(),
				cells
					.iter()
					.map(|c| match c {
						Cell::Text(v) => Some(v.clone()),
						_ => None,
					})
					.collect::<Vec<_>>(),
			),
			// Dates are only ever declared, never inferred.
			Kind::Date => unreachable!("an inferred column is never a date"),
		};
		columns.push(column);
	}
	DataFrame::new(objects.len(), columns).map_err(|error| format!("frame could not be built: {error}"))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::request::PlotSource;
	use std::io::{Read, Write};
	use std::net::TcpListener;
	use std::time::{Duration, Instant};

	fn run(source: &str) -> Result<RuneExecution, String> {
		let (_cancel, cancelled) = oneshot::channel();
		execute("<test>", source, RuneCapabilities::default(), cancelled)
	}

	/// Serve `body` to exactly one request on loopback, then stop listening.
	fn serve(body: &'static str) -> String {
		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		let address = listener.local_addr().unwrap();
		std::thread::spawn(move || {
			if let Ok((mut stream, _)) = listener.accept() {
				let mut request = [0_u8; 4096];
				let _ = stream.read(&mut request);
				let _ = write!(
					stream,
					"HTTP/1.1 200 OK\r\ncontent-type: text/csv\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
					body.len()
				);
			}
		});
		format!("http://{address}")
	}

	/// One connection that is read and never answered. `requested` fires once
	/// the request has arrived, so HTTP is provably in flight; `released`
	/// fires when the client drops the connection, which is rnx revoking it.
	/// The thread and its listener end with that connection.
	struct Stall {
		url: String,
		requested: std::sync::mpsc::Receiver<()>,
		released: std::sync::mpsc::Receiver<()>,
	}

	fn stall() -> Stall {
		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		let address = listener.local_addr().unwrap();
		let (requested_tx, requested) = std::sync::mpsc::channel();
		let (released_tx, released) = std::sync::mpsc::channel();
		std::thread::spawn(move || {
			if let Ok((mut stream, _)) = listener.accept() {
				let mut buffer = [0_u8; 4096];
				let _ = stream.read(&mut buffer);
				let _ = requested_tx.send(());
				// Blocks until the client closes its end.
				while matches!(stream.read(&mut buffer), Ok(n) if n > 0) {}
				let _ = released_tx.send(());
			}
		});
		Stall {
			url: format!("http://{address}"),
			requested,
			released,
		}
	}

	const CSV: &str = "observation_date,DGS10\n2026-09-21,4.96\n2026-09-22,4.96\n2026-09-23,5.11\n";

	fn fetch_and_plot(base: &str) -> String {
		format!(
			r#"
				pub async fn main(workspace, viz) {{
					let response = http::get("{base}/fred.csv").await?;
					let rows = [];
					let first = true;
					for line in response.body.split("\n") {{
						if first {{ first = false; continue; }}
						let parts = [];
						for part in line.split(",") {{ parts.push(part); }}
						if parts.len() != 2 {{ continue; }}
						let value = if parts[1] == "" {{ () }} else {{ std::f64::parse(parts[1])? }};
						rows.push(#{{ day: rows.len(), date: parts[0], rate: value }});
					}}
					viz.plot(viz.frame("DGS10", rows)?).scatter("day", "rate").line().show()?;
					Ok(rows.len())
				}}
			"#
		)
	}

	#[test]
	fn existing_view_sources_keep_the_view_path() {
		let uat = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../uat");
		let mut seen = 0;
		for entry in std::fs::read_dir(&uat).unwrap().flatten() {
			let path = entry.path();
			if path.extension().is_some_and(|e| e == "rn") {
				let text = std::fs::read_to_string(&path).unwrap();
				// The one exhibit that fetches its own data is local by design.
				let expected = if path.ends_with("treasury_rates.rn") {
					Mode::Local
				} else {
					Mode::Views
				};
				assert_eq!(mode(&text), expected, "{}", path.display());
				seen += 1;
			}
		}
		assert!(seen > 10, "expected the uat exhibits, saw {seen}");
	}

	#[test]
	fn a_declared_main_selects_the_local_path() {
		assert_eq!(mode("pub async fn main(workspace, viz) {}"), Mode::Local);
		assert_eq!(mode("fn helper() {}\npub fn main(w, v) { helper() }"), Mode::Local);
		assert_eq!(mode("pub fn main() {}"), Mode::LocalArity(0));
		// A body statement is not an item, and a nested main is not top level.
		assert_eq!(mode("viz.plot(workspace.cell(\"a\")?).show()?;"), Mode::Views);
		assert_eq!(mode("let f = || { fn main(a, b) {} };"), Mode::Views);
		// Unparseable text is never local; it keeps the existing path.
		let unparsed = mode("pub fn main(workspace, viz) {");
		assert!(matches!(unparsed, Mode::Unparsed(_)), "{unparsed:?}");
		assert!(!unparsed.is_local());
	}

	#[test]
	fn wrong_main_arity_is_refused_by_name() {
		let error = run("pub fn main() {}").unwrap_err();
		assert!(error.contains("main takes 0 arguments"), "{error}");
	}

	#[test]
	fn compile_errors_name_the_buffer_line_and_text() {
		let error = run("pub fn main(workspace, viz) {\n    missing_function()\n}\n").unwrap_err();
		assert!(error.starts_with("<test>:2:"), "{error}");
		assert!(error.contains("missing_function()"), "{error}");
	}

	#[test]
	fn loopback_http_becomes_a_typed_frame_and_one_plot() {
		let base = serve(CSV);
		let execution = run(&fetch_and_plot(&base)).expect("local source runs");
		assert_eq!(execution.display.as_deref(), Some("3"));
		assert_eq!(execution.requests.len(), 1);
		let PlotSource::Materialized { name, data } = &execution.requests[0].source;
		assert_eq!(name, "DGS10");
		let frame = data.projection().unwrap();
		assert_eq!(frame.height(), 3);
		let yields: Vec<Option<f64>> = frame.column("rate").unwrap().f64().unwrap().into_iter().collect();
		assert_eq!(yields, vec![Some(4.96), Some(4.96), Some(5.11)]);
		let dates = frame.column("date").unwrap().str().unwrap();
		assert_eq!(dates.get(2), Some("2026-09-23"));
	}

	fn stalled_run(stall: &Stall) -> (oneshot::Sender<()>, std::thread::JoinHandle<Result<RuneExecution, String>>) {
		let source = format!(
			"pub async fn main(workspace, viz) {{ http::get(\"{}/slow\").await?; Ok(1) }}",
			stall.url
		);
		let (cancel, cancelled) = oneshot::channel();
		let worker = std::thread::spawn(move || {
			execute("<test>", &source, RuneCapabilities::default(), cancelled)
		});
		stall
			.requested
			.recv_timeout(Duration::from_secs(10))
			.expect("the request reached the server");
		(cancel, worker)
	}

	#[test]
	fn a_rerun_cancels_a_pending_request_promptly() {
		let stall = stall();
		let (cancel, worker) = stalled_run(&stall);
		let cancelled_at = Instant::now();
		cancel.send(()).unwrap();
		assert_eq!(worker.join().unwrap().unwrap_err(), CANCELLED);
		// rnx's own HTTP deadline is 30 s; the server seeing the connection
		// dropped well before it is the proof the request was revoked.
		stall
			.released
			.recv_timeout(Duration::from_secs(5))
			.expect("the connection was released");
		assert!(cancelled_at.elapsed() < Duration::from_secs(5));
	}

	#[test]
	fn a_dropped_sender_cancels_like_a_rerun() {
		let stall = stall();
		let (cancel, worker) = stalled_run(&stall);
		drop(cancel);
		assert_eq!(worker.join().unwrap().unwrap_err(), CANCELLED);
		stall
			.released
			.recv_timeout(Duration::from_secs(5))
			.expect("the connection was released");
	}

	static HANG_POLLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

	/// Pending forever; panics when revoked, which rnx records as a failed
	/// lifecycle cleanup.
	struct Hang;
	impl std::future::Future for Hang {
		type Output = Result<i64, String>;
		fn poll(
			self: std::pin::Pin<&mut Self>,
			_: &mut std::task::Context<'_>,
		) -> std::task::Poll<Self::Output> {
			HANG_POLLED.store(true, std::sync::atomic::Ordering::SeqCst);
			std::task::Poll::Pending
		}
	}
	impl Drop for Hang {
		fn drop(&mut self) {
			panic!("hang refused to stop");
		}
	}

	fn hanging() -> rnx::Extensions {
		extensions().with_lifecycle("probe", |module, scope| {
			module
				.function("hang", move || scope.track(Hang))
				.build()
				.map_err(|error| error.to_string())?;
			Ok(Vec::new())
		})
	}

	#[test]
	fn a_failed_cleanup_outranks_cancellation() {
		let (cancel, cancelled) = oneshot::channel();
		let worker = std::thread::spawn(move || {
			execute_with(
				"<test>",
				"pub async fn main(workspace, viz) { probe::hang().await }",
				RuneCapabilities::default(),
				cancelled,
				hanging,
			)
		});
		let started = Instant::now();
		while !HANG_POLLED.load(std::sync::atomic::Ordering::SeqCst) {
			assert!(started.elapsed() < Duration::from_secs(5), "hang was never polled");
			std::thread::sleep(Duration::from_millis(5));
		}
		cancel.send(()).unwrap();
		let error = worker.join().unwrap().unwrap_err();
		assert_ne!(error, CANCELLED);
		assert!(error.contains("lifecycle cleanup"), "{error}");
	}

	#[cfg(feature = "toron")]
	/// Call a handler appended to the Treasury exhibit, without its fetches,
	/// instrumented and located exactly as a run would be.
	fn exhibit(handlers: &str, handler: &str) -> Result<String, String> {
		let source = format!("{}\n{handlers}", include_str!("../../../uat/treasury_rates.rn"));
		let instrumented = crate::rune_instrument::instrument(
			&source,
			&crate::rune_instrument::Vocabulary::from_registry(&["view"]),
		)
		.map_err(|error| locate("<exhibit>", &source, error.span.start, &error.message))?;
		let locator = Locator {
			name: "<exhibit>",
			original: &source,
			generated: &instrumented.source,
			map: Some(&instrumented),
		};
		let program =
			rnx::server::Program::compile_source("<exhibit>", &instrumented.source, extensions())
				.map_err(|failure| locator.describe(&failure))?;
		let mut invocation = program
			.prepare_with(extensions(), handler, Vec::new(), BUDGET)
			.map_err(|failure| locator.describe(&failure))?;
		let result = tokio::runtime::Builder::new_current_thread()
			.build()
			.unwrap()
			.block_on(invocation.run());
		let _ = invocation.close();
		decode_return(result.map_err(|failure| locator.describe(&failure))?)
			.map(Option::unwrap_or_default)
	}

	#[cfg(feature = "toron")]
	const CURVE_FIXTURE: &str = r#"
		// Every maturity closes on three days, except the 30-year on the last.
		fn fixture(ten_year) {
			let fetched = #{};
			for series in SERIES {
				let rows = [];
				for date in ["2026-09-21", "2026-09-22", "2026-09-23"] {
					let missing = series[1] == "30Y" && date == "2026-09-23";
					let value = if missing || (series[1] == "10Y" && !ten_year) { () } else { 4.0 + series[2] / 100.0 };
					rows.push([date, value]);
				}
				fetched[series[1]] = rows;
			}
			fetched
		}
		pub fn latest_and_overlapping() {
			let latest = curve(fixture(true), "latest", "2026-09-23")?;
			let earlier = curve(fixture(true), "earlier", "2026-09-22")?;
			Ok(`${latest[0].curve}|${latest.len()}|${latest[0].observed}|${earlier[0].curve}|${earlier.len()}`)
		}
		pub fn three_curves() { curves(fixture(true))?; Ok("") }
		pub fn no_ten_year() { curves(fixture(false))?; Ok("") }
	"#;

	#[cfg(feature = "toron")]
	#[test]
	fn a_curve_uses_one_complete_day_and_names_it() {
		assert_eq!(
			exhibit(CURVE_FIXTURE, "latest_and_overlapping").unwrap(),
			// The 30-year is missing on the 23rd, so the latest complete curve is
			// the 22nd; a second snapshot resolving to the same day stays its own
			// series under its own name.
			"latest, 2026-09-22|9|2026-09-22|earlier, 2026-09-22|9"
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn a_promised_curve_without_data_fails_by_name() {
		let error = exhibit(CURVE_FIXTURE, "three_curves").unwrap_err();
		assert!(
			error.contains("no complete yield curve on or before 2026-06-23 for \"-3M\""),
			"{error}"
		);
		let error = exhibit(CURVE_FIXTURE, "no_ten_year").unwrap_err();
		assert!(error.contains("no 10-year close"), "{error}");
	}

	#[cfg(feature = "toron")]
	mod views {
		use super::*;
		use crate::plot::spec::Presence;
		use crate::rune_relation_envelope::{RunePlotIntent, RuneViewIntent};

		fn plot(execution: &RuneExecution, set: usize, view: usize) -> &RunePlotIntent {
			match &execution.views[set].views[view].intent {
				RuneViewIntent::Plot(plot) => plot,
				RuneViewIntent::Table(_) => panic!("expected a plot view"),
			}
		}

		fn names(execution: &RuneExecution) -> Vec<String> {
			execution
				.views
				.iter()
				.flat_map(|set| set.views.iter())
				.map(|view| match &view.identity {
					crate::rune_view::LocalViewIdentity::Named(name) => name.clone(),
					other => format!("{other:?}"),
				})
				.collect()
		}

		const ROWS: &str = "let rows = [#{x: 1, y: 2.0, s: \"a\"}, #{x: 2, y: 3.0, s: \"b\"}];";

		#[test]
		fn a_subtitle_computed_from_fetched_data() {
			let base = serve("x,y\n1,2.5\n2,4.25\n");
			let source = format!(
				r#"pub async fn main(workspace, viz) {{
	let body = http::get("{base}/d").await?.body;
	let rows = [];
	for line in body.split("\n") {{
		let parts = [];
		for part in line.split(",") {{ parts.push(part); }}
		if parts.len() == 2 && parts[0] != "x" {{
			rows.push(#{{x: std::f64::parse(parts[0])?, y: std::f64::parse(parts[1])?}});
		}}
	}}
	let last = rows[rows.len() - 1].y;
	let frame = viz.frame("fetched", rows)?;
	frame.views([view("fetched", figure([title("Fetched"), subtitle(`last ${{last}}`), line("x", "y")]))])?;
}}
"#
			);
			let execution = run(&source).expect("local source runs");
			assert_eq!(names(&execution), vec!["fetched"]);
			assert_eq!(
				plot(&execution, 0, 0).authored_settings.subtitle,
				Presence::Show(Arc::new("last 4.25".to_owned()))
			);
		}

		#[test]
		fn a_helper_builds_a_figure_and_a_loop_names_views() {
			let source = format!(
				r#"fn chart(name) {{ figure([title(name), palette("tab20c"), line("x", "y", "s")]) }}
pub fn main(workspace, viz) {{
	{ROWS}
	let frame = viz.frame("f", rows)?;
	let views = [];
	for name in ["one", "two"] {{
		views.push(view(`chart ${{name}}`, chart(name)));
	}}
	frame.views(views)?;
}}
"#
			);
			let execution = run(&source).expect("local source runs");
			assert_eq!(names(&execution), vec!["chart one", "chart two"]);
			assert_eq!(
				plot(&execution, 0, 1).authored_settings.title,
				Presence::Show(Arc::new("two".to_owned()))
			);
			assert_eq!(
				plot(&execution, 0, 0).authored_settings.categorical_palette,
				crate::plot::spec::Override::Set(Arc::new("tab20c".to_owned()))
			);
		}

		#[test]
		fn a_view_not_taken_is_not_queued() {
			let source = format!(
				r#"pub fn main(workspace, viz) {{
	{ROWS}
	let frame = viz.frame("f", rows)?;
	if rows.len() > 10 {{
		frame.views([view("never", figure([line("x", "y")]))])?;
	}}
	frame.views([view("taken", figure([line("x", "y")]))])?;
}}
"#
			);
			assert_eq!(names(&run(&source).unwrap()), vec!["taken"]);
		}

		#[test]
		fn a_duplicate_view_name_is_refused_where_it_is_written() {
			let source = format!(
				r#"pub fn main(workspace, viz) {{
	{ROWS}
	let frame = viz.frame("f", rows)?;
	frame.views([view("same", figure([line("x", "y")]))])?;
	frame.views([view("same", figure([scatter("x", "y")]))])?;
}}
"#
			);
			let error = run(&source).unwrap_err();
			assert!(error.contains("view \"same\" is already defined in this run"), "{error}");
			assert!(error.starts_with("<test>:5:"), "{error}");
		}

		#[test]
		fn presentation_errors_point_at_the_original_source() {
			// A UTF-8 literal and several rewritten calls precede the error.
			let source = r#"pub fn main(workspace, viz) {
	let note = "é → ñ 漢字";
	let rows = [#{x: 1, y: 2.0}];
	let frame = viz.frame("f", rows)?;
	frame.views([view("a", figure([title("t"), subtitle(note), x_axis().label("x").tick_every("soon"), line("x", "y")]))])?;
}
"#;
			let error = run(source).unwrap_err();
			assert!(error.starts_with("<test>:5:"), "{error}");
			assert!(error.contains("tick_every"), "{error}");
			let excerpt = error.lines().nth(1).unwrap_or("");
			assert!(excerpt.contains("tick_every(\"soon\")"), "{error}");

		}

		#[test]
		fn a_script_binding_line_keeps_it_and_the_constructor_still_works() {
			let source = r#"pub fn main(workspace, viz) {
	let rows = [];
	for line in ["1,2", "2,3"] {
		let parts = [];
		for part in line.split(",") { parts.push(part); }
		rows.push(#{x: std::f64::parse(parts[0])?, y: std::f64::parse(parts[1])?});
	}
	viz.frame("f", rows)?.views([view("v", figure([line("x", "y")]))])?;
}
"#;
			assert_eq!(names(&run(source).expect("runs")), vec!["v"]);
		}

		#[test]
		fn the_basic_plot_chain_still_runs_in_a_local_source() {
			let source = format!(
				r#"pub fn main(workspace, viz) {{
	{ROWS}
	viz.plot(viz.frame("f", rows)?).scatter("x", "y").line().show()?;
}}
"#
			);
			let execution = run(&source).expect("the legacy chain runs");
			assert_eq!(execution.requests.len(), 1);
			assert!(execution.views.is_empty());
		}

		#[test]
		fn a_method_receiver_is_evaluated_once_in_either_branch() {
			// `label` is a presentation method name: on an axis it takes the
			// presentation branch, on a user object the original call.
			let source = r#"struct Tag { calls }
impl Tag {
	fn label(self, text) { self.calls.push(text); self }
}
pub fn main(workspace, viz) {
	let log = [];
	let made = || { log.push("made"); Tag { calls: [] } };
	let tag = made().label("user");
	let axis = { log.push("axis"); x_axis() }.label("presentation");
	let frame = viz.frame("f", [#{x: 1, y: 2.0}])?;
	frame.views([view("v", figure([axis, line("x", "y")]))])?;
	Ok(`${log.len()} ${tag.calls[0]}`)
}
"#;
			let execution = run(source).expect("runs");
			assert_eq!(execution.display.as_deref(), Some("2 user"));
			assert_eq!(names(&execution), vec!["v"]);
		}

		#[test]
		fn views_are_refused_outside_a_local_run() {
			let data = polars::df!("x" => [1_i32], "y" => [2_i32]).unwrap();
			let error = crate::rune_host::execute(
				r#"pub fn main(workspace, viz) { workspace.cell("c")?.views([])?; }"#,
				RuneCapabilities {
					cells: [("c".to_owned(), ResultArtifact::from_polars(Arc::new(data)))]
						.into_iter()
						.collect(),
					..RuneCapabilities::default()
				},
			)
			.unwrap_err();
			assert_eq!(error, VIEWS_LOCAL_ONLY);
		}
	}

	#[test]
	fn a_cpu_loop_is_bounded_by_the_budget() {
		let error = run("pub fn main(workspace, viz) { loop {} }").unwrap_err();
		assert!(error.to_lowercase().contains("budget"), "{error}");
	}

	fn object(pairs: &[(&str, Value)]) -> Value {
		let mut object = Object::new();
		for (key, value) in pairs {
			object
				.insert(rune::alloc::String::try_from(*key).unwrap(), value.clone())
				.unwrap();
		}
		rune::to_value(object).unwrap()
	}

	#[test]
	fn missing_keys_and_unit_are_null_never_zero() {
		let rows = vec![
			object(&[("x", Value::from(1_i64)), ("y", Value::from(2.5))]),
			object(&[("x", Value::from(2_i64)), ("y", rune::to_value(()).unwrap())]),
			object(&[("x", Value::from(3_i64))]),
		];
		let frame = frame(&rows).unwrap();
		let y: Vec<Option<f64>> = frame.column("y").unwrap().f64().unwrap().into_iter().collect();
		assert_eq!(y, vec![Some(2.5), None, None]);
		let x: Vec<Option<i64>> = frame.column("x").unwrap().i64().unwrap().into_iter().collect();
		assert_eq!(x, vec![Some(1), Some(2), Some(3)]);
	}

	#[test]
	fn integers_and_floats_widen_to_float() {
		let rows = vec![
			object(&[("v", Value::from(1_i64))]),
			object(&[("v", Value::from(1.5))]),
		];
		let v: Vec<Option<f64>> = frame(&rows).unwrap().column("v").unwrap().f64().unwrap().into_iter().collect();
		assert_eq!(v, vec![Some(1.0), Some(1.5)]);
	}

	#[test]
	fn other_mixes_are_refused_with_rows_named() {
		let rows = vec![
			object(&[("v", Value::from(1_i64))]),
			object(&[("v", Value::from(2.0))]),
			object(&[("v", rune::to_value(String::from("n/a")).unwrap())]),
		];
		let error = frame(&rows).unwrap_err();
		assert_eq!(error, "frame column `v` mixes float (row 0) and string (row 2)");
	}

	fn text(value: &str) -> Value {
		rune::to_value(String::from(value)).unwrap()
	}

	fn schema(pairs: &[(&str, Kind)]) -> BTreeMap<String, Kind> {
		pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
	}

	#[test]
	fn a_declared_date_column_becomes_a_date() {
		let rows = vec![
			object(&[("d", text("2026-09-23")), ("v", Value::from(5.11))]),
			object(&[("d", text("2024-02-29")), ("v", rune::to_value(()).unwrap())]),
			object(&[("d", rune::to_value(()).unwrap()), ("v", Value::from(4.0))]),
		];
		let frame = frame_with(&rows, &schema(&[("d", Kind::Date)])).unwrap();
		let d = frame.column("d").unwrap();
		assert_eq!(d.dtype(), &polars::prelude::DataType::Date);
		let days: Vec<Option<i32>> = d.date().unwrap().physical().into_iter().collect();
		assert_eq!(days, vec![Some(20_719), Some(19_782), None]);
	}

	#[test]
	fn dates_are_never_inferred_from_text() {
		let rows = vec![object(&[("d", text("2026-09-23"))])];
		let frame = frame(&rows).unwrap();
		assert_eq!(frame.column("d").unwrap().dtype(), &polars::prelude::DataType::String);
	}

	#[test]
	fn a_declared_date_refuses_anything_but_strict_iso() {
		for bad in ["2026-9-23", "2026-02-30", "2025-02-29", "23/09/2026", "2026-09-23T00:00"] {
			let rows = vec![object(&[("d", text("2026-09-23"))]), object(&[("d", text(bad))])];
			let error = frame_with(&rows, &schema(&[("d", Kind::Date)])).unwrap_err();
			assert!(error.contains("row 1") && error.contains("not YYYY-MM-DD"), "{bad}: {error}");
		}
		let rows = vec![object(&[("d", Value::from(20_719_i64))])];
		let error = frame_with(&rows, &schema(&[("d", Kind::Date)])).unwrap_err();
		assert_eq!(error, "frame column `d` is declared date but row 0 is integer");
	}

	#[test]
	fn a_declared_column_may_be_entirely_null() {
		let null = || rune::to_value(()).unwrap();
		let rows = vec![
			object(&[("d", null()), ("v", null()), ("x", Value::from(1_i64))]),
			object(&[("d", null()), ("v", null()), ("x", Value::from(2_i64))]),
		];
		let frame = frame_with(&rows, &schema(&[("d", Kind::Date), ("v", Kind::Float)])).unwrap();
		assert_eq!(frame.column("d").unwrap().dtype(), &polars::prelude::DataType::Date);
		assert_eq!(frame.column("v").unwrap().dtype(), &polars::prelude::DataType::Float64);
		assert_eq!(frame.column("d").unwrap().null_count(), 2);
		// Undeclared, the same column still has no type to give.
		let error = frame_with(&rows, &schema(&[("d", Kind::Date)])).unwrap_err();
		assert_eq!(error, "frame column `v` has no values; its type cannot be inferred");
	}

	#[test]
	fn a_schema_must_name_real_columns_with_known_types() {
		let rows = vec![object(&[("x", Value::from(1_i64))])];
		let error = frame_with(&rows, &schema(&[("y", Kind::Float)])).unwrap_err();
		assert_eq!(error, "frame schema names `y`, which no row has");
		let error = frame_with(&rows, &schema(&[("x", Kind::Text)])).unwrap_err();
		assert_eq!(error, "frame column `x` is declared string but row 0 is integer");
		let float = frame_with(&rows, &schema(&[("x", Kind::Float)])).unwrap();
		assert_eq!(float.column("x").unwrap().dtype(), &polars::prelude::DataType::Float64);
	}

	#[test]
	fn all_null_columns_and_non_objects_are_refused() {
		let rows = vec![object(&[("v", rune::to_value(()).unwrap())])];
		assert!(frame(&rows).unwrap_err().contains("has no values"));
		let error = frame(&[Value::from(1_i64)]).unwrap_err();
		assert!(error.starts_with("frame row 0 is"), "{error}");
		assert_eq!(frame(&[]).unwrap_err(), "frame needs at least one row");
	}
}

