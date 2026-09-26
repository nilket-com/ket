//! Schemas derive from the wire DTOs, not a second command definition.
use super::{BridgeError, DriveArgs};
use crate::{driver::Command, live};
use schemars::{JsonSchema, generate::SchemaSettings};
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) fn schema<T: JsonSchema>() -> Value {
	let mut settings = SchemaSettings::draft2020_12();
	settings.inline_subschemas = true;
	let mut schema =
		serde_json::to_value(settings.into_generator().into_root_schema_for::<T>()).unwrap();
	integer_bounds(&mut schema);
	schema
}

// Schemars names unsigned Rust widths using format annotations. JSON Schema
// validators need their numeric bounds as assertions as well.
fn integer_bounds(value: &mut Value) {
	match value {
		Value::Object(object) => {
			match object.get("format").and_then(Value::as_str) {
				Some("uint32") => {
					object.insert("maximum".into(), json!(u32::MAX));
				}
				Some("uint64") => {
					object.insert("maximum".into(), json!(u64::MAX));
				}
				_ => {}
			}
			for value in object.values_mut() {
				integer_bounds(value);
			}
		}
		Value::Array(values) => {
			for value in values {
				integer_bounds(value);
			}
		}
		_ => {}
	}
}

pub(super) fn command_schemas() -> BTreeMap<String, Value> {
	let root = schema::<Command>();
	root["oneOf"]
		.as_array()
		.expect("externally tagged Command schema")
		.iter()
		.map(|variant| {
			let (name, args) = variant["properties"]
				.as_object()
				.unwrap()
				.iter()
				.next()
				.unwrap();
			(name.clone(), args.clone())
		})
		.collect()
}

fn description(name: &str) -> &'static str {
	match name {
		"describe" => {
			"Describe current figures, their view names, incarnation IDs, settings and statuses."
		}
		"describe_notebook" => {
			"Describe notebook cells and their buffer/generation identifiers for subsequent cell operations."
		}
		"open_data" => {
			"Open a data file using the window's normal data operation. Relative paths are resolved by the window process."
		}
		"replace_cell" => {
			"Replace a notebook cell's source at the specified buffer and generation."
		}
		"run_cell" => {
			"Execute a notebook cell using the application's execution path. Code may perform filesystem, process and network operations with the user's authority."
		}
		"run_source" => {
			"Run the window's open Rune source and wait for its committed figures. Local Rune has the user's filesystem, process and network authority; this is not a sandbox."
		}
		"hide" => "Hide a series by ordinal in a named view or exact figure incarnation.",
		"show" => "Show a series by ordinal in a named view or exact figure incarnation.",
		"isolate" => "Show only the named series ordinal in the figure.",
		"restore" => "Restore all series in the figure.",
		"resize" => {
			"Resize the figure's native pane, in pixels, using the existing editor operation."
		}
		"publish" => {
			"Publish current non-orphaned figures to a package directory, preserving the publisher's refusal and witness rules. Relative paths are resolved by the window process."
		}
		"connect" => "Connect the window's external adapter using the connection form's fields.",
		"disconnect" => {
			"Disconnect the external adapter at the specified connection generation; stale generations are refused."
		}
		"connection_status" => "Read the window's adapter state and connection generation.",
		_ => panic!("Command {name} has no MCP description/annotations; update the binding"),
	}
}

fn field_description(name: &str) -> &'static str {
	match name {
		"window" => {
			"Advertised 32-character window ID from windows. Omit only when exactly one window is advertised; it is resolved once, never retargeted."
		}
		"figure" => {
			"A stable authored view name, e.g. {\"view\":\"Yields by maturity\"}, or an exact token returned by describe, e.g. {\"incarnation\":\"42\"}."
		}
		"series" => "Series ordinal from describe's series table.",
		"target" => {
			"Cell address from describe_notebook: buffer, document generation, and cell ordinal."
		}
		"buffer" => {
			"Optional notebook buffer ID from describe_notebook; omission selects the active notebook."
		}
		"source" => "Replacement cell source text.",
		"path" => "Data file path; relative to the window process working directory.",
		"directory" => {
			"Output package directory; relative to the window process working directory."
		}
		"width" => "Requested plot area width in pixels.",
		"height" => "Requested plot area height in pixels.",
		"adapter" => "Adapter identifier from the connection form, such as sqlite.",
		"name" => "Connection display name; defaults to empty.",
		"fields" => {
			"Connection form field names and values for this adapter; defaults to an empty object."
		}
		"generation" => {
			"Exact connection generation from connection_status; prevents disconnecting a replacement connection."
		}
		_ => "Argument as decoded by Polariton's command surface.",
	}
}

// A generated root embedded in a union is a subschema, not a new document.
fn output_branch<T: JsonSchema>() -> Value {
	let mut value = schema::<T>();
	let object = value.as_object_mut().unwrap();
	object.remove("$schema");
	object.remove("title");
	value
}

fn terminal_events() -> Value {
	let mut event = output_branch::<live::Event>();
	let variants = event["oneOf"].as_array_mut().expect("tagged Event schema");
	variants.retain_mut(|variant| {
		match variant["properties"]["event"]["const"].as_str().unwrap() {
			"applied" => {
				variant["properties"]["terminal"]["const"] = json!(true);
				true
			}
			"refused" | "completed" | "superseded" | "failed" | "unknown" => true,
			"welcome" | "changed" | "closed" => false,
			name => panic!("Event {name} needs an explicit MCP settlement decision"),
		}
	});
	event
}

pub(super) fn tools() -> Vec<Value> {
	let event = terminal_events();
	let bridge = output_branch::<BridgeError>();
	let output = json!({"type":"object", "anyOf":[event, bridge]});
	let mut tools = Vec::new();
	for (name, mut input) in command_schemas() {
		let read_only = matches!(
			name.as_str(),
			"describe" | "describe_notebook" | "connection_status"
		);
		let open_world = matches!(
			name.as_str(),
			"open_data" | "run_cell" | "run_source" | "publish" | "connect" | "disconnect"
		);
		let mut text = description(&name).to_owned();
		if !read_only {
			text.push_str(" Never automatically retry an unknown outcome: another call is a new request and may repeat effects.");
		}
		if input.get("properties").is_none() {
			input["properties"] = json!({});
		}
		let properties = input["properties"].as_object_mut().unwrap();
		properties.insert("window".into(), json!({"type":"string"}));
		for (field, property) in properties {
			property["description"] = json!(field_description(field));
		}
		tools.push(json!({"name":name,"description":text,"inputSchema":input,"outputSchema":output,
			"annotations":{"readOnlyHint":read_only,"destructiveHint":!read_only,"idempotentHint":false,"openWorldHint":open_world}}));
	}
	tools.push(json!({"name":"windows","description":"List advertised Polariton windows and their identities. Start a live window with polariton --agents <source.rn>, or use drive for a headless batch.",
		"inputSchema":{"type":"object","properties":{},"additionalProperties":false},
		"outputSchema":{"type":"object","anyOf":[output_branch::<super::WindowsResult>(),output_branch::<BridgeError>()]},
		"annotations":{"readOnlyHint":true,"destructiveHint":false,"idempotentHint":false,"openWorldHint":false}}));
	tools.push(json!({"name":"drive","description":"Run a headless Polariton batch in an isolated child process. Source and command paths resolve against the MCP server working directory. Code runs with the user's filesystem, process and network authority. Choose execution explicitly: by-command runs only at run_source; on-open executes on opening. Never retry an unknown result automatically; effects may already have occurred.",
		"inputSchema":schema::<DriveArgs>(),
		"outputSchema":{"type":"object","anyOf":[output_branch::<crate::driver::Receipt>(),output_branch::<BridgeError>()]},
		"annotations":{"readOnlyHint":false,"destructiveHint":true,"idempotentHint":false,"openWorldHint":true}}));
	tools
}

pub(super) fn validators(tools: &[Value]) -> BTreeMap<String, jsonschema::Validator> {
	tools
		.iter()
		.map(|tool| {
			(
				tool["name"].as_str().unwrap().to_owned(),
				jsonschema::validator_for(&tool["inputSchema"])
					.expect("generated input schema is valid"),
			)
		})
		.collect()
}

pub(super) fn decode_command(
	name: &str,
	mut args: Value,
) -> Result<(Option<String>, Command), String> {
	let object = args
		.as_object_mut()
		.ok_or("tool arguments must be an object")?;
	let window = object
		.remove("window")
		.map(|value| serde_json::from_value(value).map_err(|e| e.to_string()))
		.transpose()?;
	let command = serde_json::from_value(json!({name: args})).map_err(|e| e.to_string())?;
	Ok((window, command))
}
