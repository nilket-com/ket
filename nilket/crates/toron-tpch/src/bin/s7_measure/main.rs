//! `s7-measure`: the §8 measurement harness (ADR 0696 fast follow, design r2
//! with the Codex r1/r2 rulings). One source, built onto both the base archive
//! 9e7e3ad1 and the fix tree; `--cfg s7_fix` selects only the fix-only
//! configuration field and instrument reset. Workload, pacing, topology,
//! dataset and collection are common to both arms.
//!
//! ```text
//! s7-measure node     --manifest <json> --block-dir <dir>
//! s7-measure drive    --manifest <json> --block-dir <dir> --node-pid <pid>
//! s7-measure evaluate --manifest <json> --block-dir <dir> --arm base|fix
//! s7-measure deadline --manifest <json>          # the driver's end-to-end bound, secs
//! s7-measure compare  --blocks <dir>[,<dir>...]  # cross-block compatibility
//! ```

mod common;
mod compare;
mod drive;
mod evaluate;
mod manifest;
mod node;
mod pki;
mod prom;

use std::path::PathBuf;

fn usage() -> ! {
	eprintln!(
		"usage: s7-measure node --manifest M --block-dir D\n\
		 \x20      s7-measure drive --manifest M --block-dir D --node-pid PID\n\
		 \x20      s7-measure evaluate --manifest M --block-dir D --arm base|fix\n\
		 \x20      s7-measure deadline --manifest M\n\
		 \x20      s7-measure compare --blocks D1,D2,..."
	);
	std::process::exit(2)
}

struct Args {
	manifest: PathBuf,
	block_dir: PathBuf,
	node_pid: Option<u32>,
	arm: Option<String>,
	blocks: Vec<PathBuf>,
}

fn parse(args: &[String]) -> Args {
	let mut out = Args {
		manifest: PathBuf::new(),
		block_dir: PathBuf::new(),
		node_pid: None,
		arm: None,
		blocks: Vec::new(),
	};
	let mut it = args.iter();
	while let Some(flag) = it.next() {
		let value = it.next().unwrap_or_else(|| usage());
		match flag.as_str() {
			"--manifest" => out.manifest = value.into(),
			"--block-dir" => out.block_dir = value.into(),
			"--node-pid" => out.node_pid = Some(value.parse().unwrap_or_else(|_| usage())),
			"--arm" => out.arm = Some(value.clone()),
			"--blocks" => out.blocks = value.split(',').map(PathBuf::from).collect(),
			_ => usage(),
		}
	}
	out
}

fn require(args: &Args, manifest: bool, block: bool) {
	if (manifest && args.manifest.as_os_str().is_empty())
		|| (block && args.block_dir.as_os_str().is_empty())
	{
		usage();
	}
}

fn runtime() -> tokio::runtime::Runtime {
	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.thread_stack_size(16 * 1024 * 1024)
		.build()
		.expect("tokio runtime")
}

fn main() {
	let argv: Vec<String> = std::env::args().skip(1).collect();
	let Some((command, rest)) = argv.split_first() else {
		usage()
	};
	let args = parse(rest);
	let result = match command.as_str() {
		"node" | "drive" | "evaluate" => {
			require(&args, true, true);
			Ok(())
		}
		"deadline" => {
			require(&args, true, false);
			Ok(())
		}
		"compare" => Ok(()),
		_ => usage(),
	};
	let result = result.and_then(|()| match command.as_str() {
		"node" => {
			let rt = runtime();
			rt.block_on(async {
				tokio::spawn(async move { node::run(&args.manifest, &args.block_dir).await })
					.await
					.map_err(std::io::Error::other)?
			})
		}
		"drive" => {
			let pid = args.node_pid.unwrap_or_else(|| usage());
			let rt = runtime();
			rt.block_on(async {
				tokio::spawn(async move { drive::run(&args.manifest, &args.block_dir, pid).await })
					.await
					.map_err(std::io::Error::other)?
			})
		}
		"evaluate" => run_evaluate(&args),
		"deadline" => manifest::Manifest::load(&args.manifest).map(|(m, _)| {
			println!("{}", m.driver_deadline_secs());
		}),
		"compare" => run_compare(&args),
		_ => usage(),
	});
	if let Err(error) = result {
		eprintln!("s7-measure {command}: {error}");
		std::process::exit(1);
	}
}

fn run_evaluate(args: &Args) -> std::io::Result<()> {
	let arm = args.arm.as_deref().unwrap_or_else(|| usage());
	if arm != "base" && arm != "fix" {
		usage();
	}
	let (m, manifest_bytes) = manifest::Manifest::load(&args.manifest)?;
	let manifest_sha256 = common::sha256_hex(&manifest_bytes);
	let block = common::BlockPaths::new(&args.block_dir);
	// Missing inputs are evaluated as missing (INVALID), never skipped.
	let block_id = block
		.block_id()
		.unwrap_or_else(|e| format!("<block id unreadable: {e}>"));
	let obs = std::fs::read_to_string(block.observations()).unwrap_or_default();
	let driver: serde_json::Value = common::read_json(&block.driver_record()).unwrap_or_else(
		|e| serde_json::json!({"aborted": format!("driver record unreadable: {e}")}),
	);
	let process: serde_json::Value = common::read_json(&block.process())
		.unwrap_or_else(|e| serde_json::json!({"unreadable": e.to_string()}));
	let batches = std::fs::read_to_string(block.driver_batches()).unwrap_or_default();
	let mut verdict = evaluate::evaluate(&evaluate::Inputs {
		arm,
		manifest: &m,
		manifest_sha256: &manifest_sha256,
		block_id: &block_id,
		observations: &obs,
		driver: &driver,
		batches: &batches,
		process: &process,
	});
	verdict["binary_arm"] = serde_json::json!(common::ARM);
	common::write_json_atomic(&block.verdict(), &verdict)?;
	println!(
		"{}",
		serde_json::to_string_pretty(&serde_json::json!({
			"verdict": verdict["verdict"],
			"label": verdict["label"],
			"invalid_reasons": verdict["invalid_reasons"],
			"failed_predicates": verdict["failed_predicates"],
		}))
		.unwrap()
	);
	Ok(())
}

fn run_compare(args: &Args) -> std::io::Result<()> {
	if args.blocks.is_empty() {
		usage();
	}
	let mut blocks = Vec::new();
	for dir in &args.blocks {
		let paths = common::BlockPaths::new(dir);
		let read = |p: PathBuf| common::read_json(&p).unwrap_or(serde_json::Value::Null);
		blocks.push(compare::Block {
			name: dir.display().to_string(),
			driver: read(paths.driver_record()),
			effective_config: read(paths.effective_config()),
		});
	}
	let report = compare::report(&blocks);
	println!("{}", serde_json::to_string_pretty(&report).unwrap());
	if report["compatible"] == true {
		Ok(())
	} else {
		Err(std::io::Error::other("blocks are not comparable"))
	}
}
