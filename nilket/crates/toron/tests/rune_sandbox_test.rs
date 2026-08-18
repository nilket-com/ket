use std::time::Duration;

use toron::error::FrontendError;
use toron::ir::LogicalPlan;
use toron::rune_engine::{CapabilityCategory, RuneEngine, RuneSandboxConfig, RuneSandboxViolation};

fn sandbox_config() -> RuneSandboxConfig {
	RuneSandboxConfig {
		wall_timeout: Duration::from_secs(2),
		..RuneSandboxConfig::v1_default()
	}
}

fn sandbox_error(error: FrontendError) -> RuneSandboxViolation {
	match error {
		FrontendError::RuneSandbox { kind, .. } => kind,
		other => panic!("expected RuneSandbox error, got {other:?}"),
	}
}

fn count_script() -> &'static str {
	r#"
        pub fn main(ctx) {
            ctx.table("sales")
               .filter(col("amount").gt(lit(100)))
               .count()
        }
    "#
}

#[tokio::test]
async fn rune_sandbox_allows_m7_authoring_surface() {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");

	let plan = engine
		.execute_script(count_script(), &config)
		.await
		.expect("execute");

	assert!(matches!(plan, LogicalPlan::Aggregate { .. }));
}

#[tokio::test]
async fn capability_classifier_ignores_string_literals_on_allowed_path() {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");
	let script = r#"
        pub fn main(ctx) {
            ctx.table("std::fs").count()
        }
    "#;

	let plan = engine
		.execute_script(script, &config)
		.await
		.expect("execute");

	assert!(matches!(plan, LogicalPlan::Aggregate { .. }));
}

#[tokio::test]
async fn source_too_large_rejected_before_compile() {
	let config = RuneSandboxConfig {
		max_source_bytes: 8,
		..sandbox_config()
	};
	let engine = RuneEngine::with_config(config).expect("engine");

	let error = engine
		.execute_script(count_script(), &config)
		.await
		.expect_err("oversized source should fail");

	assert!(matches!(
		sandbox_error(error),
		RuneSandboxViolation::SourceTooLarge { bytes, budget }
			if bytes > budget && budget == config.max_source_bytes
	));
}

#[tokio::test]
async fn infinite_loop_exhausts_fuel() {
	let config = RuneSandboxConfig {
		fuel_budget: 256,
		wall_timeout: Duration::from_secs(2),
		..sandbox_config()
	};
	let engine = RuneEngine::with_config(config).expect("engine");
	let script = r#"
        pub fn main(ctx) {
            loop {}
        }
    "#;

	let result = tokio::time::timeout(
		Duration::from_secs(3),
		engine.execute_script(script, &config),
	)
	.await
	.expect("fuel exhaustion should not hang");
	let error = result.expect_err("infinite loop should exhaust fuel");

	assert!(matches!(
		sandbox_error(error),
		RuneSandboxViolation::FuelExhausted {
			fuel_consumed,
			fuel_budget
		} if fuel_consumed == config.fuel_budget && fuel_budget == config.fuel_budget
	));
}

#[tokio::test]
async fn memory_budget_breach_maps_to_typed_error() {
	let config = RuneSandboxConfig {
		memory_budget: 1,
		..sandbox_config()
	};
	let engine = RuneEngine::with_config(config).expect("engine");

	let error = engine
		.execute_script(count_script(), &config)
		.await
		.expect_err("tiny memory budget should fail");

	assert!(matches!(
		sandbox_error(error),
		RuneSandboxViolation::MemoryExhausted {
			bytes_used,
			bytes_budget
		} if bytes_used == config.memory_budget && bytes_budget == config.memory_budget
	));
}

#[tokio::test]
async fn memory_bomb_terminates_with_typed_error() {
	let config = RuneSandboxConfig {
		fuel_budget: 1_000_000,
		memory_budget: 512 * 1024,
		wall_timeout: Duration::from_secs(2),
		..sandbox_config()
	};
	let engine = RuneEngine::with_config(config).expect("engine");
	let script = r#"
        pub fn main(ctx) {
            let values = [];

            loop {
                values.push("01234567890123456789012345678901234567890123456789");
            }
        }
    "#;

	let result = tokio::time::timeout(
		Duration::from_secs(3),
		engine.execute_script(script, &config),
	)
	.await
	.expect("memory cap should prevent host OOM or hang");
	let error = result.expect_err("memory bomb should hit typed memory cap");

	assert!(matches!(
		sandbox_error(error),
		RuneSandboxViolation::MemoryExhausted {
			bytes_used,
			bytes_budget
		} if bytes_used == config.memory_budget && bytes_budget == config.memory_budget
	));
}

#[tokio::test]
async fn forbidden_capabilities_return_typed_denials() {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");
	let cases = [
		(
			CapabilityCategory::Filesystem,
			r#"pub fn main(ctx) { std::fs::read("/tmp/x"); ctx.table("sales").count() }"#,
		),
		(
			CapabilityCategory::Process,
			r#"pub fn main(ctx) { std::process::Command::new("sh"); ctx.table("sales").count() }"#,
		),
		(
			CapabilityCategory::Network,
			r#"pub fn main(ctx) { std::net::TcpStream::connect("127.0.0.1:1"); ctx.table("sales").count() }"#,
		),
		(
			CapabilityCategory::Environment,
			r#"pub fn main(ctx) { std::env::var("HOME"); ctx.table("sales").count() }"#,
		),
		(
			CapabilityCategory::Time,
			r#"pub fn main(ctx) { std::time::SystemTime::now(); ctx.table("sales").count() }"#,
		),
		(
			CapabilityCategory::Random,
			r#"pub fn main(ctx) { rand::random(); ctx.table("sales").count() }"#,
		),
		(
			CapabilityCategory::Threading,
			r#"pub fn main(ctx) { std::thread::spawn(|| {}); ctx.table("sales").count() }"#,
		),
		(
			CapabilityCategory::Unknown,
			r#"pub fn main(ctx) { std::unknown::call(); ctx.table("sales").count() }"#,
		),
	];

	for (capability, script) in cases {
		let error = engine
			.execute_script(script, &config)
			.await
			.expect_err("forbidden capability should fail");

		assert!(matches!(
			sandbox_error(error),
			RuneSandboxViolation::CapabilityDenied { capability: actual }
				if actual == capability
		));
	}
}

/// A type mistake is not a security event. `lit` takes an `i32`, so passing a
/// float produces `Expected number type, but found ::std::f64` — a message that
/// names a std path without the script having reached for any capability.
/// Reporting that as a denial tells an author to look for a sandbox problem
/// they do not have, and buries a real one in noise.
#[tokio::test]
async fn an_ordinary_type_error_is_not_a_capability_denial() {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");
	let cases = [
		r#"pub fn main(ctx) { ctx.table("sales").project([round(col("price"), lit(0.0))]) }"#,
		r#"pub fn main(ctx) { ctx.table("sales").project([lit_str(7)]) }"#,
	];

	for script in cases {
		let error = engine
			.execute_script(script, &config)
			.await
			.expect_err("a type mistake should still fail");
		assert!(
			!matches!(
				error,
				FrontendError::RuneSandbox {
					kind: RuneSandboxViolation::CapabilityDenied { .. },
					..
				}
			),
			"{script} is a type error, not a denied capability: {error:?}"
		);
	}
}

/// ADR 553 registered a `random()` scalar function, which collides with the
/// classifier's old `random(` denial pattern: a supported call appearing in any
/// error message would have been reported as a reached-for capability. The
/// signal is a foreign path, and `rand::random()` must still be denied.
#[tokio::test]
async fn the_engine_s_own_random_function_is_not_a_denied_capability() {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");

	let error = engine
		.execute_script(
			r#"pub fn main(ctx) { ctx.table("sales").project([random(7)]) }"#,
			&config,
		)
		.await
		.expect_err("a wrong-arity call should still fail");
	assert!(
		!matches!(
			error,
			FrontendError::RuneSandbox {
				kind: RuneSandboxViolation::CapabilityDenied { .. },
				..
			}
		),
		"calling the engine's own random() wrongly is a type error: {error:?}"
	);

	let denied = engine
		.execute_script(
			r#"pub fn main(ctx) { rand::random(); ctx.table("sales").count() }"#,
			&config,
		)
		.await
		.expect_err("reaching for the rand crate must still be denied");
	assert!(matches!(
		sandbox_error(denied),
		RuneSandboxViolation::CapabilityDenied {
			capability: CapabilityCategory::Random
		}
	));
}

#[tokio::test]
async fn invalid_entrypoint_returns_typed_error() {
	let config = sandbox_config();
	let engine = RuneEngine::with_config(config).expect("engine");
	let script = r#"
        pub fn not_main(ctx) {
            ctx.table("sales").count()
        }
    "#;

	let error = engine
		.execute_script(script, &config)
		.await
		.expect_err("missing main should fail");

	assert!(matches!(
		sandbox_error(error),
		RuneSandboxViolation::InvalidEntrypoint { .. }
	));
}
