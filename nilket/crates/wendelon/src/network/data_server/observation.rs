// Registration helpers and test-only barriers for mutation ownership.
// These are normal functions: ownership is registered at the caller's original
// entry point, before the returned future is polled or any job is spawned.
use super::*;
use crate::storage::finality_observation::{TableScope, WorkKind, WorkScope, with_work};
use std::future::Future;

impl WendelonDataServer {
	pub(super) fn observe_node_mutation<F: Future>(
		&self,
		future: F,
	) -> impl Future<Output = F::Output> + use<F> {
		let token = self
			.slab
			.finality_observation_registry()
			.begin(WorkScope::Node, WorkKind::Mutation);
		with_work(token, future)
	}

	pub(super) fn observe_tenant_mutation<F: Future>(
		&self,
		tenant: TenantId,
		future: F,
	) -> impl Future<Output = F::Output> + use<F> {
		let token = self
			.slab
			.finality_observation_registry()
			.begin(WorkScope::Tenant(tenant), WorkKind::Mutation);
		with_work(token, future)
	}

	pub(super) fn observe_table_write<F: Future>(
		&self,
		meta: &TableWriteRequestMeta,
		future: F,
	) -> impl Future<Output = F::Output> + use<F> {
		let token = self.slab.finality_observation_registry().begin(
			WorkScope::Table(TableScope {
				tenant: meta.tenant_id,
				object: meta.table_object_id,
				generation: meta.schema_generation,
			}),
			WorkKind::Mutation,
		);
		with_work(token, future)
	}

	pub(super) fn observe_table_delete<F: Future>(
		&self,
		meta: &TableDeleteRowsRequestMeta,
		future: F,
	) -> impl Future<Output = F::Output> + use<F> {
		let token = self.slab.finality_observation_registry().begin(
			WorkScope::Table(TableScope {
				tenant: meta.tenant_id,
				object: meta.table_object_id,
				generation: meta.schema_generation,
			}),
			WorkKind::Mutation,
		);
		with_work(token, future)
	}

	#[cfg(test)]
	pub(super) fn local_append_test_hooks(&self) -> LocalAppendTestHooks {
		// Preserve capture order and timing from the two original spawn sites.
		let failure = self.fail_next_local_durable_append.clone();
		let gate = self
			.local_observation_gate
			.lock()
			.expect("local observation gate poisoned")
			.take();
		LocalAppendTestHooks { failure, gate }
	}
}

#[cfg(test)]
pub(super) struct LocalAppendTestHooks {
	failure: Arc<StdMutex<Option<(ErrorKind, String)>>>,
	gate: Option<(Arc<Notify>, Arc<Notify>)>,
}

#[cfg(test)]
impl LocalAppendTestHooks {
	pub(super) async fn before_append(self) -> std::io::Result<()> {
		if let Some((entered, release)) = self.gate {
			entered.notify_one();
			release.notified().await;
		}
		if let Some((kind, message)) = self
			.failure
			.lock()
			.expect("local durable append failpoint poisoned")
			.take()
		{
			return Err(Error::new(
				kind,
				format!("injected local durable append failure: {message}"),
			));
		}
		Ok(())
	}
}

#[cfg(test)]
impl WendelonDataServer {
	#[cfg(test)]
	pub(crate) fn fail_next_local_durable_append_for_test(
		&self,
		kind: ErrorKind,
		message: impl Into<String>,
	) {
		*self
			.fail_next_local_durable_append
			.lock()
			.expect("local durable append failpoint poisoned") = Some((kind, message.into()));
	}
}
