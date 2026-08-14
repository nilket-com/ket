//! ADR 551 task-phase receipt publication.

use super::*;
use crate::distributed::task::TaskPhaseReceipt;
use crate::phase_ledger::{LedgerFaults, SealOutcome};

impl Executor {
	pub(super) fn publish_task_phase_receipt(&self) {
		if self.context.task_telemetry.is_none() {
			return;
		}
		match self.context.scan_meter.seal_receipt() {
			SealOutcome::Sealed(totals) => {
				join::record_task_telemetry(&self.context.task_telemetry, |task| {
					let shuffle = task
						.phases
						.shuffle_pull_decode_micros
						.checked_add(totals.shuffle_micros);
					let transport = task
						.phases
						.batch_decode_micros
						.checked_add(totals.transport_micros);
					let (Some(shuffle), Some(transport)) = (shuffle, transport) else {
						let mut faults = LedgerFaults::default();
						faults.arithmetic_overflow = 1;
						task.phase_receipt = TaskPhaseReceipt::Withheld(faults);
						return;
					};

					task.phases.scan_micros = totals.base_scan_micros;
					task.phases.build_micros = totals.exclusive_build_micros;
					task.phases.probe_micros = totals.exclusive_probe_micros;
					task.phases.spill_micros = totals.exclusive_spill_micros;
					task.phases.shuffle_pull_decode_micros = shuffle;
					task.phases.batch_decode_micros = transport;
					task.phase_receipt = TaskPhaseReceipt::Sealed;
				});
			}
			SealOutcome::Withheld(faults) => {
				join::record_task_telemetry(&self.context.task_telemetry, |task| {
					task.phase_receipt = TaskPhaseReceipt::Withheld(faults);
				});
			}
			SealOutcome::AlreadySealed => {}
		}
	}
}
