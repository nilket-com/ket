//! ADR 0726: the root check, the binding cell, admission, the fence and the
//! delivery order. Every root here is an ephemeral or per-test development
//! root; no key material is printed.
use super::*;

const TENANT: TenantId = TenantId::ZERO;

fn unverified() -> CryptoManager {
	CryptoManager::new(Arc::new(
		DevRootKekProvider::new_ephemeral_for_tests().unwrap(),
	))
}

fn authority(
	applied_index: u64,
	root_check: Option<&RootCheck>,
	keysets: &HashMap<TenantId, TenantKeyset>,
) -> KeyAuthority {
	KeyAuthority {
		applied_index,
		root_check: root_check.cloned(),
		keysets: keysets.clone(),
	}
}

fn keysets_of(rotation: &InstallTenantKekVersion) -> HashMap<TenantId, TenantKeyset> {
	let mut keyset = TenantKeyset::default();
	keyset.active_tenant_kek_id = Some(rotation.tenant_kek_id);
	keyset.keys.insert(
		rotation.tenant_kek_id,
		WrappedTenantKek {
			wrapped_key: rotation.wrapped_tenant_kek.clone(),
		},
	);
	HashMap::from([(rotation.tenant_id, keyset)])
}

/// A manager verified against its own check at index 1, with one tenant KEK
/// delivered at index 2.
fn seeded() -> (CryptoManager, RootCheck, HashMap<TenantId, TenantKeyset>) {
	let manager = unverified();
	let check = manager.candidate_root_check().unwrap();
	manager
		.sync_key_authority(&authority(1, Some(&check), &HashMap::new()))
		.unwrap();
	let keysets = keysets_of(&manager.prepare_rotation(TENANT).unwrap());
	manager
		.sync_key_authority(&authority(2, Some(&check), &keysets))
		.unwrap();
	(manager, check, keysets)
}

fn encrypt(
	manager: &CryptoManager,
	slab: u64,
	plaintext: &[u8],
) -> std::io::Result<EncryptedTieredSlab> {
	manager.encrypt_tiered_slab_object(
		SlabId::new(slab),
		&TieredSlabObjectMetadata {
			tenant_id: TENANT,
			created_hlc: 0,
			chunk_size_bytes: SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE,
			logical_base: 0,
			offset_index: vec![TieredOffsetIndexEntry {
				key_locator: crate::schema::IdentityPrefix::ZERO,
				byte_offset: 0,
				byte_length: u32::try_from(plaintext.len()).unwrap(),
				flags: 0,
			}],
			column_stats_block: None,
			has_row_tombstone_records: false,
		},
		plaintext,
	)
}

fn refusal<T>(result: std::io::Result<T>) -> RootBindingRefusal {
	let error = result.err().expect("the operation must be refused");
	RootBindingRefusal::of(&error).unwrap_or_else(|| panic!("not a binding refusal: {error}"))
}

fn cached_tenant_keks(manager: &CryptoManager) -> usize {
	manager
		.admit()
		.unwrap()
		.tenant_kek_cache
		.lock()
		.unwrap()
		.entries
		.len()
}

#[test]
fn the_root_check_encoding_is_pinned() {
	assert_eq!(
		&ROOT_CHECK_PLAINTEXT_V1,
		b"wendelon root check plaintext v1"
	);
	assert_eq!(ROOT_CHECK_VERSION, 1);
	let manager = unverified();
	let check = manager.candidate_root_check().unwrap();
	assert_eq!(check.version, 1);
	assert_eq!(check.wrapped.len(), ROOT_CHECK_WRAPPED_BYTES);
	assert_eq!(ROOT_CHECK_WRAPPED_BYTES, 40);
	// id = SHA-256(big-endian version || wrapped)[..16], computed here by hand.
	let mut input = vec![0x00, 0x01];
	input.extend_from_slice(&check.wrapped);
	let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &input);
	assert_eq!(check.id().as_bytes(), &digest.as_ref()[..16]);
	// The check is a function of the root: the same root reproduces it, another
	// root does not.
	assert_eq!(manager.candidate_root_check().unwrap(), check);
	assert_ne!(unverified().candidate_root_check().unwrap(), check);
	// The plaintext is the prescribed constant.
	let unwrapped = manager.provider.unwrap_tenant_kek(&check.wrapped).unwrap();
	assert_eq!(unwrapped.as_ref(), &ROOT_CHECK_PLAINTEXT_V1);
}

#[test]
fn a_root_check_of_another_version_or_size_is_refused_before_verification() {
	let manager = unverified();
	let good = manager.candidate_root_check().unwrap();
	let other_version = RootCheck {
		version: 2,
		..good.clone()
	};
	let short = RootCheck {
		version: 1,
		wrapped: good.wrapped[..39].to_vec(),
	};
	for bad in [&other_version, &short] {
		assert!(bad.validate().is_err());
		let error = verify_root_check(manager.provider.as_ref(), bad).unwrap_err();
		assert_eq!(error.kind(), ErrorKind::InvalidData);
		// Delivered, it is an error and the manager stays fenced: not verified,
		// and not reported as a wrong key either.
		let fresh = unverified();
		assert!(
			fresh
				.sync_key_authority(&authority(1, Some(bad), &HashMap::new()))
				.is_err()
		);
		assert_eq!(fresh.root_binding_status(), RootBindingStatus::Pending);
	}
	// Right version and size, wrong bytes: a definitive mismatch, not an error.
	assert!(!verify_root_check(manager.provider.as_ref(), &test_root_check()).unwrap());
	assert!(verify_root_check(manager.provider.as_ref(), &good).unwrap());
}

#[test]
fn an_unverified_manager_offers_only_its_candidate() {
	let manager = unverified();
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
	manager.candidate_root_check().unwrap();
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
	assert_eq!(
		refusal(manager.prepare_rotation(TENANT)),
		RootBindingRefusal::NotVerified
	);
	assert_eq!(
		refusal(encrypt(&manager, 1, b"x")),
		RootBindingRefusal::NotVerified
	);
	// Pending is retryable to the storage layer; a mismatch is not.
	let pending = encrypt(&manager, 1, b"x").unwrap_err().kind();
	assert_eq!(pending, ErrorKind::WouldBlock);
	assert!(crate::storage::manager::tier_authority::transient_tier_error(pending));
	let definitive = RootBindingRefusal::Mismatch.into_io().kind();
	assert_eq!(definitive, ErrorKind::PermissionDenied);
	assert!(!crate::storage::manager::tier_authority::transient_tier_error(definitive));
	assert_eq!(
		refusal(manager.tenant_keysets_snapshot()),
		RootBindingRefusal::NotVerified
	);
	assert_eq!(
		refusal(manager.active_tenant_kek_id(TENANT)),
		RootBindingRefusal::NotVerified
	);
	assert_eq!(
		refusal(manager.verified_root_check_id()),
		RootBindingRefusal::NotVerified
	);
	// No check in the delivered state is still pending, never a mismatch.
	manager
		.sync_key_authority(&authority(1, None, &HashMap::new()))
		.unwrap();
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
}

#[test]
fn a_root_that_does_not_reproduce_the_check_is_a_mismatch_and_refuses() {
	let (cluster, check, keysets) = seeded();
	let encrypted = encrypt(&cluster, 7, b"cluster bytes").unwrap();
	let stranger = unverified();
	stranger
		.sync_key_authority(&authority(2, Some(&check), &keysets))
		.unwrap();
	assert_eq!(
		stranger.root_binding_status(),
		RootBindingStatus::Mismatch(check.id())
	);
	assert_eq!(
		refusal(stranger.prepare_rotation(TENANT)),
		RootBindingRefusal::Mismatch
	);
	assert_eq!(
		refusal(encrypt(&stranger, 8, b"x")),
		RootBindingRefusal::Mismatch
	);
	assert_eq!(
		refusal(stranger.decrypt_tiered_slab(SlabId::new(7), &encrypted.bytes)),
		RootBindingRefusal::Mismatch
	);
	assert!(!stranger.still_current(encrypted.binding_serial));
	// The cluster's own member is verified and reads it.
	assert_eq!(
		cluster.root_binding_status(),
		RootBindingStatus::Verified(check.id())
	);
	assert_eq!(
		cluster
			.decrypt_tiered_slab(SlabId::new(7), &encrypted.bytes)
			.unwrap()
			.plaintext,
		b"cluster bytes"
	);
}

/// T-R1a. A read served from the key caches is refused once a different check
/// has been delivered, and so is a new plan; an identical check keeps serving.
#[test]
fn a_cached_key_is_unreachable_after_a_different_check_is_delivered() {
	for different in [false, true] {
		let (manager, check, keysets) = seeded();
		let encrypted = encrypt(&manager, 9, b"cached slab").unwrap();
		manager
			.decrypt_tiered_slab(SlabId::new(9), &encrypted.bytes)
			.unwrap();
		// The tenant KEK and the slab DEK are both cached now: this read is a hit.
		assert_eq!(cached_tenant_keks(&manager), 1);
		let before = manager.admit().unwrap();
		manager
			.decrypt_tiered_slab(SlabId::new(9), &encrypted.bytes)
			.unwrap();
		let other = unverified().candidate_root_check().unwrap();
		let delivered = if different { &other } else { &check };
		manager
			.sync_key_authority(&authority(3, Some(delivered), &keysets))
			.unwrap();
		let read = manager.decrypt_tiered_slab(SlabId::new(9), &encrypted.bytes);
		let plan = manager.plan_tiered_range_read(
			SlabId::new(9),
			encrypted.bytes.len() as u64,
			&encrypted.bytes,
			None,
			LogicalExtent::new(0, 4),
		);
		if different {
			assert_eq!(refusal(read), RootBindingRefusal::Mismatch);
			assert_eq!(refusal(plan), RootBindingRefusal::Mismatch);
			assert!(!manager.still_current(encrypted.binding_serial));
			// The old binding still holds its cache; nothing new can reach it.
			assert_eq!(before.tenant_kek_cache.lock().unwrap().entries.len(), 1);
		} else {
			assert_eq!(read.unwrap().plaintext, b"cached slab");
			// Whatever the plan's own outcome, it was admitted.
			assert!(
				plan.err()
					.and_then(|e| RootBindingRefusal::of(&e))
					.is_none()
			);
		}
	}
}

/// T-R1b, variants 1 and 2. An operation admitted before a binding change
/// finishes under its binding; its late cache insert is invisible to every
/// later operation; an upload admitted before is refused at `still_current`.
#[test]
fn work_admitted_before_a_binding_change_finishes_and_new_work_sees_only_the_new_binding() {
	for different_check in [false, true] {
		let (manager, check, keysets) = seeded();
		let encrypted = encrypt(&manager, 11, b"interleaved").unwrap();
		// A fresh binding with empty caches, so operation A must unwrap.
		manager
			.sync_key_authority(&authority(3, Some(&check), &keysets))
			.unwrap();
		let upload = encrypt(&manager, 12, b"admitted upload").unwrap();
		let old = manager.admit().unwrap();
		assert!(manager.still_current(upload.binding_serial));
		old.tenant_kek_cache.lock().unwrap().clear();
		old.slab_dek_cache.lock().unwrap().clear();

		let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
		let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
		binding_test_hooks::on_next_unwrap_before_insert(
			old.serial,
			Box::new(move || {
				reached_tx.send(()).unwrap();
				resume_rx.recv().unwrap();
			}),
		);
		let reader = manager.clone();
		let bytes = encrypted.bytes.clone();
		let a = std::thread::spawn(move || reader.decrypt_tiered_slab(SlabId::new(11), &bytes));
		reached_rx.recv().unwrap();

		// A holds the old binding, has unwrapped, and has not inserted yet.
		let other = unverified().candidate_root_check().unwrap();
		let delivered = if different_check { &other } else { &check };
		manager
			.sync_key_authority(&authority(4, Some(delivered), &keysets))
			.unwrap();
		resume_tx.send(()).unwrap();
		assert_eq!(a.join().unwrap().unwrap().plaintext, b"interleaved");
		// A's insert landed in the old binding only.
		assert_eq!(old.tenant_kek_cache.lock().unwrap().entries.len(), 1);
		assert!(!manager.still_current(upload.binding_serial));

		let b = manager.decrypt_tiered_slab(SlabId::new(11), &encrypted.bytes);
		if different_check {
			assert_eq!(refusal(b), RootBindingRefusal::Mismatch);
		} else {
			let current = manager.admit().unwrap();
			assert_ne!(current.serial, old.serial);
			assert_eq!(b.unwrap().plaintext, b"interleaved");
			// B unwrapped for itself: it could not see A's entry.
			assert_eq!(current.tenant_kek_cache.lock().unwrap().entries.len(), 1);
		}
	}
}

#[test]
fn a_stale_delivery_never_replaces_newer_authority() {
	let (manager, check, older) = seeded();
	let newer = keysets_of(&manager.prepare_rotation(TENANT).unwrap());
	assert_ne!(older, newer);
	manager
		.sync_key_authority(&authority(10, Some(&check), &newer))
		.unwrap();
	// An intermediate-index delivery of the same check arrives late.
	manager
		.sync_key_authority(&authority(7, Some(&check), &older))
		.unwrap();
	assert_eq!(manager.tenant_keysets_snapshot().unwrap(), newer);
	// The same index again changes nothing either.
	manager
		.sync_key_authority(&authority(10, Some(&check), &older))
		.unwrap();
	assert_eq!(manager.tenant_keysets_snapshot().unwrap(), newer);
	// A state with no check is accepted and advances the index like any other.
	manager
		.sync_key_authority(&authority(12, None, &HashMap::new()))
		.unwrap();
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
	manager
		.sync_key_authority(&authority(11, Some(&check), &newer))
		.unwrap();
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
	manager
		.sync_key_authority(&authority(13, Some(&check), &newer))
		.unwrap();
	assert_eq!(
		manager.root_binding_status(),
		RootBindingStatus::Verified(check.id())
	);
}

#[test]
fn a_delayed_verifier_does_not_install_over_a_later_fence() {
	for fenced_meanwhile in [false, true] {
		let manager = unverified();
		let check = manager.candidate_root_check().unwrap();
		if fenced_meanwhile {
			binding_test_hooks::on_next_verified_install(
				&manager,
				Box::new(|manager| manager.fence_before_publication(None, 6)),
			);
		}
		manager
			.sync_key_authority(&authority(5, Some(&check), &HashMap::new()))
			.unwrap();
		if fenced_meanwhile {
			assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
			assert_eq!(
				refusal(manager.prepare_rotation(TENANT)),
				RootBindingRefusal::NotVerified
			);
		} else {
			assert_eq!(
				manager.root_binding_status(),
				RootBindingStatus::Verified(check.id())
			);
		}
	}
}

#[test]
fn the_fence_removes_only_a_binding_of_another_root_and_blocks_older_deliveries() {
	let (manager, check, keysets) = seeded();
	let serial = manager.admit().unwrap().serial;
	// A publication of the same check leaves the binding in place.
	manager.fence_before_publication(Some(check.id()), 20);
	assert_eq!(manager.admit().unwrap().serial, serial);
	// A publication of another check, or of none, removes it at once.
	let other = unverified().candidate_root_check().unwrap();
	manager.fence_before_publication(Some(other.id()), 21);
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
	assert_eq!(
		refusal(manager.prepare_rotation(TENANT)),
		RootBindingRefusal::NotVerified
	);
	// A delivery from before the fence installs nothing.
	manager
		.sync_key_authority(&authority(20, Some(&check), &keysets))
		.unwrap();
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
	// The delivery of the fenced publication itself is verified as usual.
	manager
		.sync_key_authority(&authority(21, Some(&check), &keysets))
		.unwrap();
	assert_eq!(
		manager.root_binding_status(),
		RootBindingStatus::Verified(check.id())
	);
	manager.fence_before_publication(None, 22);
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
}

#[test]
fn a_poisoned_cell_refuses_admission_and_still_takes_the_fence() {
	let (manager, check, keysets) = seeded();
	let poisoner = manager.clone();
	let _ = std::thread::spawn(move || {
		let _guard = poisoner.cell.write().unwrap();
		panic!("poison the binding cell");
	})
	.join();
	assert_eq!(
		refusal(manager.prepare_rotation(TENANT)),
		RootBindingRefusal::NotVerified
	);
	assert_eq!(manager.root_binding_status(), RootBindingStatus::Pending);
	manager.fence_before_publication(None, 30);
	assert!(
		manager
			.sync_key_authority(&authority(31, Some(&check), &keysets))
			.is_err()
	);
	assert_eq!(
		refusal(encrypt(&manager, 1, b"x")),
		RootBindingRefusal::NotVerified
	);
}

/// The enrollment procedure on real data directories, for the development and
/// the local-sealed providers: generate, adopt, enroll, and every refusal.
#[test]
fn provider_open_rules_generate_only_when_asked_and_never_treat_a_bad_artifact_as_absent() {
	let root = crate::test_support::TestDir::new("root-open-rules");
	let secret = root.to_path_buf().join("unlock");
	std::fs::write(&secret, b"synthetic unlock secret, per test").unwrap();
	let other_secret = root.to_path_buf().join("unlock-other");
	std::fs::write(&other_secret, b"another synthetic unlock secret").unwrap();
	let sealed = |file: &Path| {
		RootKekProviderConfig::LocalSealed(LocalSealedRootKekConfig {
			unlock_secret_env: None,
			unlock_secret_file: Some(file.to_path_buf()),
		})
	};
	let open = |config: &RootKekProviderConfig, dir: &Path, mode| {
		config.initialize(RootKekProviderInit {
			data_dir: dir,
			bootstrap_mode: mode,
			owner: None,
		})
	};
	let check_of =
		|provider: &Arc<dyn RootKekProvider>| compute_root_check(provider.as_ref()).unwrap();
	for config in [RootKekProviderConfig::DevInsecure, sealed(&secret)] {
		let label = config.label();
		let forming = root.to_path_buf().join(format!("{label}-forming"));
		let member = root.to_path_buf().join(format!("{label}-member"));
		let empty = root.to_path_buf().join(format!("{label}-empty"));
		for dir in [&forming, &member, &empty] {
			std::fs::create_dir_all(dir).unwrap();
		}
		// Enroll-only on an empty directory: refused, and nothing is created.
		let error = open(&config, &empty, RootKekBootstrapMode::EnrollExisting)
			.err()
			.expect("enroll-only never generates");
		assert_eq!(error.kind(), ErrorKind::NotFound, "{label}");
		assert!(!empty.join(".wendelon").exists(), "{label}");
		// Generate, then adopt on a second generate-or-create open, then enroll.
		let generated = open(&config, &forming, RootKekBootstrapMode::GenerateOrCreate).unwrap();
		let check = check_of(&generated);
		let adopted = open(&config, &forming, RootKekBootstrapMode::GenerateOrCreate).unwrap();
		assert_eq!(check_of(&adopted), check, "{label}");
		// The documented copy gives another directory the same root.
		test_support::enroll_root_artifact(&forming, &member).unwrap();
		let enrolled = open(&config, &member, RootKekBootstrapMode::EnrollExisting).unwrap();
		assert_eq!(check_of(&enrolled), check, "{label}");
		// A separately generated root is another root.
		let stranger = root.to_path_buf().join(format!("{label}-stranger"));
		std::fs::create_dir_all(&stranger).unwrap();
		let strange = open(&config, &stranger, RootKekBootstrapMode::GenerateOrCreate).unwrap();
		assert_ne!(check_of(&strange), check, "{label}");
	}
	// local-sealed: the wrong unlock secret and a corrupt artifact are errors of
	// their own, in both modes, and never read as "absent, generate".
	let forming = root.to_path_buf().join("local-sealed-forming");
	let artifact = forming.join(".wendelon").join("local-sealed-root-kek.bin");
	let before = std::fs::read(&artifact).unwrap();
	for mode in [
		RootKekBootstrapMode::EnrollExisting,
		RootKekBootstrapMode::GenerateOrCreate,
	] {
		let error = open(&sealed(&other_secret), &forming, mode)
			.err()
			.expect("the wrong unlock secret does not open the root");
		assert_ne!(error.kind(), ErrorKind::NotFound);
		assert_eq!(std::fs::read(&artifact).unwrap(), before);
	}
	let mut corrupt = before.clone();
	let last = corrupt.len() - 1;
	corrupt[last] ^= 0x01;
	std::fs::write(&artifact, &corrupt).unwrap();
	for mode in [
		RootKekBootstrapMode::EnrollExisting,
		RootKekBootstrapMode::GenerateOrCreate,
	] {
		let error = open(&sealed(&secret), &forming, mode)
			.err()
			.expect("a corrupt artifact does not open");
		assert_ne!(error.kind(), ErrorKind::NotFound);
		assert_eq!(std::fs::read(&artifact).unwrap(), corrupt);
	}
}

/// The one classifier the setup censuses share: an empty root KEK directory,
/// or one holding only the providers' own artifact files, is neutral. Anything
/// else is not, and an unreadable directory is an error, never "fresh".
#[test]
fn only_an_exact_root_artifact_directory_is_neutral_to_the_setup_censuses() {
	use std::os::unix::fs::{PermissionsExt, symlink};
	let root = crate::test_support::TestDir::new("root-artifact-dir");
	let case = |name: &str| {
		let data_dir = root.to_path_buf().join(name);
		std::fs::create_dir_all(&data_dir).unwrap();
		data_dir
	};
	let artifacts = ROOT_KEK_ARTIFACT_FILES;
	assert_eq!(
		artifacts,
		[
			"dev-root-kek.bin",
			"local-sealed-root-kek.bin",
			"aws-kms-root-kek.bin"
		]
	);

	let empty = case("empty").join(".wendelon");
	std::fs::create_dir(&empty).unwrap();
	assert!(is_root_kek_artifact_dir(&empty).unwrap());

	for artifact in artifacts.iter().copied() {
		let dir = case(artifact).join(".wendelon");
		std::fs::create_dir(&dir).unwrap();
		std::fs::write(dir.join(artifact), b"sealed").unwrap();
		assert!(is_root_kek_artifact_dir(&dir).unwrap(), "{artifact}");
	}
	let all = case("all").join(".wendelon");
	std::fs::create_dir(&all).unwrap();
	for artifact in artifacts.iter().copied() {
		std::fs::write(all.join(artifact), b"sealed").unwrap();
	}
	assert!(is_root_kek_artifact_dir(&all).unwrap());

	// Another entry beside a real artifact: not neutral.
	for extra in [
		"dev-root-kek.bin.tmp",
		"notes",
		"storage_index.redb",
		".hidden",
	] {
		let dir = case(&format!("extra-{extra}")).join(".wendelon");
		std::fs::create_dir(&dir).unwrap();
		std::fs::write(dir.join(DEV_INSECURE_ROOT_KEK_FILE), b"sealed").unwrap();
		std::fs::write(dir.join(extra), b"x").unwrap();
		assert!(!is_root_kek_artifact_dir(&dir).unwrap(), "{extra}");
	}
	// A nested directory, even one named like an artifact.
	let nested = case("nested").join(".wendelon");
	std::fs::create_dir_all(nested.join(DEV_INSECURE_ROOT_KEK_FILE)).unwrap();
	assert!(!is_root_kek_artifact_dir(&nested).unwrap());
	// A symlinked artifact.
	let linked_file = case("linked-file").join(".wendelon");
	std::fs::create_dir(&linked_file).unwrap();
	let real = root.to_path_buf().join("real-artifact");
	std::fs::write(&real, b"sealed").unwrap();
	symlink(&real, linked_file.join(DEV_INSECURE_ROOT_KEK_FILE)).unwrap();
	assert!(!is_root_kek_artifact_dir(&linked_file).unwrap());
	// A symlinked directory, to a directory that would itself qualify.
	let linked_dir = case("linked-dir").join(".wendelon");
	symlink(&empty, &linked_dir).unwrap();
	assert!(!is_root_kek_artifact_dir(&linked_dir).unwrap());
	// The right contents under another name, and a file of the right name.
	let other_name = case("other-name").join("wendelon");
	std::fs::create_dir(&other_name).unwrap();
	assert!(!is_root_kek_artifact_dir(&other_name).unwrap());
	let a_file = case("a-file").join(".wendelon");
	std::fs::write(&a_file, b"x").unwrap();
	assert!(!is_root_kek_artifact_dir(&a_file).unwrap());
	// Missing, and unreadable: errors, not answers.
	assert!(is_root_kek_artifact_dir(&case("missing").join(".wendelon")).is_err());
	let locked = case("locked").join(".wendelon");
	std::fs::create_dir(&locked).unwrap();
	std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
	let unreadable = is_root_kek_artifact_dir(&locked);
	std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
	// Root reads any directory; only an unprivileged run can observe the error.
	if !nix_is_root() {
		assert!(unreadable.is_err());
	}
}

fn nix_is_root() -> bool {
	std::fs::read_to_string("/proc/self/status").is_ok_and(|status| {
		status
			.lines()
			.any(|line| line.starts_with("Uid:") && line.split_whitespace().nth(1) == Some("0"))
	})
}
