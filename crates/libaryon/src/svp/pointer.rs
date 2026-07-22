/// ==========================================
/// LAYER 1: PHYSICAL COMPONENTS (SVP)
/// ==========================================
/// Sparse Virtualized Projection Pointer.
/// Bypasses the OS kernel. No memmap2, no owned Strings.
/// References physical storage blocks directly via SPDK/NVMe-Direct.
#[repr(C, align(16))]
#[derive(Debug, Clone, Copy)]
pub struct SvpPointer {
	pub lba: u64,
	pub byte_length: u32,
	pub device_id: u16,
	pub head_trim: u16,
}
