use super::*;
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedBundle2 {
	pub provenance: UnverifiedFrameProvenance,
	pub claims: Vec<(ClaimProjection2, Log2)>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedData2 {
	pub frame: Vec<u8>,
	pub bundle: UnverifiedBundle2,
}
opaque!(UnverifiedBundle2, UnverifiedData2);
impl UnverifiedBundle2 {
	pub(super) fn validate(&self) -> Result2<()> {
		check(
			!self.claims.is_empty()
				&& self.claims.len() <= 512
				&& self.claims.len() == self.provenance.parents().len(),
		)?;
		self.provenance.encode().map_err(|_| RootError2::Codec)?;
		for ((c, l), p) in self.claims.iter().zip(self.provenance.parents()) {
			check_claim(c)?;
			crate::storage::mutation_visibility::root2::codec::valid_log(*l)?;
			check(c.manifest == p.manifest && c.manifest_commitment == p.commitment)?;
		}
		for r in self.provenance.references() {
			let c = &self.claims[r.parent as usize].0;
			check(
				c.stamps
					.iter()
					.any(|s| s.child == r.child && s.row == r.row),
			)?;
		}
		Ok(())
	}
	pub fn encode(&self) -> Result2<Vec<u8>> {
		self.validate()?;
		let provenance = self.provenance.encode().map_err(|_| RootError2::Codec)?;
		let mut total = 54usize
			.checked_add(provenance.len())
			.ok_or(RootError2::Overflow)?;
		for (c, _) in &self.claims {
			total = total
				.checked_add(24 + c.encode()?.len())
				.ok_or(RootError2::Overflow)?;
			if total > BUNDLE_MAX {
				return Err(RootError2::Limit);
			}
		}
		let mut b = Vec::new();
		blob(&mut b, &provenance)?;
		b.extend((self.claims.len() as u16).to_le_bytes());
		for (c, l) in &self.claims {
			blob(&mut b, &c.encode()?)?;
			log(&mut b, *l)?;
		}
		wrap(b"VISBND02", &b, BUNDLE_MAX)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISBND02", BUNDLE_MAX)?;
		let provenance = UnverifiedFrameProvenance::decode(
			r.blob(crate::storage::mutation_visibility::provenance::MAX_PROVENANCE_BYTES)?,
		)
		.map_err(|_| RootError2::Codec)?;
		let n = count16(&mut r, 512, 24)?;
		let mut claims = Vec::new();
		for _ in 0..n {
			claims.push((ClaimProjection2::decode(r.blob(CLAIM_BYTES2)?)?, r.log()?));
		}
		r.end()?;
		let v = Self { provenance, claims };
		v.validate()?;
		Ok(v)
	}
}
impl UnverifiedData2 {
	pub fn encode(&self) -> Result2<Vec<u8>> {
		if self.frame.is_empty() || self.frame.len() > FRAME_MAX {
			return Err(RootError2::Limit);
		}
		self.bundle
			.provenance
			.check_companion(&self.frame)
			.map_err(|_| RootError2::Codec)?;
		let mut b = Vec::new();
		blob(&mut b, &self.frame)?;
		blob(&mut b, &self.bundle.encode()?)?;
		wrap(b"VISDAT02", &b, DATA_MAX)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISDAT02", DATA_MAX)?;
		let f = r.blob(FRAME_MAX)?;
		check(!f.is_empty())?;
		let bundle = UnverifiedBundle2::decode(r.blob(BUNDLE_MAX)?)?;
		r.end()?;
		bundle
			.provenance
			.check_companion(f)
			.map_err(|_| RootError2::Codec)?;
		Ok(Self {
			frame: f.to_vec(),
			bundle,
		})
	}
}
