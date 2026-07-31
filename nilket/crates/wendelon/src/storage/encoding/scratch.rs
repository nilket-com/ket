#[derive(Default)]
pub struct EncodingScratch {
	buffer: Vec<u8>,
}

impl EncodingScratch {
	pub fn take(&mut self) -> Vec<u8> {
		std::mem::take(&mut self.buffer)
	}

	pub fn put(&mut self, mut buffer: Vec<u8>) {
		buffer.clear();
		self.buffer = buffer;
	}
}
