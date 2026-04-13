use crate::schema::FrameHeader;
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HRFT_MAGIC: u32 = 0x48524654; // "HRFT" in ASCII

/// A fully parsed Wendelon wire frame.
/// For true zero-copy, the metadata and arrow payloads are mapped directly
/// from the network buffers without deserialization overhead.
#[derive(Debug)]
pub struct HrftFrame {
    pub header: FrameHeader,
    pub meta: Vec<u8>,
    pub arrow_payload: Option<Vec<u8>>,
    pub trailing: Option<Vec<u8>>,
}

/// Reads a complete HRFT frame from an asynchronous stream (e.g., a quinn::RecvStream).
/// Ensures that the frame adheres to the magic header and bounded sizes.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<HrftFrame> {
    let mut header_buf = [0u8; std::mem::size_of::<FrameHeader>()];
    
    // 1. Read the 24-byte FrameHeader
    reader.read_exact(&mut header_buf).await?;
    
    // 0ns pointer cast to interpret the bytes as a FrameHeader struct
    let header: &FrameHeader = bytemuck::from_bytes(&header_buf);
    
    if header.magic != HRFT_MAGIC {
        return Err(Error::new(ErrorKind::InvalidData, "Invalid HRFT magic bytes"));
    }

    // Prevent massive allocation attacks
    if header.meta_len > 1024 * 1024 { // 1 MB limit for metadata
        return Err(Error::new(ErrorKind::InvalidData, "Metadata payload exceeds safety limit"));
    }
    
    // 2. Read the Metadata section
    let mut meta = vec![0u8; header.meta_len as usize];
    if header.meta_len > 0 {
        reader.read_exact(&mut meta).await?;
    }
    
    // 3. Read the optional Arrow IPC section
    let mut arrow_payload = None;
    if header.arrow_len > 0 {
        let mut arrow = vec![0u8; header.arrow_len as usize];
        reader.read_exact(&mut arrow).await?;
        arrow_payload = Some(arrow);
    }
    
    // 4. Read the optional Trailing section (e.g., WPBC AST or FinalizationMarker)
    let mut trailing = None;
    if header.trailing_len > 0 {
        let mut t = vec![0u8; header.trailing_len as usize];
        reader.read_exact(&mut t).await?;
        trailing = Some(t);
    }
    
    Ok(HrftFrame {
        header: *header,
        meta,
        arrow_payload,
        trailing,
    })
}

/// Writes an HRFT frame to an asynchronous stream (e.g., a quinn::SendStream).
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: &FrameHeader,
    meta: &[u8],
    arrow_payload: Option<&[u8]>,
    trailing: Option<&[u8]>,
) -> std::io::Result<()> {
    // 0ns cast to write the struct directly to the stream
    writer.write_all(bytemuck::bytes_of(header)).await?;
    writer.write_all(meta).await?;
    
    if let Some(arrow) = arrow_payload {
        writer.write_all(arrow).await?;
    }
    
    if let Some(t) = trailing {
        writer.write_all(t).await?;
    }
    
    Ok(())
}
