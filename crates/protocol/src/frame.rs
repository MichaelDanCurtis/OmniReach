use thiserror::Error;

pub const HEADER_LEN: usize = 13;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    PtyOutput = 0x01,
    PtyInput = 0x02,
    FsChunk = 0x03,
}

impl FrameKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::PtyOutput),
            0x02 => Some(Self::PtyInput),
            0x03 => Some(Self::FsChunk),
            _ => None,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("frame too short: got {got} bytes, need at least {HEADER_LEN}")]
    TooShort { got: usize },
    #[error("unknown frame kind: {0:#04x}")]
    UnknownKind(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryFrame {
    pub kind: FrameKind,
    pub handle: u32,
    pub seq: u64,
    pub payload: Vec<u8>,
}

impl BinaryFrame {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.push(self.kind as u8);
        out.extend_from_slice(&self.handle.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < HEADER_LEN {
            return Err(FrameError::TooShort { got: bytes.len() });
        }
        let kind = FrameKind::from_u8(bytes[0]).ok_or(FrameError::UnknownKind(bytes[0]))?;
        let handle = u32::from_le_bytes(bytes[1..5].try_into().expect("4 bytes"));
        let seq = u64::from_le_bytes(bytes[5..13].try_into().expect("8 bytes"));
        Ok(Self {
            kind,
            handle,
            seq,
            payload: bytes[HEADER_LEN..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_frame() {
        let f = BinaryFrame {
            kind: FrameKind::PtyOutput,
            handle: 7,
            seq: 123_456_789,
            payload: b"hello world".to_vec(),
        };
        let bytes = f.encode();
        assert_eq!(bytes.len(), HEADER_LEN + 11);
        let back = BinaryFrame::decode(&bytes).expect("decode");
        assert_eq!(back.kind, FrameKind::PtyOutput);
        assert_eq!(back.handle, 7);
        assert_eq!(back.seq, 123_456_789);
        assert_eq!(back.payload, b"hello world");
    }

    #[test]
    fn round_trips_boundary_values() {
        let f = BinaryFrame {
            kind: FrameKind::PtyInput,
            handle: u32::MAX,
            seq: u64::MAX,
            payload: vec![],
        };
        let back = BinaryFrame::decode(&f.encode()).expect("decode");
        assert_eq!(back.handle, u32::MAX);
        assert_eq!(back.seq, u64::MAX);
        assert!(back.payload.is_empty());
    }

    #[test]
    fn round_trips_arbitrary_bytes_including_invalid_utf8() {
        let payload = vec![0xff, 0xfe, 0x00, 0x80, 0xc3];
        let f = BinaryFrame {
            kind: FrameKind::PtyOutput,
            handle: 1,
            seq: 0,
            payload: payload.clone(),
        };
        assert_eq!(BinaryFrame::decode(&f.encode()).unwrap().payload, payload);
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(matches!(
            BinaryFrame::decode(&[0x01, 0x00]),
            Err(FrameError::TooShort { .. })
        ));
    }

    #[test]
    fn rejects_unknown_kind() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0] = 0x99;
        assert!(matches!(
            BinaryFrame::decode(&bytes),
            Err(FrameError::UnknownKind(0x99))
        ));
    }
}
