/// A bounded byte ring that tracks a monotonic sequence number.
///
/// `head_seq` counts every byte ever written, whether or not it is still
/// resident. Bytes older than `oldest_seq()` have been overwritten.
///
/// This type is deliberately byte-transparent: it never interprets, decodes,
/// or validates its contents. PTY output is a stream of VT escape sequences
/// and arbitrary UTF-8, and decoding it here would corrupt multi-byte
/// characters that straddle a write boundary.
pub struct Ring {
    buf: Box<[u8]>,
    head_seq: u64,
}

impl Ring {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0u8; capacity].into_boxed_slice(),
            head_seq: 0,
        }
    }

    pub fn head_seq(&self) -> u64 {
        self.head_seq
    }

    pub fn oldest_seq(&self) -> u64 {
        let cap = self.buf.len() as u64;
        self.head_seq.saturating_sub(cap.min(self.head_seq))
    }

    pub fn write(&mut self, data: &[u8]) {
        let cap = self.buf.len();
        let total = data.len() as u64;
        if cap == 0 {
            self.head_seq += total;
            return;
        }

        // A single write larger than capacity: only its tail can survive.
        let tail = if data.len() > cap {
            &data[data.len() - cap..]
        } else {
            data
        };
        let skipped = (data.len() - tail.len()) as u64;

        let pos = ((self.head_seq + skipped) % cap as u64) as usize;
        let first = tail.len().min(cap - pos);
        self.buf[pos..pos + first].copy_from_slice(&tail[..first]);
        if first < tail.len() {
            let rest = tail.len() - first;
            self.buf[..rest].copy_from_slice(&tail[first..]);
        }

        self.head_seq += total;
    }

    /// Returns `(actual_start_seq, bytes)`. If `seq` is older than
    /// `oldest_seq()` the start is clamped forward and the caller must tell
    /// the client its stream was truncated.
    pub fn read_since(&self, seq: u64) -> (u64, Vec<u8>) {
        let cap = self.buf.len();
        let start = seq.max(self.oldest_seq()).min(self.head_seq);
        let n = (self.head_seq - start) as usize;
        if n == 0 || cap == 0 {
            return (start, Vec::new());
        }

        let pos = (start % cap as u64) as usize;
        let mut out = Vec::with_capacity(n);
        let first = n.min(cap - pos);
        out.extend_from_slice(&self.buf[pos..pos + first]);
        if first < n {
            out.extend_from_slice(&self.buf[..n - first]);
        }
        (start, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ring_reports_zero_seqs() {
        let r = Ring::new(16);
        assert_eq!(r.head_seq(), 0);
        assert_eq!(r.oldest_seq(), 0);
        assert_eq!(r.read_since(0), (0, vec![]));
    }

    #[test]
    fn reads_back_everything_written_when_under_capacity() {
        let mut r = Ring::new(16);
        r.write(b"hello");
        assert_eq!(r.head_seq(), 5);
        assert_eq!(r.oldest_seq(), 0);
        assert_eq!(r.read_since(0), (0, b"hello".to_vec()));
    }

    #[test]
    fn reads_suffix_from_midpoint() {
        let mut r = Ring::new(16);
        r.write(b"hello");
        assert_eq!(r.read_since(2), (2, b"llo".to_vec()));
    }

    #[test]
    fn overwrites_oldest_when_full_and_reports_truncation() {
        let mut r = Ring::new(4);
        r.write(b"abcdef");
        assert_eq!(r.head_seq(), 6);
        assert_eq!(r.oldest_seq(), 2);
        // asking for seq 0 gets clamped up to oldest_seq
        assert_eq!(r.read_since(0), (2, b"cdef".to_vec()));
    }

    #[test]
    fn single_write_larger_than_capacity_keeps_only_the_tail() {
        let mut r = Ring::new(4);
        r.write(b"abcdefghij");
        assert_eq!(r.head_seq(), 10);
        assert_eq!(r.oldest_seq(), 6);
        assert_eq!(r.read_since(0), (6, b"ghij".to_vec()));
    }

    #[test]
    fn wraps_correctly_across_many_writes() {
        let mut r = Ring::new(8);
        for _ in 0..10 {
            r.write(b"xyz");
        }
        assert_eq!(r.head_seq(), 30);
        assert_eq!(r.oldest_seq(), 22);
        let (start, bytes) = r.read_since(22);
        assert_eq!(start, 22);
        assert_eq!(bytes.len(), 8);
        // "xyzxyzxyz..." — byte at absolute index n is "xyz"[n % 3]
        let expected: Vec<u8> = (22u64..30).map(|n| b"xyz"[(n % 3) as usize]).collect();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn read_since_future_seq_returns_empty() {
        let mut r = Ring::new(16);
        r.write(b"hello");
        assert_eq!(r.read_since(99), (5, vec![]));
    }

    #[test]
    fn property_read_since_always_yields_a_suffix() {
        let mut r = Ring::new(64);
        let mut all: Vec<u8> = Vec::new();
        let mut n: u8 = 0;
        for len in [1usize, 7, 3, 31, 64, 2, 100, 5] {
            let chunk: Vec<u8> = (0..len)
                .map(|_| {
                    n = n.wrapping_add(1);
                    n
                })
                .collect();
            r.write(&chunk);
            all.extend_from_slice(&chunk);

            let (start, got) = r.read_since(r.oldest_seq());
            let expected = &all[start as usize..];
            assert_eq!(got, expected, "failed after writing {len} bytes");
        }
    }

    #[test]
    fn multibyte_utf8_split_across_writes_survives_replay() {
        // "日本語" is 9 bytes; write it split mid-character.
        let s = "日本語".as_bytes();
        let mut r = Ring::new(64);
        r.write(&s[..4]); // splits the 2nd codepoint
        r.write(&s[4..]);
        let (_, got) = r.read_since(0);
        assert_eq!(got, s, "ring must be byte-transparent, never decoding");
    }
}
