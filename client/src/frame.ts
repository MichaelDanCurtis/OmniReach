// Wire format mirror of crates/protocol/src/frame.rs.
// Little-endian: [0] kind u8 | [1..5] handle u32 | [5..13] seq u64 | [13..] payload
export const HEADER_LEN = 13;

export const FrameKind = {
  PtyOutput: 0x01,
  PtyInput: 0x02,
  FsChunk: 0x03,
} as const;

export interface Frame {
  kind: number;
  handle: number;
  seq: bigint;
  payload: Uint8Array;
}

export function encodeFrame(
  kind: number,
  handle: number,
  seq: bigint,
  payload: Uint8Array,
): Uint8Array {
  const out = new Uint8Array(HEADER_LEN + payload.length);
  const view = new DataView(out.buffer);
  out[0] = kind;
  view.setUint32(1, handle, true);
  view.setBigUint64(5, seq, true);
  out.set(payload, HEADER_LEN);
  return out;
}

export function decodeFrame(buf: ArrayBuffer): Frame {
  if (buf.byteLength < HEADER_LEN) {
    throw new Error(`frame too short: ${buf.byteLength}`);
  }
  const view = new DataView(buf);
  return {
    kind: view.getUint8(0),
    handle: view.getUint32(1, true),
    seq: view.getBigUint64(5, true),
    // Subarray view over the same buffer — no copy on the hot path.
    payload: new Uint8Array(buf, HEADER_LEN),
  };
}
