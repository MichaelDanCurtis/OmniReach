import { decodeFrame, encodeFrame, FrameKind } from "./frame";

export type Status =
  | "connected"
  | "reconnecting"
  | "disconnected"
  | "auth-expired";

interface Pending {
  resolve: (v: unknown) => void;
  reject: (e: Error) => void;
}

// Reconnect policy, per the design spec: 250ms base, x1.8, cap 15s, +/-20% jitter.
const BASE_DELAY_MS = 250;
const MAX_DELAY_MS = 15_000;
const FACTOR = 1.8;
const JITTER = 0.2;

type OutputCb = (handle: number, seq: bigint, data: Uint8Array) => void;
type StatusCb = (s: Status) => void;
type ResyncCb = (sessionId: string, fromSeq: bigint) => void;
type ExitCb = (sessionId: string, status: number) => void;
type ChunkCb = (handle: number, seq: bigint, data: Uint8Array) => void;
type EventCb = (msg: Record<string, any>) => void;

export class Connection {
  private ws: WebSocket | null = null;
  private nextId = 1;
  private pending = new Map<number, Pending>();
  private attempt = 0;
  private closedByUser = false;

  private outputCb: OutputCb = () => {};
  private statusCb: StatusCb = () => {};
  private resyncCb: ResyncCb = () => {};
  private exitCb: ExitCb = () => {};
  private openCb: () => void = () => {};
  private fsChunkCb: ChunkCb = () => {};
  private browserFrameCb: ChunkCb = () => {};
  private eventCb: EventCb = () => {};

  constructor(private url: string) {}

  onOutput(cb: OutputCb) {
    this.outputCb = cb;
  }
  onStatus(cb: StatusCb) {
    this.statusCb = cb;
  }
  onResync(cb: ResyncCb) {
    this.resyncCb = cb;
  }
  onExit(cb: ExitCb) {
    this.exitCb = cb;
  }
  onOpen(cb: () => void) {
    this.openCb = cb;
  }
  onFsChunk(cb: ChunkCb) {
    this.fsChunkCb = cb;
  }
  onBrowserFrame(cb: ChunkCb) {
    this.browserFrameCb = cb;
  }
  /** Any unsolicited control event not handled internally (browser.console, …). */
  onEvent(cb: EventCb) {
    this.eventCb = cb;
  }

  connect(): void {
    this.closedByUser = false;
    this.open();
  }

  close(): void {
    this.closedByUser = true;
    this.ws?.close();
    this.statusCb("disconnected");
  }

  private open(): void {
    this.statusCb("reconnecting");
    const ws = new WebSocket(this.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    ws.onopen = () => {
      this.attempt = 0;
      this.statusCb("connected");
      this.openCb();
    };

    ws.onmessage = (ev) => {
      if (typeof ev.data === "string") {
        this.handleControl(JSON.parse(ev.data));
        return;
      }
      const f = decodeFrame(ev.data as ArrayBuffer);
      if (f.kind === FrameKind.PtyOutput) {
        this.outputCb(f.handle, f.seq, f.payload);
      } else if (f.kind === FrameKind.FsChunk) {
        // File bytes are collected by the control-request id that asked for
        // them. The client picks that id, so a chunk can never arrive before
        // its collector is registered — no race, no buffering-by-unknown-key.
        this.fsChunkCb(f.handle, f.seq, f.payload);
      } else if (f.kind === FrameKind.BrowserFrame) {
        this.browserFrameCb(f.handle, f.seq, f.payload);
      }
    };

    ws.onclose = () => {
      // Reject everything in flight. A pending promise that never settles is
      // a silently hung UI, which is the failure mode this project cares most
      // about avoiding.
      for (const [, p] of this.pending) {
        p.reject(new Error("connection closed"));
      }
      this.pending.clear();
      if (this.closedByUser) {
        this.statusCb("disconnected");
      } else {
        this.scheduleReconnect();
      }
    };

    // onclose always follows onerror, so recovery is handled in one place.
    ws.onerror = () => {};
  }

  private scheduleReconnect(): void {
    this.statusCb("reconnecting");
    const raw = Math.min(
      BASE_DELAY_MS * Math.pow(FACTOR, this.attempt),
      MAX_DELAY_MS,
    );
    const jitter = raw * JITTER * (Math.random() * 2 - 1);
    this.attempt += 1;
    setTimeout(() => this.open(), Math.max(0, raw + jitter));
  }

  private handleControl(msg: Record<string, any>): void {
    if (typeof msg.id === "number") {
      const p = this.pending.get(msg.id);
      if (!p) return;
      this.pending.delete(msg.id);
      if (msg.ok) p.resolve(msg.result);
      else p.reject(new Error(`${msg.error?.code}: ${msg.error?.message}`));
      return;
    }
    if (msg.event === "pty.resync") {
      this.resyncCb(msg.session_id, BigInt(msg.from_seq));
    } else if (msg.event === "pty.exit") {
      this.exitCb(msg.session_id, msg.status);
    } else {
      this.eventCb(msg);
    }
  }

  /** Next control-request id. Callers that must pre-register a binary
   *  collector need the id before sending — see `sendWithId`. */
  peekId(): number {
    return this.nextId;
  }

  /** Like `send`, but the id is allocated by `peekId` beforehand so the caller
   *  can register a binary-frame collector keyed on it first. */
  sendWithId(id: number, op: string, params: unknown): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.ws?.readyState !== WebSocket.OPEN) {
        reject(new Error("not connected"));
        return;
      }
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, op, params }));
    });
  }

  /** Reserve the next id, bumping the counter. */
  takeId(): number {
    return this.nextId++;
  }

  sendBinary(kind: number, handle: number, seq: bigint, data: Uint8Array): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    this.ws.send(encodeFrame(kind, handle, seq, data));
  }

  send(op: string, params: unknown): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.ws?.readyState !== WebSocket.OPEN) {
        reject(new Error("not connected"));
        return;
      }
      const id = this.nextId++;
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, op, params }));
    });
  }

  sendInput(handle: number, data: string): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    const payload = new TextEncoder().encode(data);
    this.ws.send(encodeFrame(FrameKind.PtyInput, handle, 0n, payload));
  }
}
