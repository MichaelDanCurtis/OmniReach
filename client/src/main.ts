import "@xterm/xterm/css/xterm.css";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import { Connection, type Status } from "./connection";

const HOST_URL =
  (import.meta.env.VITE_HOST_URL as string | undefined) ??
  "ws://127.0.0.1:7717/ws";

/** First-ever launch spawns this many terminals — the product's founding ask. */
const FIRST_LAUNCH_PANES = 4;

const WORKSPACE_KEY = "omnireach.workspace.v2";
/** Pre-multi-terminal key; migrated then removed. */
const LEGACY_KEY = "omnireach.session";

interface Workspace {
  sessions: string[];
  active: number;
}

interface Pane {
  sessionId: string;
  /** Per-connection handle; null while unattached. Reassigned on every reconnect. */
  handle: number | null;
  /**
   * High-water byte offset this pane has consumed. Only meaningful while the
   * xterm buffer that consumed those bytes is alive — a fresh launch must
   * replay from 0 instead, or the pane comes back blank (see attachPane).
   */
  lastSeq: bigint;
  /**
   * Each pane owns its decoder: TextDecoder with stream:true is stateful, and
   * a shared one would weld together multi-byte sequences from different
   * sessions when their frames interleave.
   */
  decoder: TextDecoder;
  term: Terminal;
  fit: FitAddon;
  el: HTMLDivElement;
  alive: boolean;
}

const statusEl = document.getElementById("status") as HTMLSpanElement;
const tabsEl = document.getElementById("tabs") as HTMLElement;
const terminalsEl = document.getElementById("terminals") as HTMLDivElement;
const newBtn = document.getElementById("new-pane") as HTMLButtonElement;
const closeBtn = document.getElementById("close-pane") as HTMLButtonElement;

const panes: Pane[] = [];
let activeIdx = 0;
const byHandle = new Map<number, Pane>();

const conn = new Connection(HOST_URL);

// ---------------------------------------------------------------------------
// Persistence — session ids only. lastSeq is deliberately NOT persisted: it
// describes a buffer that dies with the page, and resuming a fresh buffer at
// an old offset is exactly the blank-terminal-on-restart bug.
// ---------------------------------------------------------------------------

function loadWorkspace(): Workspace | null {
  const raw = localStorage.getItem(WORKSPACE_KEY);
  if (raw) return JSON.parse(raw) as Workspace;

  const legacy = localStorage.getItem(LEGACY_KEY);
  if (legacy) {
    localStorage.removeItem(LEGACY_KEY);
    try {
      const old = JSON.parse(legacy) as { sessionId: string };
      return { sessions: [old.sessionId], active: 0 };
    } catch {
      return null;
    }
  }
  return null;
}

function persist(): void {
  const ws: Workspace = {
    sessions: panes.map((p) => p.sessionId),
    active: activeIdx,
  };
  localStorage.setItem(WORKSPACE_KEY, JSON.stringify(ws));
}

// ---------------------------------------------------------------------------
// Pane lifecycle
// ---------------------------------------------------------------------------

function makePane(sessionId: string): Pane {
  const el = document.createElement("div");
  el.className = "pane";
  terminalsEl.appendChild(el);

  // Canvas renderer (xterm's default), not WebGL: Tauri renders in WKWebView,
  // where WebGL is the less-travelled path for high-throughput terminals.
  const term = new Terminal({
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
    fontSize: 13,
    theme: { background: "#10100f", foreground: "#e8e6e3" },
    scrollback: 20_000,
  });
  const fit = new FitAddon();
  term.loadAddon(fit);
  term.open(el);

  const pane: Pane = {
    sessionId,
    handle: null,
    lastSeq: 0n,
    decoder: new TextDecoder("utf-8"),
    term,
    fit,
    el,
    alive: true,
  };

  term.onData((d) => {
    if (pane.handle != null) conn.sendInput(pane.handle, d);
  });
  term.onResize(({ cols, rows }) => {
    if (pane.handle != null) {
      conn.send("pty.resize", { handle: pane.handle, cols, rows }).catch(() => {});
    }
  });

  panes.push(pane);
  return pane;
}

function destroyPane(pane: Pane): void {
  if (pane.handle != null) byHandle.delete(pane.handle);
  pane.term.dispose();
  pane.el.remove();
  const i = panes.indexOf(pane);
  if (i >= 0) panes.splice(i, 1);
}

async function createPane(): Promise<Pane | null> {
  try {
    const r = await conn.send("pty.create", {
      shell: null,
      cwd: null,
      cols: 80,
      rows: 24,
    });
    const pane = makePane(r.session_id);
    pane.handle = r.handle;
    byHandle.set(r.handle, pane);
    return pane;
  } catch (e) {
    // Surfacing beats vanishing: put the failure in the active pane if any.
    panes[activeIdx]?.term.write(`\r\n\x1b[31mcreate failed: ${e}\x1b[0m\r\n`);
    return null;
  }
}

/**
 * Attach (or re-attach) a pane to its host session.
 *
 * `fresh` distinguishes the two resume cases, and they need different offsets:
 * - fresh buffer (app launch): replay from 0 so the ring repaints the
 *   terminal — attaching at the old offset would show a blank pane until new
 *   output arrived;
 * - live buffer (network blip): resume at lastSeq so nothing is duplicated.
 */
async function attachPane(pane: Pane, fresh: boolean): Promise<boolean> {
  const since = fresh ? 0n : pane.lastSeq;
  try {
    const r = await conn.send("pty.attach", {
      session_id: pane.sessionId,
      since: Number(since),
    });
    if (fresh) {
      pane.term.reset();
      pane.decoder = new TextDecoder("utf-8");
      pane.lastSeq = 0n;
    }
    pane.handle = r.handle;
    byHandle.set(r.handle, pane);
    if (r.truncated_from != null) {
      // An honest marker beats a silent gap in an agent transcript. On a
      // long-lived session this is expected: the ring is finite.
      pane.term.write("\x1b[2m— older output lost —\x1b[0m\r\n");
      pane.lastSeq = BigInt(r.truncated_from);
    }
    return true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Tab bar
// ---------------------------------------------------------------------------

function renderTabs(): void {
  tabsEl.textContent = "";
  panes.forEach((pane, i) => {
    const b = document.createElement("button");
    b.className =
      "tab" + (i === activeIdx ? " active" : "") + (pane.alive ? "" : " dead");
    b.textContent = `${i + 1}`;
    b.title = pane.sessionId;
    b.addEventListener("click", () => setActive(i));
    tabsEl.appendChild(b);
  });
}

function setActive(i: number): void {
  if (i < 0 || i >= panes.length) return;
  activeIdx = i;
  panes.forEach((p, j) => p.el.classList.toggle("active", j === i));
  renderTabs();
  persist();
  const pane = panes[i];
  pane.fit.fit();
  pane.term.focus();
}

// ---------------------------------------------------------------------------
// Connection events
// ---------------------------------------------------------------------------

function setStatus(s: Status) {
  statusEl.textContent = s;
  statusEl.className = `status ${s}`;
}

conn.onStatus(setStatus);

conn.onOutput((h, seq, data) => {
  const pane = byHandle.get(h);
  if (!pane) return; // stale handle from before a reconnect — replay covers it
  pane.term.write(pane.decoder.decode(data, { stream: true }));
  pane.lastSeq = seq + BigInt(data.length);
});

conn.onResync((sessionId, fromSeq) => {
  const pane = panes.find((p) => p.sessionId === sessionId);
  if (!pane) return;
  // Deliberately NOT clearing: scrollback above this point is still valid.
  pane.term.write("\r\n\x1b[2m— resynced, some output skipped —\x1b[0m\r\n");
  pane.lastSeq = fromSeq;
});

conn.onExit((sessionId, status) => {
  const pane = panes.find((p) => p.sessionId === sessionId);
  if (!pane) return;
  pane.alive = false;
  pane.term.write(
    `\r\n\x1b[2m— session exited (status ${status}) —\x1b[0m\r\n`,
  );
  renderTabs(); // pane stays until closed, so its final output stays readable
});

let firstOpen = true;
conn.onOpen(async () => {
  const fresh = firstOpen;
  firstOpen = false;

  if (fresh) {
    const saved = loadWorkspace();
    if (saved) {
      for (const id of saved.sessions) makePane(id);
      const results = await Promise.all(
        panes.map((p) => attachPane(p, true)),
      );
      // Prune sessions the host no longer has (restart, retention sweep).
      const gone = panes.filter((_, i) => !results[i]);
      gone.forEach(destroyPane);
      activeIdx = Math.min(saved.active, Math.max(0, panes.length - 1));
    }
    if (panes.length === 0) {
      const n = saved ? 1 : FIRST_LAUNCH_PANES;
      for (let i = 0; i < n; i++) await createPane();
    }
  } else {
    // Network blip: buffers are intact, resume each pane where it left off.
    byHandle.clear();
    for (const pane of panes) {
      pane.handle = null;
      if (pane.alive) await attachPane(pane, false);
    }
  }

  persist();
  renderTabs();
  setActive(Math.min(activeIdx, panes.length - 1));
});

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

newBtn.addEventListener("click", async () => {
  const pane = await createPane();
  if (pane) {
    persist();
    setActive(panes.length - 1);
  }
});

closeBtn.addEventListener("click", async () => {
  const pane = panes[activeIdx];
  if (!pane) return;
  if (pane.alive) {
    conn.send("pty.kill", { session_id: pane.sessionId }).catch(() => {});
  }
  destroyPane(pane);
  if (panes.length === 0) await createPane();
  persist();
  setActive(Math.min(activeIdx, panes.length - 1));
});

window.addEventListener("resize", () => panes[activeIdx]?.fit.fit());

setStatus("disconnected");
conn.connect();

// Dev-only hook so headless verification can drive the app's REAL send path
// (conn.sendInput) and read pane buffers. Not defined in production builds.
if (import.meta.env.DEV) {
  (window as unknown as Record<string, unknown>).__omnireach = {
    panes,
    count: () => panes.length,
    activeIdx: () => activeIdx,
    send: (i: number, data: string) => {
      const p = panes[i];
      if (p?.handle != null) conn.sendInput(p.handle, data);
    },
    text: (i: number) => {
      const buf = panes[i]?.term.buffer.active;
      if (!buf) return "";
      const lines: string[] = [];
      for (let r = 0; r < buf.length; r++) {
        lines.push(buf.getLine(r)?.translateToString(true) ?? "");
      }
      return lines.join("\n");
    },
  };
}
