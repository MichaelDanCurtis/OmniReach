import "@xterm/xterm/css/xterm.css";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import { Connection, type Status } from "./connection";

const HOST_URL =
  (import.meta.env.VITE_HOST_URL as string | undefined) ??
  "ws://127.0.0.1:7717/ws";
const RESUME_KEY = "omnireach.session";

interface Resume {
  sessionId: string;
  lastSeq: string; // BigInt is not JSON-serializable
}

const statusEl = document.getElementById("status") as HTMLDivElement;
const termEl = document.getElementById("terminal") as HTMLDivElement;

// Canvas renderer (xterm's default), NOT the WebGL addon: Tauri renders in
// WKWebView, where the WebGL path is the less-travelled one for terminals
// pushing high throughput.
const term = new Terminal({
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
  fontSize: 13,
  theme: { background: "#10100f", foreground: "#e8e6e3" },
  scrollback: 20_000,
});
const fit = new FitAddon();
term.loadAddon(fit);
term.open(termEl);
fit.fit();

// stream: true so multi-byte sequences split across frames decode correctly.
const decoder = new TextDecoder("utf-8");
let handle: number | null = null;
let lastSeq = 0n;

function setStatus(s: Status) {
  statusEl.textContent = s;
  statusEl.className = `status ${s}`;
}

function loadResume(): Resume | null {
  const raw = localStorage.getItem(RESUME_KEY);
  return raw ? (JSON.parse(raw) as Resume) : null;
}

function saveResume(sessionId: string) {
  localStorage.setItem(
    RESUME_KEY,
    JSON.stringify({
      sessionId,
      lastSeq: lastSeq.toString(),
    } satisfies Resume),
  );
}

const conn = new Connection(HOST_URL);

conn.onStatus(setStatus);

conn.onOutput((h, seq, data) => {
  if (h !== handle) return;
  term.write(decoder.decode(data, { stream: true }));
  lastSeq = seq + BigInt(data.length);
});

conn.onResync((_sessionId, fromSeq) => {
  // Deliberately NOT clearing: client-side scrollback above this point is
  // still valid, and throwing it away would lose more than the gap did.
  term.write("\r\n\x1b[2m— resynced, some output skipped —\x1b[0m\r\n");
  lastSeq = fromSeq;
});

conn.onExit((_sessionId, status) => {
  term.write(`\r\n\x1b[2m— session exited (status ${status}) —\x1b[0m\r\n`);
  localStorage.removeItem(RESUME_KEY);
  handle = null;
});

conn.onOpen(async () => {
  const resume = loadResume();
  if (resume) {
    try {
      const r = await conn.send("pty.attach", {
        session_id: resume.sessionId,
        since: Number(BigInt(resume.lastSeq)),
      });
      handle = r.handle;
      lastSeq = BigInt(resume.lastSeq);
      if (r.truncated_from != null) {
        // An honest marker beats a silent gap in an agent transcript.
        term.write("\r\n\x1b[2m— output lost —\x1b[0m\r\n");
        lastSeq = BigInt(r.truncated_from);
      }
      return;
    } catch {
      localStorage.removeItem(RESUME_KEY);
      term.write(
        "\r\n\x1b[2m— previous session unavailable, starting a new one —\x1b[0m\r\n",
      );
    }
  }

  const r = await conn.send("pty.create", {
    shell: null,
    cwd: null,
    cols: term.cols,
    rows: term.rows,
  });
  handle = r.handle;
  lastSeq = 0n;
  saveResume(r.session_id);
});

term.onData((d) => {
  if (handle != null) conn.sendInput(handle, d);
});

term.onResize(({ cols, rows }) => {
  if (handle != null) {
    conn.send("pty.resize", { handle, cols, rows }).catch(() => {});
  }
});

window.addEventListener("resize", () => fit.fit());

// Persist the resume offset periodically so an abrupt app kill still resumes.
setInterval(() => {
  const r = loadResume();
  if (r) saveResume(r.sessionId);
}, 2000);

setStatus("disconnected");
conn.connect();
