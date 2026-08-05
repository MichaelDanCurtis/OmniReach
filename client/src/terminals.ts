import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import type { Connection } from "./connection";

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
  /** Per-connection handle; null while unattached. Reassigned every reconnect. */
  handle: number | null;
  /**
   * High-water byte offset this pane has consumed. Only meaningful while the
   * xterm buffer that consumed those bytes is alive — a fresh launch replays
   * from 0 instead, or the pane comes back blank (see `attach`).
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

export class TerminalsView {
  private panes: Pane[] = [];
  private activeIdx = 0;
  private byHandle = new Map<number, Pane>();
  private firstOpen = true;

  constructor(
    private conn: Connection,
    private host: HTMLDivElement,
    private tabsEl: HTMLElement,
  ) {
    conn.onOutput((h, seq, data) => {
      const pane = this.byHandle.get(h);
      if (!pane) return; // stale handle from before a reconnect — replay covers it
      pane.term.write(pane.decoder.decode(data, { stream: true }));
      pane.lastSeq = seq + BigInt(data.length);
    });

    conn.onResync((sessionId, fromSeq) => {
      const pane = this.panes.find((p) => p.sessionId === sessionId);
      if (!pane) return;
      // Deliberately NOT clearing: scrollback above this point is still valid.
      pane.term.write("\r\n\x1b[2m— resynced, some output skipped —\x1b[0m\r\n");
      pane.lastSeq = fromSeq;
    });

    conn.onExit((sessionId, status) => {
      const pane = this.panes.find((p) => p.sessionId === sessionId);
      if (!pane) return;
      pane.alive = false;
      pane.term.write(
        `\r\n\x1b[2m— session exited (status ${status}) —\x1b[0m\r\n`,
      );
      this.renderTabs(); // pane stays until closed, so final output stays readable
    });
  }

  get count(): number {
    return this.panes.length;
  }
  get active(): number {
    return this.activeIdx;
  }

  // -------------------------------------------------------------------------
  // Persistence — session ids only. lastSeq is deliberately NOT persisted: it
  // describes a buffer that dies with the page, and resuming a fresh buffer at
  // an old offset is exactly the blank-terminal-on-restart bug.
  // -------------------------------------------------------------------------

  private loadWorkspace(): Workspace | null {
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

  private persist(): void {
    localStorage.setItem(
      WORKSPACE_KEY,
      JSON.stringify({
        sessions: this.panes.map((p) => p.sessionId),
        active: this.activeIdx,
      } satisfies Workspace),
    );
  }

  // -------------------------------------------------------------------------
  // Pane lifecycle
  // -------------------------------------------------------------------------

  private makePane(sessionId: string): Pane {
    const el = document.createElement("div");
    el.className = "pane";
    this.host.appendChild(el);

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
      if (pane.handle != null) this.conn.sendInput(pane.handle, d);
    });
    term.onResize(({ cols, rows }) => {
      if (pane.handle != null) {
        this.conn
          .send("pty.resize", { handle: pane.handle, cols, rows })
          .catch(() => {});
      }
    });

    this.panes.push(pane);
    return pane;
  }

  private destroyPane(pane: Pane): void {
    if (pane.handle != null) this.byHandle.delete(pane.handle);
    pane.term.dispose();
    pane.el.remove();
    const i = this.panes.indexOf(pane);
    if (i >= 0) this.panes.splice(i, 1);
  }

  async createPane(): Promise<Pane | null> {
    try {
      const r = await this.conn.send("pty.create", {
        shell: null,
        cwd: null,
        cols: 80,
        rows: 24,
      });
      const pane = this.makePane(r.session_id);
      pane.handle = r.handle;
      this.byHandle.set(r.handle, pane);
      return pane;
    } catch (e) {
      this.panes[this.activeIdx]?.term.write(
        `\r\n\x1b[31mcreate failed: ${e}\x1b[0m\r\n`,
      );
      return null;
    }
  }

  /**
   * Attach (or re-attach) a pane to its host session.
   *
   * `fresh` distinguishes two resume cases needing different offsets:
   * - fresh buffer (app launch): replay from 0 so the ring repaints the
   *   terminal — attaching at the old offset shows a blank pane until new
   *   output arrives;
   * - live buffer (network blip): resume at lastSeq so nothing duplicates.
   */
  private async attach(pane: Pane, fresh: boolean): Promise<boolean> {
    const since = fresh ? 0n : pane.lastSeq;
    try {
      const r = await this.conn.send("pty.attach", {
        session_id: pane.sessionId,
        since: Number(since),
      });
      if (fresh) {
        pane.term.reset();
        pane.decoder = new TextDecoder("utf-8");
        pane.lastSeq = 0n;
      }
      pane.handle = r.handle;
      this.byHandle.set(r.handle, pane);
      if (r.truncated_from != null) {
        // An honest marker beats a silent gap in an agent transcript.
        pane.term.write("\x1b[2m— older output lost —\x1b[0m\r\n");
        pane.lastSeq = BigInt(r.truncated_from);
      }
      return true;
    } catch {
      return false;
    }
  }

  /** Called on every (re)connect. */
  async onConnected(): Promise<void> {
    const fresh = this.firstOpen;
    this.firstOpen = false;

    if (fresh) {
      const saved = this.loadWorkspace();
      if (saved) {
        for (const id of saved.sessions) this.makePane(id);
        const results = await Promise.all(
          this.panes.map((p) => this.attach(p, true)),
        );
        // Prune sessions the host no longer has (restart, retention sweep).
        this.panes.filter((_, i) => !results[i]).forEach((p) => this.destroyPane(p));
        this.activeIdx = Math.min(
          saved.active,
          Math.max(0, this.panes.length - 1),
        );
      }
      if (this.panes.length === 0) {
        const n = saved ? 1 : FIRST_LAUNCH_PANES;
        for (let i = 0; i < n; i++) await this.createPane();
      }
    } else {
      // Network blip: buffers are intact, resume each pane where it left off.
      this.byHandle.clear();
      for (const pane of this.panes) {
        pane.handle = null;
        if (pane.alive) await this.attach(pane, false);
      }
    }

    this.persist();
    this.renderTabs();
    this.setActive(Math.min(this.activeIdx, this.panes.length - 1));
  }

  // -------------------------------------------------------------------------
  // Tabs
  // -------------------------------------------------------------------------

  renderTabs(): void {
    this.tabsEl.textContent = "";
    this.panes.forEach((pane, i) => {
      const b = document.createElement("button");
      b.className =
        "tab" +
        (i === this.activeIdx ? " active" : "") +
        (pane.alive ? "" : " dead");
      b.textContent = `${i + 1}`;
      b.title = pane.sessionId;
      b.addEventListener("click", () => this.setActive(i));
      this.tabsEl.appendChild(b);
    });
  }

  setActive(i: number): void {
    if (i < 0 || i >= this.panes.length) return;
    this.activeIdx = i;
    this.panes.forEach((p, j) => p.el.classList.toggle("active", j === i));
    this.renderTabs();
    this.persist();
    const pane = this.panes[i];
    pane.fit.fit();
    pane.term.focus();
  }

  async newPane(): Promise<void> {
    const pane = await this.createPane();
    if (pane) {
      this.persist();
      this.setActive(this.panes.length - 1);
    }
  }

  async closeActive(): Promise<void> {
    const pane = this.panes[this.activeIdx];
    if (!pane) return;
    if (pane.alive) {
      this.conn
        .send("pty.kill", { session_id: pane.sessionId })
        .catch(() => {});
    }
    this.destroyPane(pane);
    if (this.panes.length === 0) await this.createPane();
    this.persist();
    this.setActive(Math.min(this.activeIdx, this.panes.length - 1));
  }

  /** Re-fit the visible pane; call when the view becomes visible or resizes. */
  refit(): void {
    this.panes[this.activeIdx]?.fit.fit();
  }

  /** Dev-only introspection for headless verification. */
  debug() {
    return {
      panes: this.panes,
      count: () => this.panes.length,
      activeIdx: () => this.activeIdx,
      send: (i: number, data: string) => {
        const p = this.panes[i];
        if (p?.handle != null) this.conn.sendInput(p.handle, data);
      },
      text: (i: number) => {
        const buf = this.panes[i]?.term.buffer.active;
        if (!buf) return "";
        const lines: string[] = [];
        for (let r = 0; r < buf.length; r++) {
          lines.push(buf.getLine(r)?.translateToString(true) ?? "");
        }
        return lines.join("\n");
      },
    };
  }
}
