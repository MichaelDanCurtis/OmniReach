import type { Connection } from "./connection";

interface Tab {
  tab_id: string;
  handle: number;
  url: string;
  title: string;
  active: boolean;
}

interface FrameMeta {
  width: number;
  height: number;
  device_width: number;
  device_height: number;
}

/** Keys xterm-style names don't cover; CDP wants a `key` plus sometimes text. */
const KEY_ALIASES: Record<string, string> = {
  Backspace: "Backspace",
  Enter: "Enter",
  Tab: "Tab",
  Escape: "Escape",
  ArrowUp: "ArrowUp",
  ArrowDown: "ArrowDown",
  ArrowLeft: "ArrowLeft",
  ArrowRight: "ArrowRight",
  Delete: "Delete",
  Home: "Home",
  End: "End",
  PageUp: "PageUp",
  PageDown: "PageDown",
};

export class BrowserView {
  private tabs: Tab[] = [];
  private activeTab: string | null = null;
  private launched = false;

  private canvas!: HTMLCanvasElement;
  private ctx!: CanvasRenderingContext2D;
  private urlEl!: HTMLInputElement;
  private tabsEl!: HTMLDivElement;
  private consoleEl!: HTMLDivElement;
  private statusEl!: HTMLDivElement;

  /** Latest frame metadata, needed to map a click back to viewport coords. */
  private meta: FrameMeta | null = null;
  private frameCount = 0;

  constructor(
    private conn: Connection,
    private root: HTMLDivElement,
  ) {
    this.buildDom();

    conn.onBrowserFrame((handle, _seq, jpeg) => {
      const tab = this.tabs.find((t) => t.handle === handle);
      if (!tab || tab.tab_id !== this.activeTab) return; // stale/background tab
      this.drawFrame(jpeg);
    });

    conn.onEvent((msg) => {
      if (msg.event === "browser.console") {
        this.appendConsole(msg.level, msg.text);
      } else if (msg.event === "browser.frame_meta") {
        this.meta = {
          width: msg.width,
          height: msg.height,
          device_width: msg.device_width,
          device_height: msg.device_height,
        };
      } else if (msg.event === "browser.download") {
        this.appendConsole("info", `downloaded → ${msg.path}`);
      }
    });
  }

  private buildDom(): void {
    this.root.className = "view browser";
    this.root.innerHTML = `
      <div class="br-bar">
        <button class="ctl" data-act="back" title="Back">←</button>
        <button class="ctl" data-act="forward" title="Forward">→</button>
        <button class="ctl" data-act="reload" title="Reload">⟳</button>
        <input class="br-url" placeholder="https://…" spellcheck="false" />
        <button class="ctl" data-act="go">Go</button>
        <button class="ctl" data-act="newtab" title="New tab">+</button>
        <button class="ctl" data-act="closetab" title="Close tab">×</button>
        <button class="ctl wide" data-act="console">Console</button>
      </div>
      <div class="br-tabs"></div>
      <div class="br-stage"><canvas class="br-canvas"></canvas></div>
      <div class="br-console hidden">
        <div class="br-console-log"></div>
        <input class="br-eval" placeholder="evaluate JavaScript…" spellcheck="false" />
      </div>
      <div class="br-status"></div>
    `;

    this.canvas = this.root.querySelector(".br-canvas") as HTMLCanvasElement;
    this.ctx = this.canvas.getContext("2d")!;
    this.urlEl = this.root.querySelector(".br-url") as HTMLInputElement;
    this.tabsEl = this.root.querySelector(".br-tabs") as HTMLDivElement;
    this.consoleEl = this.root.querySelector(".br-console-log") as HTMLDivElement;
    this.statusEl = this.root.querySelector(".br-status") as HTMLDivElement;

    this.root.querySelector(".br-bar")!.addEventListener("click", (e) => {
      const act = (e.target as HTMLElement).dataset?.act;
      if (act) void this.onAction(act);
    });
    this.urlEl.addEventListener("keydown", (e) => {
      if ((e as KeyboardEvent).key === "Enter") void this.go(this.urlEl.value);
    });

    const evalEl = this.root.querySelector(".br-eval") as HTMLInputElement;
    evalEl.addEventListener("keydown", async (e) => {
      if ((e as KeyboardEvent).key !== "Enter" || !this.activeTab) return;
      const expr = evalEl.value;
      evalEl.value = "";
      this.appendConsole("input", expr);
      try {
        const r = await this.conn.send("browser.evaluate", {
          tab_id: this.activeTab,
          expression: expr,
        });
        this.appendConsole("result", String(r.result ?? ""));
      } catch (err) {
        this.appendConsole("error", String(err));
      }
    });

    this.wireInput();
  }

  // -------------------------------------------------------------------------
  // Rendering
  // -------------------------------------------------------------------------

  private drawFrame(jpeg: Uint8Array): void {
    // Copy before handing to Blob: the payload is a view over the socket
    // buffer, which the next message reuses.
    const blob = new Blob([new Uint8Array(jpeg)], { type: "image/jpeg" });
    const url = URL.createObjectURL(blob);
    const img = new Image();
    img.onload = () => {
      if (this.canvas.width !== img.width || this.canvas.height !== img.height) {
        this.canvas.width = img.width;
        this.canvas.height = img.height;
      }
      this.ctx.drawImage(img, 0, 0);
      URL.revokeObjectURL(url);
      this.frameCount++;
    };
    img.onerror = () => URL.revokeObjectURL(url);
    img.src = url;
  }

  private setStatus(msg: string, kind: "info" | "error" = "info"): void {
    this.statusEl.textContent = msg;
    this.statusEl.className = `br-status ${kind}`;
  }

  private appendConsole(level: string, text: string): void {
    const line = document.createElement("div");
    line.className = `br-line ${level}`;
    line.textContent = text;
    this.consoleEl.appendChild(line);
    this.consoleEl.scrollTop = this.consoleEl.scrollHeight;
  }

  // -------------------------------------------------------------------------
  // Input forwarding
  // -------------------------------------------------------------------------

  /**
   * Map a pointer event on the canvas to CSS-viewport coordinates.
   *
   * Two scalings compose here and BOTH matter: the canvas is CSS-displayed at
   * a size that differs from its pixel size, and the screencast JPEG is itself
   * a scaled render of the viewport. Collapsing either one is why clicks land
   * off-target.
   */
  private toViewport(e: MouseEvent): { x: number; y: number } {
    const rect = this.canvas.getBoundingClientRect();
    const imgW = this.canvas.width || 1;
    const imgH = this.canvas.height || 1;

    const imgX = ((e.clientX - rect.left) / (rect.width || 1)) * imgW;
    const imgY = ((e.clientY - rect.top) / (rect.height || 1)) * imgH;

    const devW = this.meta?.device_width || imgW;
    const devH = this.meta?.device_height || imgH;

    return { x: (imgX * devW) / imgW, y: (imgY * devH) / imgH };
  }

  private wireInput(): void {
    const mouse = (kind: string) => async (e: MouseEvent) => {
      if (!this.activeTab) return;
      e.preventDefault();
      const { x, y } = this.toViewport(e);
      await this.conn
        .send("browser.mouse", {
          tab_id: this.activeTab,
          kind,
          x,
          y,
          button: e.button === 2 ? "right" : e.button === 1 ? "middle" : "left",
          click_count: 1,
        })
        .catch(() => {});
    };

    this.canvas.addEventListener("mousedown", mouse("mousePressed"));
    this.canvas.addEventListener("mouseup", mouse("mouseReleased"));
    // Throttle moves to animation frames: an unthrottled move stream would
    // flood the tunnel with control frames and starve the screencast.
    let moveQueued = false;
    this.canvas.addEventListener("mousemove", (e) => {
      if (moveQueued || !this.activeTab) return;
      moveQueued = true;
      requestAnimationFrame(() => {
        moveQueued = false;
        void mouse("mouseMoved")(e);
      });
    });
    this.canvas.addEventListener("contextmenu", (e) => e.preventDefault());

    this.canvas.addEventListener("wheel", async (e) => {
      if (!this.activeTab) return;
      e.preventDefault();
      const { x, y } = this.toViewport(e);
      await this.conn
        .send("browser.mouse", {
          tab_id: this.activeTab,
          kind: "mouseWheel",
          x,
          y,
          button: "none",
          click_count: 0,
          delta_x: e.deltaX,
          delta_y: e.deltaY,
        })
        .catch(() => {});
    });

    // The canvas must be focusable for it to receive key events at all.
    this.canvas.tabIndex = 0;
    this.canvas.addEventListener("keydown", async (e) => {
      if (!this.activeTab) return;
      e.preventDefault();
      const key = KEY_ALIASES[e.key] ?? e.key;
      // Printable keys carry text; named keys must not, or Chrome inserts the
      // literal name into the page.
      const text = e.key.length === 1 ? e.key : undefined;
      await this.conn
        .send("browser.key", {
          tab_id: this.activeTab,
          kind: text ? "keyDown" : "rawKeyDown",
          key,
          text,
        })
        .catch(() => {});
    });
    this.canvas.addEventListener("keyup", async (e) => {
      if (!this.activeTab) return;
      e.preventDefault();
      await this.conn
        .send("browser.key", {
          tab_id: this.activeTab,
          kind: "keyUp",
          key: KEY_ALIASES[e.key] ?? e.key,
          text: null,
        })
        .catch(() => {});
    });
  }

  // -------------------------------------------------------------------------
  // Actions
  // -------------------------------------------------------------------------

  private async onAction(act: string): Promise<void> {
    try {
      switch (act) {
        case "go":
          await this.go(this.urlEl.value);
          break;
        case "back":
          await this.evalNav("history.back()");
          break;
        case "forward":
          await this.evalNav("history.forward()");
          break;
        case "reload":
          await this.evalNav("location.reload()");
          break;
        case "newtab":
          await this.newTab();
          break;
        case "closetab":
          await this.closeActive();
          break;
        case "console":
          this.root.querySelector(".br-console")!.classList.toggle("hidden");
          break;
      }
    } catch (e) {
      this.setStatus(String(e), "error");
    }
  }

  private async evalNav(expr: string): Promise<void> {
    if (!this.activeTab) return;
    await this.conn.send("browser.evaluate", {
      tab_id: this.activeTab,
      expression: expr,
    });
  }

  /** Lazily launch Chrome on the host — it is expensive, so not at startup. */
  async ensureLaunched(): Promise<void> {
    if (this.launched) return;
    this.setStatus("starting remote browser…");
    await this.conn.send("browser.launch", {});
    this.launched = true;
    await this.refreshTabs();
    if (this.tabs.length === 0) await this.newTab();
    this.setStatus("ready");
  }

  async onShown(): Promise<void> {
    try {
      await this.ensureLaunched();
    } catch (e) {
      this.setStatus(`could not start remote browser: ${e}`, "error");
    }
  }

  private async refreshTabs(): Promise<void> {
    const r = await this.conn.send("browser.list_tabs", {});
    this.tabs = r.tabs ?? [];
    if (this.activeTab && !this.tabs.some((t) => t.tab_id === this.activeTab)) {
      this.activeTab = null;
    }
    if (!this.activeTab && this.tabs.length) {
      await this.selectTab(this.tabs[0].tab_id);
    }
    this.renderTabs();
  }

  private renderTabs(): void {
    this.tabsEl.textContent = "";
    for (const t of this.tabs) {
      const b = document.createElement("button");
      b.className = "tab" + (t.tab_id === this.activeTab ? " active" : "");
      b.textContent = (t.title || t.url || "new tab").slice(0, 24);
      b.title = t.url;
      b.addEventListener("click", () => void this.selectTab(t.tab_id));
      this.tabsEl.appendChild(b);
    }
  }

  async selectTab(tabId: string): Promise<void> {
    // Only one tab screencasts at a time — background tabs stream nothing,
    // which is the whole reason this is cheap enough to run over a tunnel.
    if (this.activeTab && this.activeTab !== tabId) {
      await this.conn
        .send("browser.screencast_stop", { tab_id: this.activeTab })
        .catch(() => {});
    }
    this.activeTab = tabId;
    await this.conn.send("browser.activate_tab", { tab_id: tabId });
    await this.conn.send("browser.screencast_start", {
      tab_id: tabId,
      quality: 60,
      max_width: 1600,
    });
    const t = this.tabs.find((x) => x.tab_id === tabId);
    if (t) this.urlEl.value = t.url;
    this.renderTabs();
    this.canvas.focus();
  }

  private async newTab(): Promise<void> {
    const r = await this.conn.send("browser.new_tab", { url: "about:blank" });
    await this.refreshTabs();
    if (r.tab?.tab_id) await this.selectTab(r.tab.tab_id);
  }

  private async closeActive(): Promise<void> {
    if (!this.activeTab) return;
    await this.conn.send("browser.close_tab", { tab_id: this.activeTab });
    this.activeTab = null;
    await this.refreshTabs();
  }

  async go(url: string): Promise<void> {
    if (!url || !this.activeTab) return;
    const full = /^[a-z]+:/i.test(url) ? url : `https://${url}`;
    await this.conn.send("browser.navigate", { tab_id: this.activeTab, url: full });
    this.urlEl.value = full;
    setTimeout(() => void this.refreshTabs().catch(() => {}), 800);
  }

  /** Dev-only introspection for headless verification. */
  debug() {
    return {
      tabs: () => this.tabs,
      activeTab: () => this.activeTab,
      frames: () => this.frameCount,
      meta: () => this.meta,
      canvasSize: () => ({ w: this.canvas.width, h: this.canvas.height }),
      ensureLaunched: () => this.ensureLaunched(),
      go: (u: string) => this.go(u),
      evaluate: async (expr: string) => {
        if (!this.activeTab) return null;
        const r = await this.conn.send("browser.evaluate", {
          tab_id: this.activeTab,
          expression: expr,
        });
        return r.result;
      },
      clickAt: (cssX: number, cssY: number) => {
        const rect = this.canvas.getBoundingClientRect();
        const ev = new MouseEvent("mousedown", {
          clientX: rect.left + cssX,
          clientY: rect.top + cssY,
          bubbles: true,
        });
        this.canvas.dispatchEvent(ev);
        this.canvas.dispatchEvent(
          new MouseEvent("mouseup", {
            clientX: rect.left + cssX,
            clientY: rect.top + cssY,
            bubbles: true,
          }),
        );
      },
    };
  }
}
