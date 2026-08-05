import "@xterm/xterm/css/xterm.css";
import { Connection, type Status } from "./connection";
import { TerminalsView } from "./terminals";
import { FilesView } from "./files";
import { BrowserView } from "./browser";

/**
 * Where to connect.
 *
 * Inside the Tauri app the Rust core runs a loopback proxy that attaches
 * Cloudflare Access credentials — the webview's WebSocket API cannot set
 * request headers, so it cannot authenticate on its own. In a plain browser
 * (dev server) there is no proxy, so connect directly.
 */
async function resolveHostUrl(): Promise<string> {
  const override = import.meta.env.VITE_HOST_URL as string | undefined;
  if (override) return override;

  const tauri = (window as any).__TAURI__;
  if (tauri?.core?.invoke) {
    // Let a failure surface: a proxy that did not bind means credentials
    // would be silently dropped, which fails later and less clearly.
    return await tauri.core.invoke("ws_url");
  }
  return "ws://127.0.0.1:7717/ws";
}

type ViewName = "terminals" | "files" | "browser";
const VIEW_KEY = "omnireach.view";

const statusEl = document.getElementById("status") as HTMLSpanElement;
const navEl = document.getElementById("nav") as HTMLElement;
const termTabsEl = document.getElementById("term-tabs") as HTMLElement;
const termCtlsEl = document.getElementById("term-ctls") as HTMLElement;
const viewsEl = document.getElementById("views") as HTMLDivElement;

// Placeholder until `resolveHostUrl` answers; `connect()` is not called until
// then, so the value is never used to dial.
const conn = new Connection("ws://127.0.0.1:7717/ws");

// Each view owns its own root element; switching toggles `.visible`.
const termRoot = document.createElement("div");
termRoot.className = "view terminals";
const filesRoot = document.createElement("div");
const browserRoot = document.createElement("div");
viewsEl.append(termRoot, filesRoot, browserRoot);

const terminals = new TerminalsView(conn, termRoot, termTabsEl);
const files = new FilesView(conn, filesRoot);
const browser = new BrowserView(conn, browserRoot);

const roots: Record<ViewName, HTMLDivElement> = {
  terminals: termRoot,
  files: filesRoot,
  browser: browserRoot,
};

let current: ViewName =
  (localStorage.getItem(VIEW_KEY) as ViewName | null) ?? "terminals";

function setStatus(s: Status) {
  statusEl.textContent = s;
  statusEl.className = `status ${s}`;
}

function renderNav(): void {
  navEl.textContent = "";
  (["terminals", "files", "browser"] as ViewName[]).forEach((name) => {
    const b = document.createElement("button");
    b.className = "nav-btn" + (name === current ? " active" : "");
    b.textContent = name;
    b.dataset.view = name;
    b.addEventListener("click", () => void showView(name));
    navEl.appendChild(b);
  });
}

async function showView(name: ViewName): Promise<void> {
  current = name;
  localStorage.setItem(VIEW_KEY, name);
  for (const [k, el] of Object.entries(roots)) {
    el.classList.toggle("visible", k === name);
  }
  // Terminal tabs and controls belong to the terminals view only.
  termTabsEl.classList.toggle("hidden", name !== "terminals");
  termCtlsEl.classList.toggle("hidden", name !== "terminals");
  renderNav();

  if (name === "terminals") {
    // xterm cannot measure a hidden element, so re-fit once visible.
    terminals.refit();
  } else if (name === "browser") {
    await browser.onShown();
  } else if (name === "files") {
    await files.onConnected().catch(() => {});
  }
}

conn.onStatus(setStatus);

conn.onOpen(async () => {
  await terminals.onConnected();
  await showView(current);
});

document.getElementById("new-pane")!.addEventListener("click", () => {
  void terminals.newPane();
});
document.getElementById("close-pane")!.addEventListener("click", () => {
  void terminals.closeActive();
});

window.addEventListener("resize", () => {
  if (current === "terminals") terminals.refit();
});

renderNav();
setStatus("disconnected");

resolveHostUrl()
  .then((url) => {
    conn.setUrl(url);
    conn.connect();
  })
  .catch((e) => {
    setStatus("disconnected");
    statusEl.title = String(e);
    // Visible, not silent: without a URL there is nothing to retry against.
    const banner = document.createElement("div");
    banner.className = "fatal";
    banner.textContent = `Cannot determine host: ${e}`;
    document.body.prepend(banner);
  });

// Dev-only hook so headless verification can drive the app's REAL code paths
// rather than reimplementing them. Not defined in production builds.
if (import.meta.env.DEV) {
  (window as unknown as Record<string, unknown>).__omnireach = {
    ...terminals.debug(),
    files: files.debug(),
    browser: browser.debug(),
    view: () => current,
    showView: (n: ViewName) => showView(n),
  };
}
