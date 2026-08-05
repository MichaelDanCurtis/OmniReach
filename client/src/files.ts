import { FrameKind } from "./frame";
import type { Connection } from "./connection";

/** Matches the design's 1 MB chunking with resume-by-offset. */
const CHUNK = 1024 * 1024;

interface Entry {
  name: string;
  path: string;
  is_dir: boolean;
  is_symlink: boolean;
  size: number;
  modified_ms: number | null;
}

function humanSize(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

function humanTime(ms: number | null): string {
  if (ms == null) return "";
  return new Date(ms).toLocaleString();
}

/** Join a directory and a child name without assuming the host's separator. */
function joinPath(dir: string, name: string): string {
  const sep = dir.includes("\\") && !dir.startsWith("/") ? "\\" : "/";
  return dir.endsWith(sep) ? dir + name : dir + sep + name;
}

function parentPath(p: string): string {
  const sep = p.includes("\\") && !p.startsWith("/") ? "\\" : "/";
  const trimmed = p.endsWith(sep) && p.length > 1 ? p.slice(0, -1) : p;
  const i = trimmed.lastIndexOf(sep);
  if (i <= 0) return sep === "\\" ? trimmed.slice(0, i + 1) : "/";
  return trimmed.slice(0, i);
}

export class FilesView {
  private cwd = "";
  private entries: Entry[] = [];
  private selected: string | null = null;

  private pathEl!: HTMLInputElement;
  private listEl!: HTMLDivElement;
  private statusEl!: HTMLDivElement;

  /** In-flight download collectors, keyed by the control-request id. */
  private collectors = new Map<number, Uint8Array[]>();

  constructor(
    private conn: Connection,
    private root: HTMLDivElement,
  ) {
    this.buildDom();
    conn.onFsChunk((handle, _seq, data) => {
      // Copy: the frame's payload is a view over the socket buffer, which the
      // next message reuses. Keeping the view would alias future data.
      this.collectors.get(handle)?.push(new Uint8Array(data));
    });
  }

  private buildDom(): void {
    this.root.className = "view files";
    this.root.innerHTML = `
      <div class="fs-bar">
        <button class="ctl" data-act="up" title="Parent directory">↑</button>
        <button class="ctl" data-act="home" title="Home">⌂</button>
        <input class="fs-path" spellcheck="false" />
        <button class="ctl" data-act="go" title="Go">→</button>
        <span class="fs-spacer"></span>
        <button class="ctl wide" data-act="upload">Upload</button>
        <button class="ctl wide" data-act="mkdir">New folder</button>
        <button class="ctl wide" data-act="download">Download</button>
        <button class="ctl wide" data-act="rename">Rename</button>
        <button class="ctl wide danger" data-act="delete">Delete</button>
      </div>
      <div class="fs-list"></div>
      <div class="fs-status"></div>
    `;
    this.pathEl = this.root.querySelector(".fs-path") as HTMLInputElement;
    this.listEl = this.root.querySelector(".fs-list") as HTMLDivElement;
    this.statusEl = this.root.querySelector(".fs-status") as HTMLDivElement;

    this.root.querySelector(".fs-bar")!.addEventListener("click", (e) => {
      const act = (e.target as HTMLElement).dataset?.act;
      if (!act) return;
      void this.onAction(act);
    });
    this.pathEl.addEventListener("keydown", (e) => {
      if ((e as KeyboardEvent).key === "Enter") void this.navigate(this.pathEl.value);
    });
  }

  private setStatus(msg: string, kind: "info" | "error" = "info"): void {
    this.statusEl.textContent = msg;
    this.statusEl.className = `fs-status ${kind}`;
  }

  private async onAction(act: string): Promise<void> {
    try {
      switch (act) {
        case "up":
          if (this.cwd) await this.navigate(parentPath(this.cwd));
          break;
        case "home":
          await this.goHome();
          break;
        case "go":
          await this.navigate(this.pathEl.value);
          break;
        case "download":
          await this.downloadSelected();
          break;
        case "upload":
          await this.uploadPrompt();
          break;
        case "mkdir":
          await this.mkdirPrompt();
          break;
        case "rename":
          await this.renamePrompt();
          break;
        case "delete":
          await this.deleteSelected();
          break;
      }
    } catch (e) {
      this.setStatus(String(e), "error");
    }
  }

  async onConnected(): Promise<void> {
    if (!this.cwd) await this.goHome();
  }

  private async goHome(): Promise<void> {
    const r = await this.conn.send("fs.roots", {});
    await this.navigate(r.home ?? r.roots?.[0] ?? "/");
  }

  async navigate(path: string): Promise<void> {
    if (!path) return;
    const r = await this.conn.send("fs.list", { path });
    this.cwd = r.path ?? path;
    this.entries = r.entries ?? [];
    this.selected = null;
    this.pathEl.value = this.cwd;
    this.render();
    this.setStatus(`${this.entries.length} items`);
  }

  private render(): void {
    this.listEl.textContent = "";

    // Directories first, then files; each alphabetical. Matches every file
    // manager the user already knows.
    const sorted = [...this.entries].sort((a, b) => {
      if (a.is_dir !== b.is_dir) return a.is_dir ? -1 : 1;
      return a.name.localeCompare(b.name);
    });

    for (const ent of sorted) {
      const row = document.createElement("div");
      row.className = "fs-row" + (ent.path === this.selected ? " selected" : "");

      const icon = document.createElement("span");
      icon.className = "fs-icon";
      icon.textContent = ent.is_symlink ? "↪" : ent.is_dir ? "▸" : "·";

      const name = document.createElement("span");
      name.className = "fs-name";
      name.textContent = ent.name;

      const size = document.createElement("span");
      size.className = "fs-size";
      size.textContent = ent.is_dir ? "" : humanSize(ent.size);

      const time = document.createElement("span");
      time.className = "fs-time";
      time.textContent = humanTime(ent.modified_ms);

      row.append(icon, name, size, time);
      row.addEventListener("click", () => {
        this.selected = ent.path;
        this.render();
      });
      row.addEventListener("dblclick", () => {
        if (ent.is_dir) void this.navigate(ent.path).catch((e) => this.setStatus(String(e), "error"));
        else void this.download(ent).catch((e) => this.setStatus(String(e), "error"));
      });
      this.listEl.appendChild(row);
    }
  }

  private selectedEntry(): Entry | null {
    return this.entries.find((e) => e.path === this.selected) ?? null;
  }

  // -------------------------------------------------------------------------
  // Transfer
  // -------------------------------------------------------------------------

  private async downloadSelected(): Promise<void> {
    const ent = this.selectedEntry();
    if (!ent) return this.setStatus("select a file first", "error");
    if (ent.is_dir) return this.setStatus("cannot download a directory", "error");
    await this.download(ent);
  }

  /**
   * Pull the file chunk by chunk.
   *
   * The request id is allocated BEFORE sending so the binary collector is
   * registered first — a chunk can never arrive for an unknown key, which
   * removes the buffering race entirely.
   */
  private async download(ent: Entry): Promise<void> {
    const parts: Uint8Array[] = [];
    let offset = 0;

    for (;;) {
      const id = this.conn.takeId();
      this.collectors.set(id, []);
      try {
        const r = await this.conn.sendWithId(id, "fs.read", {
          path: ent.path,
          offset,
          len: CHUNK,
        });
        for (const p of this.collectors.get(id) ?? []) parts.push(p);
        const got = Number(r.len ?? 0);
        offset += got;
        this.setStatus(
          `downloading ${ent.name} — ${humanSize(offset)}${
            r.total ? ` / ${humanSize(Number(r.total))}` : ""
          }`,
        );
        if (r.eof || got === 0) break;
      } finally {
        this.collectors.delete(id);
      }
    }

    const blob = new Blob(parts as BlobPart[]);
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = ent.name;
    a.click();
    // Revoke on the next tick: revoking synchronously can race the download.
    setTimeout(() => URL.revokeObjectURL(url), 10_000);
    this.setStatus(`downloaded ${ent.name} (${humanSize(blob.size)})`);
  }

  private async uploadPrompt(): Promise<void> {
    const input = document.createElement("input");
    input.type = "file";
    input.multiple = true;
    input.addEventListener("change", async () => {
      for (const file of Array.from(input.files ?? [])) {
        try {
          await this.upload(file);
        } catch (e) {
          this.setStatus(`${file.name}: ${e}`, "error");
          return;
        }
      }
      await this.navigate(this.cwd);
    });
    input.click();
  }

  /**
   * Push a file chunk by chunk. Each chunk's binary frame is sent BEFORE its
   * control message, so the host always has the bytes in hand when the write
   * op arrives — WebSocket preserves order, so this is deterministic.
   *
   * Every chunk carries its absolute offset, which makes a resumed upload just
   * a write at the offset the host already has.
   */
  private async upload(file: File): Promise<void> {
    const dest = joinPath(this.cwd, file.name);
    let offset = 0;

    while (offset < file.size) {
      const slice = file.slice(offset, offset + CHUNK);
      const bytes = new Uint8Array(await slice.arrayBuffer());
      const id = this.conn.takeId();
      this.conn.sendBinary(FrameKind.FsChunk, id, BigInt(offset), bytes);
      await this.conn.sendWithId(id, "fs.write", {
        path: dest,
        offset,
        len: bytes.length,
      });
      offset += bytes.length;
      this.setStatus(
        `uploading ${file.name} — ${humanSize(offset)} / ${humanSize(file.size)}`,
      );
    }
    // A zero-byte file still needs creating; the loop above never runs for it.
    if (file.size === 0) {
      const id = this.conn.takeId();
      this.conn.sendBinary(FrameKind.FsChunk, id, 0n, new Uint8Array());
      await this.conn.sendWithId(id, "fs.write", { path: dest, offset: 0, len: 0 });
    }
    this.setStatus(`uploaded ${file.name}`);
  }

  // -------------------------------------------------------------------------
  // Mutations
  // -------------------------------------------------------------------------

  private async mkdirPrompt(): Promise<void> {
    const name = prompt("New folder name:");
    if (!name) return;
    await this.conn.send("fs.mkdir", { path: joinPath(this.cwd, name) });
    await this.navigate(this.cwd);
  }

  private async renamePrompt(): Promise<void> {
    const ent = this.selectedEntry();
    if (!ent) return this.setStatus("select something first", "error");
    const name = prompt("Rename to:", ent.name);
    if (!name || name === ent.name) return;
    await this.conn.send("fs.rename", {
      from: ent.path,
      to: joinPath(this.cwd, name),
    });
    await this.navigate(this.cwd);
  }

  private async deleteSelected(): Promise<void> {
    const ent = this.selectedEntry();
    if (!ent) return this.setStatus("select something first", "error");
    // Confirm every delete: the host refuses non-empty directories, but a
    // single mis-click on the wrong file is still unrecoverable.
    if (!confirm(`Delete ${ent.name}? This cannot be undone.`)) return;
    await this.conn.send("fs.remove", { path: ent.path });
    await this.navigate(this.cwd);
  }

  /** Dev-only introspection for headless verification. */
  debug() {
    return {
      cwd: () => this.cwd,
      entries: () => this.entries,
      navigate: (p: string) => this.navigate(p),
      select: (p: string) => {
        this.selected = p;
        this.render();
      },
      // Exercises the real chunked read path and returns the assembled bytes.
      readAll: async (path: string): Promise<Uint8Array> => {
        const parts: Uint8Array[] = [];
        let offset = 0;
        for (;;) {
          const id = this.conn.takeId();
          this.collectors.set(id, []);
          try {
            const r = await this.conn.sendWithId(id, "fs.read", {
              path,
              offset,
              len: CHUNK,
            });
            for (const p of this.collectors.get(id) ?? []) parts.push(p);
            const got = Number(r.len ?? 0);
            offset += got;
            if (r.eof || got === 0) break;
          } finally {
            this.collectors.delete(id);
          }
        }
        const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
        let at = 0;
        for (const p of parts) {
          out.set(p, at);
          at += p.length;
        }
        return out;
      },
      writeAll: async (path: string, bytes: Uint8Array): Promise<void> => {
        const id = this.conn.takeId();
        this.conn.sendBinary(FrameKind.FsChunk, id, 0n, bytes);
        await this.conn.sendWithId(id, "fs.write", {
          path,
          offset: 0,
          len: bytes.length,
        });
      },
    };
  }
}
