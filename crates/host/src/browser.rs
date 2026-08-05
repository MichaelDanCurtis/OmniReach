//! Remote headless-Chrome session (M6/M7).
//!
//! One Chrome process per host, launched lazily, driven over raw CDP. The
//! service owns tabs, streams the active tab as JPEG frames, forwards devtools
//! console output, injects mouse and key events, and points downloads at a
//! directory the file browser can see.
//!
//! # Four traps, all of which make a failure look like a hang
//!
//! 1. **`Browser::launch` returns `(Browser, Handler)` and the `Handler` is a
//!    stream that MUST be polled.** It is the thing that actually pumps the
//!    devtools websocket. Forget to `tokio::spawn` a loop draining it and every
//!    subsequent call — `new_page`, `execute`, everything — waits forever on a
//!    response that will never be decoded. No error, no timeout, just silence.
//!    See [`BrowserService::launch`].
//!
//! 2. **`Page.startScreencast` stalls after a handful of frames unless every
//!    frame is acknowledged** with `Page.screencastFrameAck`. Chrome keeps only
//!    a couple of unacked frames in flight, then stops producing. The symptom is
//!    a stream that looks fine for ~200ms and then freezes, which reads as a
//!    rendering bug rather than a protocol one. The ack is the FIRST thing the
//!    frame loop does, before decoding or fanning out — see
//!    [`BrowserService::start_screencast`].
//!
//! 3. **A `BrowserService` dropped without [`BrowserService::shutdown`] leaves a
//!    live Chrome process behind.** chromiumoxide's `Drop` only logs a warning;
//!    it cannot await the child. Tests that leak Chromes poison every later run
//!    on the machine, so `shutdown` closes the browser, reaps the child, and
//!    only then stops the handler task — in that order, because `Browser::close`
//!    is itself a CDP round-trip and needs the handler alive.
//!
//! 4. **`thiserror` reserves a field named `source`** (it must implement
//!    `std::error::Error`). [`BrowserError`] therefore uses tuple variants, and
//!    `pty.rs` uses `cause` for the same reason.
//!
//! # Coordinate mapping
//!
//! `Input.dispatchMouseEvent` takes **CSS viewport** coordinates. A screencast
//! frame is a *scaled* render — Chrome honours `maxWidth`/`maxHeight` and picks
//! its own scale factor — so the JPEG's pixel dimensions are generally NOT the
//! viewport's. [`ScreencastFrame`] therefore carries both: `width`/`height` are
//! the JPEG's real pixel dimensions, `device_width`/`device_height` the CSS
//! viewport. The client maps a click as
//!
//! ```text
//! image_px.x  = (click.x / displayed_width) * width
//! viewport.x  = image_px.x * (device_width / width)
//! viewport.y  = image_px.y * (device_height / height) + offset_top
//! ```
//!
//! Assuming image pixels are viewport pixels is what makes clicks land tens of
//! pixels off target, so this is covered by a dedicated test that clicks a real
//! button and asserts the DOM changed.

use base64::Engine as _;
use chromiumoxide::cdp::browser_protocol::browser::{
    SetDownloadBehaviorBehavior, SetDownloadBehaviorParams,
};
use chromiumoxide::cdp::browser_protocol::input::{
    DispatchKeyEventParams, DispatchKeyEventType, DispatchMouseEventParams, DispatchMouseEventType,
    MouseButton,
};
use chromiumoxide::cdp::browser_protocol::page::{
    EnableParams as PageEnableParams, EventScreencastFrame, ScreencastFrameAckParams,
    StartScreencastFormat, StartScreencastParams, StopScreencastParams,
};
use chromiumoxide::cdp::browser_protocol::target::CreateTargetParams;
use chromiumoxide::cdp::js_protocol::runtime::{
    EvaluateParams, EventConsoleApiCalled, EventExceptionThrown, RemoteObject,
};
use chromiumoxide::keys::USKEYBOARD_LAYOUT;
use chromiumoxide::{Browser, BrowserConfig, Page};
use futures_util::StreamExt;
use parking_lot::Mutex;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// How long Chrome gets to print its devtools websocket URL before the launch
/// is called a failure.
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(20);

/// Ceiling on any single CDP round-trip we initiate. A wedged renderer must
/// surface as [`BrowserError::Timeout`], never as a stalled control channel — a
/// hang here would be indistinguishable from an idle browser.
const OP_TIMEOUT: Duration = Duration::from_secs(30);

/// Frames are large; a subscriber this far behind is not going to catch up, and
/// buffering more of them is just a slower way to run out of memory.
const FRAME_CAPACITY: usize = 64;
const CONSOLE_CAPACITY: usize = 256;

/// One tab, as the client sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabInfo {
    pub tab_id: String,
    pub url: String,
    pub title: String,
    pub active: bool,
}

/// A single screencast frame, already decoded.
///
/// `jpeg` is raw JPEG bytes, not base64: CDP delivers base64 but this is the
/// heaviest stream in the system and the wire frame is binary, so the ~33%
/// base64 tax is paid nowhere.
#[derive(Debug, Clone)]
pub struct ScreencastFrame {
    pub tab_id: String,
    /// Monotonic across the whole service, so a client can detect drops.
    pub seq: u64,
    pub jpeg: Vec<u8>,
    /// Real pixel width of `jpeg`, read from its SOF marker.
    pub width: u32,
    /// Real pixel height of `jpeg`, read from its SOF marker.
    pub height: u32,
    /// `metadata.offsetTop` — DIP offset of the visible viewport from the top of
    /// the page, e.g. under a fixed toolbar.
    pub offset_top: f64,
    /// `metadata.pageScaleFactor` — pinch zoom, 1.0 unzoomed.
    pub page_scale: f64,
    /// `metadata.deviceWidth` — CSS viewport width, the unit
    /// `Input.dispatchMouseEvent` expects.
    pub device_width: f64,
    /// `metadata.deviceHeight` — CSS viewport height.
    pub device_height: f64,
}

/// One devtools console line.
#[derive(Debug, Clone)]
pub struct ConsoleLine {
    pub tab_id: String,
    /// CDP's own level string: `log`, `debug`, `info`, `error`, `warning`,
    /// `trace`, … Uncaught exceptions are reported as `error`.
    pub level: String,
    pub text: String,
}

#[derive(Debug, Error)]
pub enum BrowserError {
    // Tuple variants throughout: see trap 4 in the module docs.
    #[error("failed to launch chrome: {0}")]
    Launch(String),
    #[error("no such tab")]
    NoSuchTab,
    #[error("cdp error: {0}")]
    Cdp(String),
    #[error("browser operation timed out")]
    Timeout,
}

impl BrowserError {
    pub fn code(&self) -> &'static str {
        match self {
            BrowserError::Launch(_) => "LAUNCH_FAILED",
            BrowserError::NoSuchTab => "NO_SUCH_TAB",
            BrowserError::Cdp(_) => "CDP_ERROR",
            BrowserError::Timeout => "TIMEOUT",
        }
    }
}

fn cdp(e: impl std::fmt::Display) -> BrowserError {
    BrowserError::Cdp(e.to_string())
}

/// Bound a CDP round-trip so a wedged Chrome reports rather than hangs.
async fn deadline<F, T>(fut: F) -> Result<T, BrowserError>
where
    F: Future<Output = Result<T, BrowserError>>,
{
    match tokio::time::timeout(OP_TIMEOUT, fut).await {
        Ok(r) => r,
        Err(_) => Err(BrowserError::Timeout),
    }
}

struct Tab {
    id: String,
    page: Page,
    /// Console + exception forwarders. Aborted when the tab closes.
    listeners: Vec<JoinHandle<()>>,
}

struct ActiveCast {
    tab_id: String,
    page: Page,
    task: JoinHandle<()>,
}

pub struct BrowserService {
    /// `None` after [`BrowserService::shutdown`]. A `tokio` mutex because the
    /// guard is held across CDP awaits.
    browser: tokio::sync::Mutex<Option<Browser>>,
    /// The task draining `Handler`. Trap 1: without it, nothing works.
    handler: Mutex<Option<JoinHandle<()>>>,
    tabs: Mutex<Vec<Tab>>,
    active: Mutex<Option<String>>,
    /// At most one tab screencasts at a time.
    screencast: tokio::sync::Mutex<Option<ActiveCast>>,
    frames_tx: broadcast::Sender<ScreencastFrame>,
    console_tx: broadcast::Sender<ConsoleLine>,
    seq: Arc<AtomicU64>,
    /// Throwaway Chrome profile. Sharing one with another Chrome makes the
    /// second launch silently attach to the first instead of starting.
    user_data_dir: PathBuf,
}

impl BrowserService {
    /// Launch Chrome and attach to its devtools websocket.
    ///
    /// `chrome_path` overrides executable detection. `downloads_dir` is where
    /// `Browser.setDownloadBehavior` drops files, so the M5 file browser can
    /// list what the remote browser downloaded; it is created if absent.
    pub async fn launch(
        chrome_path: Option<String>,
        downloads_dir: PathBuf,
    ) -> Result<Arc<Self>, BrowserError> {
        std::fs::create_dir_all(&downloads_dir).map_err(|e| {
            BrowserError::Launch(format!(
                "downloads dir {}: {e}",
                downloads_dir.to_string_lossy()
            ))
        })?;

        let user_data_dir =
            std::env::temp_dir().join(format!("omnireach-chrome-{}", Uuid::new_v4()));

        let mut builder = BrowserConfig::builder()
            // `--headless=new` rather than the old headless shell: the old one
            // needs `enableBeginFrameControl` before it produces screencast
            // frames at all.
            .new_headless_mode()
            .user_data_dir(&user_data_dir)
            .launch_timeout(LAUNCH_TIMEOUT);
        if let Some(path) = chrome_path {
            builder = builder.chrome_executable(path);
        }
        // `build` is where "no Chrome on this machine" surfaces, because that is
        // where executable detection runs.
        let config = builder.build().map_err(BrowserError::Launch)?;

        let (browser, mut handler) = Browser::launch(config)
            .await
            .map_err(|e| BrowserError::Launch(e.to_string()))?;

        // TRAP 1. This loop is not bookkeeping — it is the devtools websocket
        // pump. Every call below deadlocks without it.
        let handler_task = tokio::spawn(async move {
            while let Some(res) = handler.next().await {
                if let Err(e) = res {
                    tracing::debug!("chromiumoxide handler: {e}");
                }
            }
        });

        // Downloads land somewhere the file browser can reach, instead of being
        // blocked (headless Chrome's default) or scattered into the profile.
        let behavior = SetDownloadBehaviorParams {
            behavior: SetDownloadBehaviorBehavior::Allow,
            browser_context_id: None,
            download_path: Some(downloads_dir.to_string_lossy().into_owned()),
            events_enabled: Some(true),
        };
        if let Err(e) = browser.execute(behavior).await {
            // Not fatal: tabs, screencast and input all still work, only the
            // download destination is Chrome's default. Say so rather than
            // failing the whole session.
            tracing::warn!("Browser.setDownloadBehavior failed: {e}");
        }

        Ok(Arc::new(Self {
            browser: tokio::sync::Mutex::new(Some(browser)),
            handler: Mutex::new(Some(handler_task)),
            tabs: Mutex::new(Vec::new()),
            active: Mutex::new(None),
            screencast: tokio::sync::Mutex::new(None),
            frames_tx: broadcast::Sender::new(FRAME_CAPACITY),
            console_tx: broadcast::Sender::new(CONSOLE_CAPACITY),
            seq: Arc::new(AtomicU64::new(0)),
            user_data_dir,
        }))
    }

    /// Open a tab, defaulting to `about:blank`.
    pub async fn new_tab(&self, url: Option<String>) -> Result<TabInfo, BrowserError> {
        let target = url.unwrap_or_else(|| "about:blank".to_string());
        let page = deadline(async {
            let guard = self.browser.lock().await;
            let browser = guard.as_ref().ok_or_else(shut_down)?;
            browser
                .new_page(CreateTargetParams::new(target))
                .await
                .map_err(cdp)
        })
        .await?;

        // Screencast needs the Page domain; console capture needs Runtime.
        // chromiumoxide's target init enables neither.
        page.execute(PageEnableParams::default())
            .await
            .map_err(cdp)?;
        page.enable_runtime().await.map_err(cdp)?;

        let id = page.target_id().inner().clone();
        let listeners = self.spawn_console_listeners(&page, &id).await?;

        self.tabs.lock().push(Tab {
            id: id.clone(),
            page: page.clone(),
            listeners,
        });
        *self.active.lock() = Some(id.clone());

        Ok(tab_info(&page, id, true).await)
    }

    pub async fn list_tabs(&self) -> Result<Vec<TabInfo>, BrowserError> {
        // Snapshot under the lock, then await. Holding a sync guard across an
        // await is both a clippy error and a real deadlock risk.
        let snapshot: Vec<(String, Page)> = self
            .tabs
            .lock()
            .iter()
            .map(|t| (t.id.clone(), t.page.clone()))
            .collect();
        let active = self.active.lock().clone();

        let mut out = Vec::with_capacity(snapshot.len());
        for (id, page) in snapshot {
            let is_active = active.as_deref() == Some(id.as_str());
            out.push(tab_info(&page, id, is_active).await);
        }
        Ok(out)
    }

    pub async fn close_tab(&self, tab_id: &str) -> Result<(), BrowserError> {
        // Stop the cast first: closing the page out from under the ack loop just
        // produces noise on the way down.
        self.stop_cast_if(tab_id).await;

        let tab = {
            let mut tabs = self.tabs.lock();
            let idx = tabs
                .iter()
                .position(|t| t.id == tab_id)
                .ok_or(BrowserError::NoSuchTab)?;
            tabs.remove(idx)
        };
        for h in &tab.listeners {
            h.abort();
        }

        {
            let next = self.tabs.lock().last().map(|t| t.id.clone());
            let mut active = self.active.lock();
            if active.as_deref() == Some(tab_id) {
                *active = next;
            }
        }

        deadline(async { tab.page.close().await.map_err(cdp) }).await
    }

    pub async fn activate_tab(&self, tab_id: &str) -> Result<(), BrowserError> {
        let page = self.page(tab_id)?;
        deadline(async { page.activate().await.map(|_| ()).map_err(cdp) }).await?;
        *self.active.lock() = Some(tab_id.to_string());
        Ok(())
    }

    pub async fn navigate(&self, tab_id: &str, url: &str) -> Result<(), BrowserError> {
        let page = self.page(tab_id)?;
        deadline(async { page.goto(url).await.map(|_| ()).map_err(cdp) }).await
    }

    /// Start streaming JPEG frames for `tab_id` on [`Self::subscribe_frames`].
    ///
    /// Only one tab casts at a time; starting a new one stops the previous.
    /// `quality` is 0–100, `max_width` caps the rendered width in pixels —
    /// Chrome picks a scale that fits, which is why the frame carries both its
    /// own pixel size and the CSS viewport size.
    pub async fn start_screencast(
        &self,
        tab_id: &str,
        quality: u32,
        max_width: u32,
    ) -> Result<(), BrowserError> {
        let page = self.page(tab_id)?;
        let mut guard = self.screencast.lock().await;
        if let Some(prev) = guard.take() {
            stop_cast(prev).await;
        }

        // Subscribe BEFORE Page.startScreencast. The other order races: Chrome
        // can emit the first frame before the listener exists, and that frame is
        // then never acked, which under trap 2 wedges the stream at zero frames
        // rather than merely dropping one.
        let mut frames = page
            .event_listener::<EventScreencastFrame>()
            .await
            .map_err(cdp)?;

        let tx = self.frames_tx.clone();
        let seq = Arc::clone(&self.seq);
        let id = tab_id.to_string();
        let ack_page = page.clone();
        let task = tokio::spawn(async move {
            while let Some(frame) = frames.next().await {
                // TRAP 2: ack first, unconditionally. Anything that can fail or
                // block has to happen after Chrome has been told to keep going.
                if let Err(e) = ack_page
                    .execute(ScreencastFrameAckParams::new(frame.session_id))
                    .await
                {
                    tracing::debug!("screencastFrameAck failed, stopping cast: {e}");
                    break;
                }

                let jpeg = match base64::engine::general_purpose::STANDARD
                    .decode(AsRef::<str>::as_ref(&frame.data))
                {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!("screencast frame was not valid base64: {e}");
                        continue;
                    }
                };
                let md = &frame.metadata;
                // Fall back to the viewport size only if the JPEG is
                // unparseable; that makes the scale ratio 1.0, which is wrong
                // but visible, rather than silently dropping the frame.
                let (width, height) = jpeg_dimensions(&jpeg).unwrap_or_else(|| {
                    tracing::warn!("could not read JPEG dimensions from screencast frame");
                    (md.device_width as u32, md.device_height as u32)
                });

                // A send error only means nobody is subscribed right now, which
                // is normal while no client is viewing the browser.
                let _ = tx.send(ScreencastFrame {
                    tab_id: id.clone(),
                    seq: seq.fetch_add(1, Ordering::Relaxed),
                    jpeg,
                    width,
                    height,
                    offset_top: md.offset_top,
                    page_scale: md.page_scale_factor,
                    device_width: md.device_width,
                    device_height: md.device_height,
                });
            }
        });

        let params = StartScreencastParams {
            format: Some(StartScreencastFormat::Jpeg),
            quality: Some(quality.into()),
            max_width: Some(max_width.into()),
            max_height: None,
            every_nth_frame: None,
        };
        if let Err(e) = page.execute(params).await {
            task.abort();
            return Err(cdp(e));
        }

        *guard = Some(ActiveCast {
            tab_id: tab_id.to_string(),
            page,
            task,
        });
        Ok(())
    }

    /// Stop the screencast for `tab_id`. Idempotent: stopping a tab that is not
    /// casting succeeds. Unknown tabs are still an error.
    pub async fn stop_screencast(&self, tab_id: &str) -> Result<(), BrowserError> {
        self.page(tab_id)?;
        self.stop_cast_if(tab_id).await;
        Ok(())
    }

    pub fn subscribe_frames(&self) -> broadcast::Receiver<ScreencastFrame> {
        self.frames_tx.subscribe()
    }

    pub fn subscribe_console(&self) -> broadcast::Receiver<ConsoleLine> {
        self.console_tx.subscribe()
    }

    /// Dispatch a mouse event. `kind` is one of `mousePressed`, `mouseReleased`,
    /// `mouseMoved`, `mouseWheel`; `button` one of `none`, `left`, `middle`,
    /// `right`, `back`, `forward`. `x`/`y` are **CSS viewport** coordinates —
    /// see the module docs on mapping them back from frame pixels.
    pub async fn send_mouse(
        &self,
        tab_id: &str,
        kind: &str,
        x: f64,
        y: f64,
        button: &str,
        click_count: i64,
    ) -> Result<(), BrowserError> {
        let page = self.page(tab_id)?;
        let r#type: DispatchMouseEventType = kind
            .parse()
            .map_err(|_| BrowserError::Cdp(format!("unknown mouse event kind '{kind}'")))?;
        let button: MouseButton = button
            .parse()
            .map_err(|_| BrowserError::Cdp(format!("unknown mouse button '{button}'")))?;

        // Chrome wants `buttons` (the held-button bitfield) as well as `button`;
        // some listeners read only the former.
        let held = match (&r#type, &button) {
            (DispatchMouseEventType::MouseReleased, _) | (_, MouseButton::None) => 0,
            (_, MouseButton::Left) => 1,
            (_, MouseButton::Right) => 2,
            (_, MouseButton::Middle) => 4,
            (_, MouseButton::Back) => 8,
            (_, MouseButton::Forward) => 16,
        };

        let mut params = DispatchMouseEventParams::new(r#type, x, y);
        params.button = Some(button);
        params.buttons = Some(held);
        params.click_count = Some(click_count);

        deadline(async { page.execute(params).await.map(|_| ()).map_err(cdp) }).await
    }

    /// Dispatch a key event. `kind` is one of `keyDown`, `keyUp`, `rawKeyDown`,
    /// `char`. `key` is a DOM key value (`a`, `Enter`, `ArrowLeft`); `text` is
    /// the character it produces, required for `keyDown` to actually insert
    /// text.
    pub async fn send_key(
        &self,
        tab_id: &str,
        kind: &str,
        key: &str,
        text: Option<String>,
    ) -> Result<(), BrowserError> {
        let page = self.page(tab_id)?;
        let r#type: DispatchKeyEventType = kind
            .parse()
            .map_err(|_| BrowserError::Cdp(format!("unknown key event kind '{kind}'")))?;

        let mut params = DispatchKeyEventParams::new(r#type);
        params.key = Some(key.to_string());
        // Without windowsVirtualKeyCode and code, Chrome treats named keys like
        // Enter and Backspace as inert: the event fires but nothing acts on it.
        if let Some(def) = USKEYBOARD_LAYOUT.iter().find(|d| d.key == key) {
            params.windows_virtual_key_code = Some(def.key_code);
            params.native_virtual_key_code = Some(def.key_code);
            params.code = Some(def.code.to_string());
        }
        let text = text.or_else(|| {
            // A lone printable character is its own text.
            let mut chars = key.chars();
            match (chars.next(), chars.next()) {
                (Some(_), None) => Some(key.to_string()),
                _ => None,
            }
        });
        if let Some(t) = text {
            params.unmodified_text = Some(t.clone());
            params.text = Some(t);
        }

        deadline(async { page.execute(params).await.map(|_| ()).map_err(cdp) }).await
    }

    /// Evaluate an expression in the page and return its result as JSON.
    ///
    /// `undefined` and values that cannot be serialised come back as `"null"`.
    /// A thrown exception is a [`BrowserError::Cdp`], not a silent null.
    pub async fn evaluate(&self, tab_id: &str, expression: &str) -> Result<String, BrowserError> {
        let page = self.page(tab_id)?;
        let mut params = EvaluateParams::new(expression.to_string());
        params.return_by_value = Some(true);
        params.await_promise = Some(true);

        deadline(async {
            let result = page.evaluate_expression(params).await.map_err(cdp)?;
            Ok(result
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()))
        })
        .await
    }

    /// Close every tab, quit Chrome, reap the child, then stop the handler.
    ///
    /// Idempotent. Trap 3: a service dropped without this leaves a live Chrome.
    /// The ordering matters — `Browser::close` is a CDP round-trip, so it needs
    /// the handler task still running.
    pub async fn shutdown(&self) {
        if let Some(cast) = self.screencast.lock().await.take() {
            stop_cast(cast).await;
        }
        let tabs: Vec<Tab> = self.tabs.lock().drain(..).collect();
        for tab in &tabs {
            for h in &tab.listeners {
                h.abort();
            }
        }
        *self.active.lock() = None;

        if let Some(mut browser) = self.browser.lock().await.take() {
            // Ask nicely, then insist. Either way `wait` is what actually reaps
            // the process; skipping it is how zombies accumulate.
            if tokio::time::timeout(OP_TIMEOUT, browser.close())
                .await
                .is_err()
            {
                tracing::warn!("Browser.close did not return; killing chrome");
                if let Some(Err(e)) = browser.kill().await {
                    tracing::warn!("could not kill chrome: {e}");
                }
            }
            if tokio::time::timeout(OP_TIMEOUT, browser.wait())
                .await
                .is_err()
            {
                tracing::warn!("chrome did not exit after close; killing");
                if let Some(Err(e)) = browser.kill().await {
                    tracing::warn!("could not kill chrome: {e}");
                }
                let _ = browser.wait().await;
            }
        }

        if let Some(h) = self.handler.lock().take() {
            h.abort();
        }
        // Best effort: a leftover profile directory is untidy, not broken.
        let _ = std::fs::remove_dir_all(&self.user_data_dir);
    }

    fn page(&self, tab_id: &str) -> Result<Page, BrowserError> {
        self.tabs
            .lock()
            .iter()
            .find(|t| t.id == tab_id)
            .map(|t| t.page.clone())
            .ok_or(BrowserError::NoSuchTab)
    }

    async fn stop_cast_if(&self, tab_id: &str) {
        let mut guard = self.screencast.lock().await;
        if guard.as_ref().is_some_and(|c| c.tab_id == tab_id) {
            if let Some(cast) = guard.take() {
                stop_cast(cast).await;
            }
        }
    }

    /// One task per event source, both feeding the same console broadcast:
    /// `console.*` calls and uncaught exceptions. A devtools console that shows
    /// the former but swallows the latter is worse than none.
    async fn spawn_console_listeners(
        &self,
        page: &Page,
        tab_id: &str,
    ) -> Result<Vec<JoinHandle<()>>, BrowserError> {
        let mut calls = page
            .event_listener::<EventConsoleApiCalled>()
            .await
            .map_err(cdp)?;
        let tx = self.console_tx.clone();
        let id = tab_id.to_string();
        let call_task = tokio::spawn(async move {
            while let Some(ev) = calls.next().await {
                let text = ev
                    .args
                    .iter()
                    .map(render_remote_object)
                    .collect::<Vec<_>>()
                    .join(" ");
                let _ = tx.send(ConsoleLine {
                    tab_id: id.clone(),
                    level: ev.r#type.as_ref().to_string(),
                    text,
                });
            }
        });

        let mut thrown = page
            .event_listener::<EventExceptionThrown>()
            .await
            .map_err(cdp)?;
        let tx = self.console_tx.clone();
        let id = tab_id.to_string();
        let throw_task = tokio::spawn(async move {
            while let Some(ev) = thrown.next().await {
                let d = &ev.exception_details;
                let text = d
                    .exception
                    .as_ref()
                    .map(render_remote_object)
                    .unwrap_or_else(|| d.text.clone());
                let _ = tx.send(ConsoleLine {
                    tab_id: id.clone(),
                    level: "error".to_string(),
                    text,
                });
            }
        });

        Ok(vec![call_task, throw_task])
    }
}

fn shut_down() -> BrowserError {
    BrowserError::Cdp("browser has been shut down".to_string())
}

async fn stop_cast(cast: ActiveCast) {
    cast.task.abort();
    // Best effort: the page may already be gone, which is exactly the case when
    // this is called from close_tab.
    let _ = tokio::time::timeout(
        OP_TIMEOUT,
        cast.page.execute(StopScreencastParams::default()),
    )
    .await;
}

/// URL and title, tolerantly. A tab whose title cannot be read is still a tab —
/// failing the whole listing over it would hide every other tab from the client.
async fn tab_info(page: &Page, id: String, active: bool) -> TabInfo {
    let url = page.url().await.ok().flatten().unwrap_or_default();
    let title = page.get_title().await.ok().flatten().unwrap_or_default();
    TabInfo {
        tab_id: id,
        url,
        title,
        active,
    }
}

/// Render a console argument the way devtools would: the value if it came back
/// by value, otherwise Chrome's own description.
fn render_remote_object(obj: &RemoteObject) -> String {
    match obj.value.as_ref() {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => obj
            .description
            .clone()
            .or_else(|| obj.unserializable_value.as_ref().map(|u| u.inner().clone()))
            .unwrap_or_else(|| obj.r#type.as_ref().to_string()),
    }
}

/// Pixel dimensions of a JPEG, read from its start-of-frame header.
///
/// The screencast metadata's `deviceWidth`/`deviceHeight` are the CSS viewport,
/// not the rendered image — Chrome scales to fit `maxWidth`/`maxHeight`. The
/// client needs both to map clicks, and the image's true size exists only in the
/// JPEG itself.
fn jpeg_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 4 || data[0] != 0xFF || data[1] != 0xD8 {
        return None;
    }
    let mut i = 2;
    while i + 1 < data.len() {
        // Segments are `FF <marker>`; runs of FF are padding.
        if data[i] != 0xFF {
            i += 1;
            continue;
        }
        let marker = data[i + 1];
        if marker == 0xFF {
            i += 1;
            continue;
        }
        i += 2;
        // Standalone markers carry no length payload.
        if marker == 0x01 || (0xD0..=0xD9).contains(&marker) {
            continue;
        }
        if i + 1 >= data.len() {
            return None;
        }
        let len = u16::from_be_bytes([data[i], data[i + 1]]) as usize;
        if len < 2 {
            return None;
        }
        // SOF0..SOF15 hold the frame header; C4/C8/CC are DHT/JPG/DAC and are
        // NOT start-of-frame despite sitting in the same marker range.
        let is_sof = (0xC0..=0xCF).contains(&marker) && !matches!(marker, 0xC4 | 0xC8 | 0xCC);
        if is_sof {
            // length(2) precision(1) height(2) width(2)
            if i + 7 > data.len() {
                return None;
            }
            let height = u16::from_be_bytes([data[i + 3], data[i + 4]]) as u32;
            let width = u16::from_be_bytes([data[i + 5], data[i + 6]]) as u32;
            return Some((width, height));
        }
        i += len;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::jpeg_dimensions;

    /// SOI, a decoy APP0 whose payload contains bytes that would look like a SOF
    /// if the length prefix were ignored, then a real SOF0 of 640x360.
    fn synthetic_jpeg() -> Vec<u8> {
        let mut v = vec![0xFF, 0xD8];
        // APP0, length 8, payload includes a fake `FF C0` to prove the parser
        // skips by length rather than scanning for marker bytes.
        v.extend_from_slice(&[0xFF, 0xE0, 0x00, 0x08, 0xFF, 0xC0, 0x00, 0x11, 0x08, 0x02]);
        // SOF0: length 17, precision 8, height 0x0168 = 360, width 0x0280 = 640.
        v.extend_from_slice(&[0xFF, 0xC0, 0x00, 0x11, 0x08, 0x01, 0x68, 0x02, 0x80]);
        v.extend_from_slice(&[0u8; 8]);
        v
    }

    #[test]
    fn reads_dimensions_from_the_start_of_frame_header() {
        assert_eq!(jpeg_dimensions(&synthetic_jpeg()), Some((640, 360)));
    }

    #[test]
    fn rejects_input_that_is_not_a_jpeg() {
        assert_eq!(jpeg_dimensions(b""), None);
        assert_eq!(jpeg_dimensions(b"not a jpeg at all"), None);
        // Valid SOI, no frame header: must report failure, not a garbage size.
        assert_eq!(jpeg_dimensions(&[0xFF, 0xD8, 0xFF, 0xD9]), None);
    }

    #[test]
    fn does_not_mistake_a_huffman_table_for_a_frame_header() {
        // FF C4 sits inside the SOF marker range but is DHT. Treating it as a
        // frame header yields plausible-looking nonsense dimensions, which is
        // the worst kind of wrong for coordinate mapping.
        let mut v = vec![0xFF, 0xD8];
        v.extend_from_slice(&[0xFF, 0xC4, 0x00, 0x07, 0x00, 0x99, 0x00, 0x99, 0x00]);
        v.extend_from_slice(&[0xFF, 0xC2, 0x00, 0x11, 0x08, 0x00, 0x64, 0x00, 0xC8]);
        assert_eq!(jpeg_dimensions(&v), Some((200, 100)));
    }
}
