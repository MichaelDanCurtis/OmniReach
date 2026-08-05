// The whole file is meaningless without the module it tests; without this the
// `--no-default-features` build fails to compile a test for code it excluded.
#![cfg(feature = "remote-browser")]

//! Integration tests for the remote headless-Chrome session (M6/M7).
//!
//! These launch a REAL Chrome. Three consequences shape everything below.
//!
//! # 1. A test that leaks a Chrome poisons the machine
//!
//! This is the same class of trap as the PTY tests documented in `replay.rs`:
//! teardown that never happens turns a failed assertion into an accumulating
//! pile of live processes, and the next run inherits them. So no test calls
//! `shutdown` "at the end" — the body runs inside [`drive`], which catches a
//! panic, shuts the browser down, and only then re-raises. A failing assertion
//! and a successful teardown are not in competition.
//!
//! # 2. Launches are expensive, so there are exactly two
//!
//! One for tab/navigation/console/evaluate lifecycle, one for screencast and
//! click coordinate mapping. Run serially — several Chromes at once is flaky:
//!
//! ```text
//! cargo test -p omnireach-host --test browser -- --test-threads=1
//! ```
//!
//! # 3. No network, and a machine without Chrome must not fail
//!
//! Every page is a `data:` URL, so the suite is hermetic. If Chrome cannot be
//! found the tests print a skip notice and return. That gate is deliberately
//! narrow: it fires only when chromiumoxide's own executable detection finds
//! nothing. On a machine that HAS Chrome, a launch failure is a real failure and
//! panics — otherwise "skip on launch error" would quietly hide the regression
//! it exists to catch.

use futures_util::FutureExt;
use omnireach_host::browser::{BrowserError, BrowserService};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout, Instant};

/// The page the screencast test casts.
///
/// Deliberately NOT self-animating. Measured on headless Chrome 150: a page
/// running a CSS animation at ~120fps produces exactly **1** screencast frame
/// and then goes silent, because in headless the capture is driven by the
/// browser-side surface being committed, not by the renderer animating. Frames
/// flow when the host pokes the page. So the test below drives repaints over
/// CDP rather than relying on the page to repaint itself — do not "fix" this
/// back to a CSS animation, it will look like the ack loop is broken.
const CAST_PAGE: &str = "data:text/html,<title>omnireach-cast</title>\
<body style='margin:0'>cast</body>";

/// A button at a known CSS-pixel rectangle whose click handler mutates the DOM.
/// `margin:0` matters: the assertion is about coordinates, so the page must not
/// introduce an offset of its own.
const CLICKABLE: &str = "data:text/html,<title>unclicked</title><body style='margin:0'>\
<button id='b' style='position:absolute;left:100px;top:60px;width:200px;height:80px' \
onclick=\"document.title='clicked'\">go</button></body>";

/// Is there a Chrome on this machine at all?
///
/// Used to tell "no browser installed, skip" apart from "browser installed but
/// launching it broke", which must fail.
fn chrome_is_installed() -> bool {
    chromiumoxide::detection::default_executable(Default::default()).is_ok()
}

/// Launch, run `body`, then ALWAYS shut the browser down before any panic
/// escapes. See the module docs.
async fn drive<F, Fut>(body: F)
where
    F: FnOnce(Arc<BrowserService>) -> Fut,
    Fut: Future<Output = ()>,
{
    let downloads =
        std::env::temp_dir().join(format!("omnireach-test-downloads-{}", uuid::Uuid::new_v4()));

    let service = match BrowserService::launch(None, downloads.clone()).await {
        Ok(s) => s,
        Err(e) => {
            assert!(
                !chrome_is_installed(),
                "Chrome IS installed on this machine, so a launch failure is a real \
                 failure and must not be skipped: {e} (code {})",
                e.code()
            );
            eprintln!("SKIP: no Chrome on this machine, remote-browser tests not run ({e})");
            return;
        }
    };

    let outcome = AssertUnwindSafe(body(Arc::clone(&service)))
        .catch_unwind()
        .await;

    service.shutdown().await;
    let _ = std::fs::remove_dir_all(&downloads);

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn tabs_navigation_evaluation_and_console() {
    drive(|svc| async move {
        // --- new_tab + list_tabs ---------------------------------------
        let first = svc.new_tab(None).await.expect("open first tab");
        assert!(first.active, "a freshly opened tab is the active one");

        let tabs = svc.list_tabs().await.expect("list tabs");
        assert_eq!(tabs.len(), 1, "exactly the tab we opened: {tabs:?}");
        assert_eq!(tabs[0].tab_id, first.tab_id);

        // --- navigate, then read the URL and title back ------------------
        svc.navigate(&first.tab_id, CAST_PAGE)
            .await
            .expect("navigate");

        let tabs = svc.list_tabs().await.expect("list tabs after navigate");
        assert!(
            tabs[0].url.starts_with("data:text/html"),
            "url did not follow the navigation: {:?}",
            tabs[0].url
        );
        assert_eq!(
            tabs[0].title, "omnireach-cast",
            "title did not follow the navigation: {tabs:?}"
        );

        // --- evaluate returns a value from the page ----------------------
        let sum = svc
            .evaluate(&first.tab_id, "1 + 2")
            .await
            .expect("evaluate");
        assert_eq!(sum, "3", "evaluate must return the page's value as JSON");

        let title = svc
            .evaluate(&first.tab_id, "document.title")
            .await
            .expect("evaluate document.title");
        assert_eq!(
            title, "\"omnireach-cast\"",
            "a JS string comes back JSON-quoted"
        );

        // A thrown exception is an error, never a silent null — a console that
        // reports failures as `null` results is worse than one that errors.
        let boom = svc.evaluate(&first.tab_id, "throw new Error('nope')").await;
        assert!(
            matches!(boom, Err(BrowserError::Cdp(_))),
            "a page exception must surface as CDP_ERROR, got {boom:?}"
        );

        // --- console capture ---------------------------------------------
        // Subscribe BEFORE producing output: broadcast delivers nothing that
        // predates the receiver, so the other order is a startup race that
        // fails intermittently.
        let mut console = svc.subscribe_console();
        svc.evaluate(&first.tab_id, "console.warn('hello', 42); null")
            .await
            .expect("console.warn");

        let line = timeout(Duration::from_secs(10), console.recv())
            .await
            .expect("timed out waiting for a console line")
            .expect("console channel closed");
        assert_eq!(line.tab_id, first.tab_id);
        assert_eq!(line.level, "warning", "CDP's own level string is preserved");
        assert_eq!(
            line.text, "hello 42",
            "console arguments are rendered and joined"
        );

        // --- a second tab, activation, and close -------------------------
        let second = svc
            .new_tab(Some(CLICKABLE.to_string()))
            .await
            .expect("open second tab");
        assert_ne!(second.tab_id, first.tab_id);

        let tabs = svc.list_tabs().await.expect("list two tabs");
        assert_eq!(tabs.len(), 2, "{tabs:?}");
        assert!(
            tabs.iter().filter(|t| t.active).count() == 1
                && tabs.iter().any(|t| t.active && t.tab_id == second.tab_id),
            "the newest tab is the only active one: {tabs:?}"
        );

        svc.activate_tab(&first.tab_id).await.expect("activate");
        let tabs = svc.list_tabs().await.expect("list after activate");
        assert!(
            tabs.iter().any(|t| t.active && t.tab_id == first.tab_id),
            "activate_tab must move the active flag: {tabs:?}"
        );

        // --- close_tab ----------------------------------------------------
        svc.close_tab(&second.tab_id).await.expect("close tab");
        let tabs = svc.list_tabs().await.expect("list after close");
        assert_eq!(tabs.len(), 1, "closed tab is gone: {tabs:?}");
        assert_eq!(tabs[0].tab_id, first.tab_id);

        // Operating on a closed tab is NO_SUCH_TAB, not a hang and not a
        // success that quietly does nothing.
        let err = svc.close_tab(&second.tab_id).await.unwrap_err();
        assert_eq!(err.code(), "NO_SUCH_TAB");
        let err = svc
            .navigate(&second.tab_id, "about:blank")
            .await
            .unwrap_err();
        assert_eq!(err.code(), "NO_SUCH_TAB");
    })
    .await;
}

#[tokio::test]
async fn screencast_streams_frames_and_clicks_land_on_target() {
    drive(|svc| async move {
        let tab = svc
            .new_tab(Some(CAST_PAGE.to_string()))
            .await
            .expect("open tab");

        // --- screencast: a sustained stream proves the ack loop -----------
        // Chrome stops producing once a few frames go unacknowledged. Measured
        // on headless Chrome 150 with the host driving repaints for 6s:
        //
        //     with Page.screencastFrameAck    236 frames
        //     without it                        3 frames
        //
        // So the bar has to clear 3 to mean anything. Ten is decisively above
        // the stalled ceiling and an order of magnitude below the healthy rate.
        const MIN_FRAMES: usize = 10;

        let mut frames_rx = svc.subscribe_frames();
        // max_width well below the 800px viewport, so the JPEG really is a
        // scaled render and the coordinate mapping below is actually exercised.
        svc.start_screencast(&tab.tab_id, 60, 400)
            .await
            .expect("start screencast");

        let mut frames = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while frames.len() < MIN_FRAMES && Instant::now() < deadline {
            // Poke the page so there is something new to capture; see CAST_PAGE.
            svc.evaluate(
                &tab.tab_id,
                "document.body.style.background = \
                 '#' + Math.floor(Math.random() * 16777215).toString(16); null",
            )
            .await
            .expect("repaint the page");

            match timeout(Duration::from_millis(500), frames_rx.recv()).await {
                Ok(Ok(f)) => frames.push(f),
                // Lagging is backpressure, not a failure — it is exactly what
                // the sequence numbers exist to make visible.
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(e)) => panic!("frame channel closed: {e}"),
                Err(_) => continue,
            }
        }

        assert!(
            frames.len() >= MIN_FRAMES,
            "expected at least {MIN_FRAMES} screencast frames, got {}. A handful \
             then silence is the unacknowledged-frame trap: Chrome will not emit \
             another frame until the previous ones are acked.",
            frames.len()
        );

        let f = &frames[0];
        assert_eq!(f.tab_id, tab.tab_id);
        assert!(!f.jpeg.is_empty(), "frame carried no image");
        assert_eq!(
            &f.jpeg[..2],
            &[0xFF, 0xD8],
            "frame payload must be decoded JPEG bytes, not base64"
        );
        assert!(
            f.width > 0 && f.height > 0 && f.width <= 400,
            "implausible frame size {}x{} for max_width 400",
            f.width,
            f.height
        );
        assert!(
            f.device_width > 0.0 && f.device_height > 0.0,
            "frame is missing the CSS viewport size the client needs to map clicks"
        );
        assert!(
            f.device_width > f.width as f64,
            "max_width 400 did not scale the render below the {}px viewport, so the \
             coordinate mapping below would be an identity transform and would not \
             test anything (frame {}x{})",
            f.device_width,
            f.width,
            f.height
        );
        assert!(f.page_scale > 0.0, "page_scale must be a real scale factor");
        assert!(
            frames.windows(2).all(|w| w[1].seq > w[0].seq),
            "frame sequence numbers must strictly increase so a client can detect \
             drops: {:?}",
            frames.iter().map(|f| f.seq).collect::<Vec<_>>()
        );

        // Keep the metadata; the cast is no longer needed.
        let device_width = f.device_width;
        let device_height = f.device_height;
        let image_width = f.width as f64;
        let image_height = f.height as f64;
        let offset_top = f.offset_top;
        svc.stop_screencast(&tab.tab_id)
            .await
            .expect("stop screencast");

        // --- clicking through that mapping actually hits the button -------
        svc.navigate(&tab.tab_id, CLICKABLE)
            .await
            .expect("navigate to the clickable page");

        // Where the button really is, in CSS viewport pixels.
        let rect = svc
            .evaluate(
                &tab.tab_id,
                "JSON.stringify(document.getElementById('b').getBoundingClientRect().toJSON())",
            )
            .await
            .expect("read button rect");
        let rect: serde_json::Value =
            serde_json::from_str(rect.trim_matches('"').replace("\\\"", "\"").as_str())
                .expect("button rect json");
        let css_x = rect["x"].as_f64().unwrap() + rect["width"].as_f64().unwrap() / 2.0;
        let css_y = rect["y"].as_f64().unwrap() + rect["height"].as_f64().unwrap() / 2.0;

        // Walk the round trip a client makes: page pixels -> frame pixels (what
        // the user sees and clicks) -> back to CSS viewport pixels using only
        // what ScreencastFrame carries. If the frame did not carry
        // device_width/device_height the client would have to assume frame
        // pixels ARE viewport pixels, and this click would land at half the
        // intended position — off the button entirely.
        let frame_x = css_x * (image_width / device_width);
        let frame_y = css_y * (image_height / device_height);
        let click_x = frame_x * (device_width / image_width);
        let click_y = frame_y * (device_height / image_height) + offset_top;

        assert!(
            (click_x - css_x).abs() < 1.0 && (click_y - css_y).abs() < 1.0,
            "the documented mapping is not its own inverse: ({click_x}, {click_y}) \
             from ({css_x}, {css_y})"
        );

        svc.send_mouse(&tab.tab_id, "mousePressed", click_x, click_y, "left", 1)
            .await
            .expect("mousePressed");
        svc.send_mouse(&tab.tab_id, "mouseReleased", click_x, click_y, "left", 1)
            .await
            .expect("mouseReleased");

        // The handler is synchronous, but the dispatch is not; give it a beat.
        let mut title = String::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            title = svc
                .evaluate(&tab.tab_id, "document.title")
                .await
                .expect("read title");
            if title == "\"clicked\"" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            title, "\"clicked\"",
            "the click did not reach the button — this is the coordinate mapping \
             landing off target, the failure the frame metadata exists to prevent"
        );

        // A click that should MISS must miss, otherwise the test above would
        // pass for a mapping that just hits everything.
        svc.navigate(&tab.tab_id, CLICKABLE)
            .await
            .expect("re-navigate");
        svc.send_mouse(
            &tab.tab_id,
            "mousePressed",
            click_x,
            click_y + 300.0,
            "left",
            1,
        )
        .await
        .expect("mousePressed off-target");
        svc.send_mouse(
            &tab.tab_id,
            "mouseReleased",
            click_x,
            click_y + 300.0,
            "left",
            1,
        )
        .await
        .expect("mouseReleased off-target");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let title = svc
            .evaluate(&tab.tab_id, "document.title")
            .await
            .expect("read title");
        assert_eq!(
            title, "\"unclicked\"",
            "a click 300px below the button registered anyway, so the on-target \
             assertion proves nothing"
        );

        // --- keyboard input reaches the page ------------------------------
        svc.evaluate(
            &tab.tab_id,
            "document.body.insertAdjacentHTML('beforeend', \
             '<input id=i autofocus>'); document.getElementById('i').focus(); null",
        )
        .await
        .expect("add an input");
        for (kind, key) in [("keyDown", "a"), ("keyUp", "a")] {
            svc.send_key(&tab.tab_id, kind, key, None)
                .await
                .expect("send key");
        }
        let typed = svc
            .evaluate(&tab.tab_id, "document.getElementById('i').value")
            .await
            .expect("read input value");
        assert_eq!(typed, "\"a\"", "a keyDown with text must insert the text");

        // Unknown event kinds are rejected rather than silently dropped.
        let err = svc
            .send_mouse(&tab.tab_id, "teleport", 1.0, 1.0, "left", 1)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "CDP_ERROR");
    })
    .await;
}
