use crate::pty::{PtyError, PtySession, PtySpec};
use omnireach_protocol::control::SessionInfo;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// How long a finished session's output stays readable after the child exits.
///
/// Dead sessions are deliberately retained: the product case is starting a long
/// agent run, closing the laptop, and reading what happened an hour later. A
/// session that vanished the moment its command finished would lose exactly the
/// output the user came back for.
pub const DEFAULT_DEAD_RETENTION: Duration = Duration::from_secs(60 * 60);

struct Entry {
    session: Arc<PtySession>,
    /// First time this session was *observed* dead. `None` while alive.
    /// Observed rather than actual, because nothing watches the child
    /// continuously; that only shifts eviction later, never earlier.
    died_at: Option<Instant>,
}

pub struct SessionRegistry {
    sessions: RwLock<HashMap<Uuid, Entry>>,
    scrollback: usize,
}

impl SessionRegistry {
    pub fn new(scrollback: usize) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            scrollback,
        }
    }

    pub fn create(&self, spec: PtySpec) -> Result<Arc<PtySession>, PtyError> {
        let session = PtySession::spawn(&spec, self.scrollback)?;
        self.sessions.write().insert(
            session.id(),
            Entry {
                session: Arc::clone(&session),
                died_at: None,
            },
        );
        Ok(session)
    }

    pub fn get(&self, id: Uuid) -> Option<Arc<PtySession>> {
        self.sessions
            .read()
            .get(&id)
            .map(|e| Arc::clone(&e.session))
    }

    pub fn list(&self) -> Vec<SessionInfo> {
        self.sessions
            .read()
            .values()
            .map(|e| {
                let s = &e.session;
                let (cols, rows) = s.size();
                SessionInfo {
                    session_id: s.id(),
                    shell: s.spec().shell.clone(),
                    cwd: s.spec().cwd.clone().unwrap_or_default(),
                    cols,
                    rows,
                    alive: s.is_alive(),
                    head_seq: s.head_seq(),
                }
            })
            .collect()
    }

    pub fn kill(&self, id: Uuid) -> Result<(), PtyError> {
        match self.get(id) {
            Some(s) => s.kill(),
            None => Ok(()),
        }
    }

    /// Remove a session outright, regardless of retention. Used when a client
    /// explicitly discards a session rather than merely closing it.
    pub fn remove(&self, id: Uuid) -> Option<Arc<PtySession>> {
        self.sessions.write().remove(&id).map(|e| e.session)
    }

    /// Evict sessions that have been dead longer than `retention`.
    ///
    /// Stamps the death time on first observation, so retention is measured
    /// from when the child actually finished rather than from process start.
    /// Call periodically; it is cheap and holds the write lock only briefly.
    pub fn reap_dead(&self, retention: Duration) {
        let now = Instant::now();
        let mut map = self.sessions.write();

        for entry in map.values_mut() {
            if !entry.session.is_alive() && entry.died_at.is_none() {
                entry.died_at = Some(now);
            }
        }

        map.retain(|_, e| match e.died_at {
            Some(t) => now.duration_since(t) < retention,
            None => true,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sh(script: &str) -> PtySpec {
        PtySpec {
            shell: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            cwd: None,
            cols: 80,
            rows: 24,
        }
    }

    /// Wait for a session to report dead. Sessions MUST be dead before a test
    /// returns: `Runtime::drop` waits for `spawn_blocking` tasks and cannot
    /// cancel them, so a live PTY read loop hangs teardown forever.
    async fn await_dead(s: &Arc<PtySession>) {
        for _ in 0..200 {
            if !s.is_alive() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("session never died; test teardown would hang");
    }

    #[tokio::test]
    async fn create_then_get_returns_the_same_session() {
        let reg = SessionRegistry::new(4096);
        let s = reg.create(sh("exit 0")).unwrap();
        let got = reg.get(s.id()).expect("session should be retrievable");
        assert_eq!(got.id(), s.id());
        await_dead(&s).await;
    }

    #[tokio::test]
    async fn list_reports_every_session() {
        let reg = SessionRegistry::new(4096);
        let a = reg.create(sh("exit 0")).unwrap();
        let b = reg.create(sh("exit 0")).unwrap();
        let ids: Vec<_> = reg.list().into_iter().map(|i| i.session_id).collect();
        assert!(ids.contains(&a.id()));
        assert!(ids.contains(&b.id()));
        assert_eq!(ids.len(), 2);
        await_dead(&a).await;
        await_dead(&b).await;
    }

    #[tokio::test]
    async fn get_returns_none_for_unknown_id() {
        let reg = SessionRegistry::new(4096);
        assert!(reg.get(Uuid::new_v4()).is_none());
    }

    #[tokio::test]
    async fn killed_session_stops_reporting_alive() {
        let reg = SessionRegistry::new(4096);
        let s = reg.create(sh("sleep 30")).unwrap();
        reg.kill(s.id()).unwrap();
        await_dead(&s).await;
    }

    #[tokio::test]
    async fn session_survives_dropping_every_subscriber() {
        // The registry-level statement of the product thesis: sessions belong
        // to the host, not to any connection.
        let reg = SessionRegistry::new(64 * 1024);
        let s = reg.create(sh("sleep 0.1; printf 'still-here'")).unwrap();
        drop(s.subscribe()); // a client attached, then vanished

        for _ in 0..100 {
            let (_, b) = s.read_since(0);
            if String::from_utf8_lossy(&b).contains("still-here") {
                await_dead(&s).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("session output was lost when the subscriber went away");
    }

    #[tokio::test]
    async fn dead_sessions_are_retained_so_late_clients_can_read_them() {
        let reg = SessionRegistry::new(64 * 1024);
        let s = reg.create(sh("printf 'finished-work'")).unwrap();
        await_dead(&s).await;

        // A generous retention window must NOT evict it...
        reg.reap_dead(Duration::from_secs(3600));
        let got = reg.get(s.id()).expect("dead session evicted too early");

        // ...and its final output must still be readable.
        let (_, bytes) = got.read_since(0);
        assert!(
            String::from_utf8_lossy(&bytes).contains("finished-work"),
            "a client reconnecting after the task finished must still see its output"
        );
    }

    #[tokio::test]
    async fn dead_sessions_are_evicted_once_retention_elapses() {
        let reg = SessionRegistry::new(4096);
        let s = reg.create(sh("exit 0")).unwrap();
        await_dead(&s).await;

        // First sweep stamps the death time; nothing is evicted yet.
        reg.reap_dead(Duration::from_secs(3600));
        assert!(reg.get(s.id()).is_some());

        // Zero retention means anything already stamped dead is now expired.
        reg.reap_dead(Duration::ZERO);
        assert!(
            reg.get(s.id()).is_none(),
            "dead session should be evicted once retention elapses"
        );
    }

    #[tokio::test]
    async fn reaping_never_evicts_a_live_session() {
        let reg = SessionRegistry::new(4096);
        let s = reg.create(sh("sleep 30")).unwrap();

        reg.reap_dead(Duration::ZERO);
        assert!(
            reg.get(s.id()).is_some(),
            "a running session must survive even the most aggressive reap"
        );

        reg.kill(s.id()).unwrap();
        await_dead(&s).await;
    }

    #[tokio::test]
    async fn remove_discards_a_session_regardless_of_retention() {
        let reg = SessionRegistry::new(4096);
        let s = reg.create(sh("sleep 30")).unwrap();
        let removed = reg
            .remove(s.id())
            .expect("remove should return the session");
        assert_eq!(removed.id(), s.id());
        assert!(reg.get(s.id()).is_none());

        removed.kill().unwrap();
        await_dead(&removed).await;
    }
}
