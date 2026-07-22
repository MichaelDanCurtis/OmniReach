use crate::ring::Ring;
use parking_lot::Mutex;
use portable_pty::{native_pty_system, CommandBuilder, MasterPty, PtySize};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;

const BROADCAST_CAPACITY: usize = 256;
const READ_CHUNK: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct PtySpec {
    pub shell: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Error)]
pub enum PtyError {
    #[error("failed to open pty: {0}")]
    Open(String),
    // NOTE: the field is `cause`, not `source`. thiserror reserves a field
    // named `source` for the error-source chain and rejects a `String` there.
    #[error("failed to spawn '{shell}': {cause}")]
    Spawn { shell: String, cause: String },
    #[error("failed to write to pty: {0}")]
    Write(String),
    #[error("failed to resize pty: {0}")]
    Resize(String),
    #[error("failed to kill child: {0}")]
    Kill(String),
}

pub struct PtySession {
    id: Uuid,
    spec: PtySpec,
    ring: Mutex<Ring>,
    tx: broadcast::Sender<Arc<Vec<u8>>>,
    master: Mutex<Box<dyn MasterPty + Send>>,
    writer: Mutex<Box<dyn Write + Send>>,
    killer: Mutex<Box<dyn portable_pty::ChildKiller + Send + Sync>>,
    alive: AtomicBool,
    size: Mutex<(u16, u16)>,
}

impl PtySession {
    pub fn spawn(spec: &PtySpec, scrollback: usize) -> Result<Arc<Self>, PtyError> {
        let pty_system = native_pty_system();
        let size = PtySize {
            rows: spec.rows,
            cols: spec.cols,
            pixel_width: 0,
            pixel_height: 0,
        };
        let pair = pty_system
            .openpty(size)
            .map_err(|e| PtyError::Open(e.to_string()))?;

        let mut cmd = CommandBuilder::new(&spec.shell);
        for a in &spec.args {
            cmd.arg(a);
        }
        if let Some(cwd) = &spec.cwd {
            cmd.cwd(cwd);
        }

        let mut child = pair.slave.spawn_command(cmd).map_err(|e| PtyError::Spawn {
            shell: spec.shell.clone(),
            cause: e.to_string(),
        })?;
        // Dropping the slave here is important: otherwise the master read
        // never sees EOF when the child exits.
        drop(pair.slave);

        let killer = child.clone_killer();
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| PtyError::Open(e.to_string()))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|e| PtyError::Open(e.to_string()))?;

        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);

        let session = Arc::new(Self {
            id: Uuid::new_v4(),
            spec: spec.clone(),
            ring: Mutex::new(Ring::new(scrollback)),
            tx,
            master: Mutex::new(pair.master),
            writer: Mutex::new(writer),
            killer: Mutex::new(killer),
            alive: AtomicBool::new(true),
            size: Mutex::new((spec.cols, spec.rows)),
        });

        // Blocking read loop. portable-pty's reader has no async interface.
        {
            let session = Arc::clone(&session);
            let mut reader = reader;
            tokio::task::spawn_blocking(move || {
                let mut buf = vec![0u8; READ_CHUNK];
                loop {
                    match reader.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            // Bytes only. Never decode here — a multi-byte
                            // sequence split across reads would be corrupted
                            // irrecoverably in the ring.
                            let chunk = Arc::new(buf[..n].to_vec());
                            session.ring.lock().write(&chunk);
                            let _ = session.tx.send(chunk);
                        }
                        Err(_) => break,
                    }
                }
                session.alive.store(false, Ordering::SeqCst);
            });
        }

        // Reap the child so `is_alive` flips even if the reader lingers.
        {
            let session = Arc::clone(&session);
            tokio::task::spawn_blocking(move || {
                let _ = child.wait();
                session.alive.store(false, Ordering::SeqCst);
            });
        }

        Ok(session)
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn spec(&self) -> &PtySpec {
        &self.spec
    }

    pub fn size(&self) -> (u16, u16) {
        *self.size.lock()
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub fn head_seq(&self) -> u64 {
        self.ring.lock().head_seq()
    }

    pub fn read_since(&self, seq: u64) -> (u64, Vec<u8>) {
        self.ring.lock().read_since(seq)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Vec<u8>>> {
        self.tx.subscribe()
    }

    pub fn write_input(&self, bytes: &[u8]) -> Result<(), PtyError> {
        let mut w = self.writer.lock();
        w.write_all(bytes)
            .map_err(|e| PtyError::Write(e.to_string()))?;
        w.flush().map_err(|e| PtyError::Write(e.to_string()))
    }

    pub fn resize(&self, cols: u16, rows: u16) -> Result<(), PtyError> {
        self.master
            .lock()
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| PtyError::Resize(e.to_string()))?;
        *self.size.lock() = (cols, rows);
        Ok(())
    }

    pub fn kill(&self) -> Result<(), PtyError> {
        self.killer
            .lock()
            .kill()
            .map_err(|e| PtyError::Kill(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn echo_spec(text: &str) -> PtySpec {
        // Unix-only test shell; the Windows path is type-checked, not run.
        PtySpec {
            shell: "/bin/sh".into(),
            args: vec!["-c".into(), format!("printf '%s' '{text}'")],
            cwd: None,
            cols: 80,
            rows: 24,
        }
    }

    #[tokio::test]
    async fn captures_child_output_into_the_ring() {
        let s = PtySession::spawn(&echo_spec("hello-omnireach"), 64 * 1024).unwrap();
        // Give the blocking reader a moment to drain the child.
        for _ in 0..50 {
            if s.head_seq() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (start, bytes) = s.read_since(0);
        assert_eq!(start, 0);
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("hello-omnireach"), "got: {text:?}");
    }

    #[tokio::test]
    async fn subscribers_receive_live_output() {
        let spec = PtySpec {
            shell: "/bin/sh".into(),
            args: vec!["-c".into(), "sleep 0.2; printf 'late-output'".into()],
            cwd: None,
            cols: 80,
            rows: 24,
        };
        let s = PtySession::spawn(&spec, 64 * 1024).unwrap();
        let mut rx = s.subscribe();
        let chunk = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for output")
            .expect("broadcast closed");
        assert!(String::from_utf8_lossy(&chunk).contains("late-output"));
    }

    #[tokio::test]
    async fn reports_death_after_the_child_exits() {
        let s = PtySession::spawn(&echo_spec("bye"), 4096).unwrap();
        for _ in 0..100 {
            if !s.is_alive() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !s.is_alive(),
            "session should report dead after child exits"
        );
    }

    #[tokio::test]
    async fn accepts_input_and_echoes_it_back() {
        let spec = PtySpec {
            shell: "/bin/sh".into(),
            args: vec![],
            cwd: None,
            cols: 80,
            rows: 24,
        };
        let s = PtySession::spawn(&spec, 64 * 1024).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        s.write_input(b"printf 'from-stdin'\n").unwrap();

        let mut found = false;
        for _ in 0..100 {
            let (_, bytes) = s.read_since(0);
            if String::from_utf8_lossy(&bytes).contains("from-stdin") {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(found, "expected shell to process written input");

        // An interactive shell never exits on its own. `Runtime::drop` WAITS
        // for `spawn_blocking` tasks, and the read loop is parked in an
        // uncancellable `read()` syscall, so without killing the child the
        // test hangs forever at teardown. Killing closes the last slave fd,
        // the read returns, and the blocking task can finish.
        s.kill().unwrap();
        let mut dead = false;
        for _ in 0..100 {
            if !s.is_alive() {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(dead, "session should report dead after kill()");
    }

    #[tokio::test]
    async fn resize_does_not_error() {
        let spec = PtySpec {
            shell: "/bin/sh".into(),
            args: vec![],
            cwd: None,
            cols: 80,
            rows: 24,
        };
        let s = PtySession::spawn(&spec, 4096).unwrap();
        s.resize(120, 40).unwrap();
        assert_eq!(s.size(), (120, 40));

        // See the note in `accepts_input_and_echoes_it_back`: an interactive
        // shell must be killed or runtime teardown blocks on the read loop.
        s.kill().unwrap();
        let mut dead = false;
        for _ in 0..100 {
            if !s.is_alive() {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(dead, "session should report dead after kill()");
    }
}
