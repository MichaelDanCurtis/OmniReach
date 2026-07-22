use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
pub struct ControlRequest {
    pub id: u64,
    pub op: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlResponse {
    pub id: u64,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

impl ControlResponse {
    pub fn ok(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            result: None,
            error: Some(ProtocolError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event")]
pub enum ControlEvent {
    #[serde(rename = "pty.exit")]
    PtyExit { session_id: Uuid, status: i32 },
    #[serde(rename = "pty.resync")]
    PtyResync { session_id: Uuid, from_seq: u64 },
}

#[derive(Debug, Clone, Deserialize)]
pub struct PtyCreateParams {
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PtyAttachParams {
    pub session_id: Uuid,
    #[serde(default)]
    pub since: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PtyResizeParams {
    pub handle: u32,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub session_id: Uuid,
    pub shell: String,
    pub cwd: String,
    pub cols: u16,
    pub rows: u16,
    pub alive: bool,
    pub head_seq: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_a_request() {
        let json = r#"{"id":42,"op":"pty.create","params":{"cols":120,"rows":40}}"#;
        let req: ControlRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, 42);
        assert_eq!(req.op, "pty.create");
        let p: PtyCreateParams = serde_json::from_value(req.params).unwrap();
        assert_eq!(p.cols, 120);
        assert_eq!(p.rows, 40);
        assert!(p.shell.is_none());
    }

    #[test]
    fn serializes_ok_response() {
        let r = ControlResponse::ok(42, serde_json::json!({"handle": 1}));
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["id"], 42);
        assert_eq!(v["ok"], true);
        assert_eq!(v["result"]["handle"], 1);
        assert!(v.get("error").is_none(), "ok response must omit error");
    }

    #[test]
    fn serializes_error_response() {
        let r = ControlResponse::err(7, "SHELL_NOT_FOUND", "pwsh.exe not on PATH");
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"]["code"], "SHELL_NOT_FOUND");
        assert_eq!(v["error"]["message"], "pwsh.exe not on PATH");
        assert!(v.get("result").is_none(), "error response must omit result");
    }

    #[test]
    fn serializes_events_with_event_tag() {
        let id = uuid::Uuid::nil();
        let v = serde_json::to_value(ControlEvent::PtyExit {
            session_id: id,
            status: 0,
        })
        .unwrap();
        assert_eq!(v["event"], "pty.exit");
        assert_eq!(v["status"], 0);

        let v = serde_json::to_value(ControlEvent::PtyResync {
            session_id: id,
            from_seq: 918_233,
        })
        .unwrap();
        assert_eq!(v["event"], "pty.resync");
        assert_eq!(v["from_seq"], 918_233);
    }
}
