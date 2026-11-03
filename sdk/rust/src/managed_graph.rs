//! Lossless MGO/1 original body, not an execution frame or admission permit.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const BODY_LIMIT: usize = 192 << 10;
const NIL_COUNT: u32 = u32::MAX;

#[derive(Clone, Debug, Serialize)]
pub struct ManagedGraphStreamRequest {
    pub stream: String,
    pub after_sequence: u64,
    pub limit: u32,
    pub wait_ms: u32,
    pub consistency: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManagedGraphStreamRecord {
    pub sequence: u64,
    pub kind: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManagedGraphStreamRead {
    pub records: Vec<ManagedGraphStreamRecord>,
    pub applied_slot: u64,
    pub consensus_tip: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManagedGraphOffsetRead {
    pub sequence: u64,
    pub found: bool,
    pub applied_slot: u64,
    pub consensus_tip: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ManagedGraphReceipt {
    pub slot: u64,
    pub status: String,
    pub applied: bool,
    pub error_code: Option<String>,
    pub retry_through_slot: Option<u64>,
}

impl ManagedGraphReceipt {
    pub fn require_committed(&self) -> Result<()> {
        if self.status == "committed" {
            return Ok(());
        }
        Err(Error {
            code: self
                .error_code
                .clone()
                .unwrap_or_else(|| self.status.clone()),
            message: "managed Graph mutation is not committed".into(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ManagedGraphValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float32Bits(u32),
    Float64Bits(u64),
    Number(String),
    Bytes(Vec<u8>),
    List(Option<Vec<ManagedGraphValue>>),
    Map(Option<BTreeMap<Vec<u8>, ManagedGraphValue>>),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ManagedGraphCommand {
    pub request_id: Vec<u8>,
    pub cypher: Vec<u8>,
    pub args: Option<BTreeMap<Vec<u8>, ManagedGraphValue>>,
    pub events: Option<Vec<ManagedGraphEvent>>,
    pub stream_offset: Option<ManagedGraphOffset>,
    pub stream_trim: Option<ManagedGraphTrim>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManagedGraphEvent {
    pub stream: Vec<u8>,
    pub kind: Vec<u8>,
    pub payload: ManagedGraphValue,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManagedGraphOffset {
    pub stream: Vec<u8>,
    pub consumer: Vec<u8>,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManagedGraphTrim {
    pub stream: Vec<u8>,
    pub through_sequence: u64,
}

fn invalid() -> Error {
    Error {
        code: "invalid_request".into(),
        message: "invalid managed Graph original".into(),
    }
}

struct Writer {
    bytes: Vec<u8>,
    nodes: usize,
}

impl Writer {
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > BODY_LIMIT - self.bytes.len() {
            return Err(invalid());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn byte(&mut self, value: u8) -> Result<()> {
        self.append(&[value])
    }
    fn count(&mut self, count: usize) -> Result<()> {
        if count > 65_536 {
            return Err(invalid());
        }
        self.append(&(count as u32).to_be_bytes())
    }
    fn raw(&mut self, value: &[u8]) -> Result<()> {
        let remaining = BODY_LIMIT - self.bytes.len();
        if remaining < 4 || value.len() > remaining - 4 {
            return Err(invalid());
        }
        self.append(&(value.len() as u32).to_be_bytes())?;
        self.append(value)
    }
    fn map(
        &mut self,
        items: Option<&BTreeMap<Vec<u8>, ManagedGraphValue>>,
        depth: usize,
    ) -> Result<()> {
        self.byte(9)?;
        let Some(items) = items else {
            return self.append(&NIL_COUNT.to_be_bytes());
        };
        self.count(items.len())?;
        for (key, item) in items {
            self.raw(key)?;
            self.value(item, depth + 1)?;
        }
        Ok(())
    }
    fn value(&mut self, value: &ManagedGraphValue, depth: usize) -> Result<()> {
        self.nodes += 1;
        if depth > 32 || self.nodes > 65_536 {
            return Err(invalid());
        }
        match value {
            ManagedGraphValue::Null => self.byte(0),
            ManagedGraphValue::Bool(false) => self.byte(1),
            ManagedGraphValue::Bool(true) => self.byte(2),
            ManagedGraphValue::Integer(v) => {
                self.byte(3)?;
                self.append(&v.to_be_bytes())
            }
            ManagedGraphValue::Float32Bits(bits) => {
                if !f32::from_bits(*bits).is_finite() {
                    return Err(invalid());
                }
                self.byte(4)?;
                self.append(&bits.to_be_bytes())
            }
            ManagedGraphValue::Float64Bits(bits) => {
                if !f64::from_bits(*bits).is_finite() {
                    return Err(invalid());
                }
                self.byte(5)?;
                self.append(&bits.to_be_bytes())
            }
            ManagedGraphValue::Number(v) => {
                let remaining = BODY_LIMIT - self.bytes.len();
                if remaining < 5 || v.len() > remaining - 5 || !valid_number(v) {
                    return Err(invalid());
                }
                self.byte(6)?;
                self.raw(v.as_bytes())
            }
            ManagedGraphValue::Bytes(v) => {
                self.byte(7)?;
                self.raw(v)
            }
            ManagedGraphValue::List(None) => {
                self.byte(8)?;
                self.append(&NIL_COUNT.to_be_bytes())
            }
            ManagedGraphValue::List(Some(items)) => {
                self.byte(8)?;
                self.count(items.len())?;
                for item in items {
                    self.value(item, depth + 1)?;
                }
                Ok(())
            }
            ManagedGraphValue::Map(items) => self.map(items.as_ref(), depth),
        }
    }
}

fn valid_number(value: &str) -> bool {
    // A Number is ASCII JSON syntax, not a serde Value conversion. Retain its
    // original lexeme after validating the same finite numeric domain as Go.
    if value.is_empty() || value.trim() != value {
        return false;
    }
    if serde_json::from_str::<serde_json::Number>(value).is_err() {
        return false;
    }
    value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok_and(f64::is_finite)
}

impl ManagedGraphCommand {
    pub fn encode_execution(&self) -> Result<Vec<u8>> {
        let body = self.encode_original()?;
        let mut frame = Vec::with_capacity(9 + body.len());
        frame.extend_from_slice(b"QGRX\x01");
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    pub fn encode_original(&self) -> Result<Vec<u8>> {
        let mut writer = Writer {
            bytes: b"MGO\x01".to_vec(),
            nodes: 1, // root Args map is the first value node, as in Go MGO/1
        };
        writer.raw(&self.request_id)?;
        writer.raw(&self.cypher)?;
        writer.map(self.args.as_ref(), 1)?;
        match &self.events {
            None => writer.append(&NIL_COUNT.to_be_bytes())?,
            Some(events) => {
                writer.count(events.len())?;
                for event in events {
                    writer.raw(&event.stream)?;
                    writer.raw(&event.kind)?;
                    writer.value(&event.payload, 1)?;
                }
            }
        }
        match &self.stream_offset {
            None => writer.byte(0)?,
            Some(offset) => {
                writer.byte(1)?;
                writer.raw(&offset.stream)?;
                writer.raw(&offset.consumer)?;
                writer.append(&offset.sequence.to_be_bytes())?;
            }
        }
        match &self.stream_trim {
            None => writer.byte(0)?,
            Some(trim) => {
                writer.byte(1)?;
                writer.raw(&trim.stream)?;
                writer.append(&trim.through_sequence.to_be_bytes())?;
            }
        }
        Ok(writer.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_frame_matches_go_shared_golden() {
        let command = ManagedGraphCommand {
            request_id: b"r".to_vec(),
            cypher: b"RETURN 1".to_vec(),
            ..Default::default()
        };
        let hex: String = command
            .encode_execution()
            .unwrap()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            hex,
            "5147525801000000204d474f0100000001720000000852455455524e203109ffffffffffffffff0000"
        );
    }

    #[test]
    fn graph_context_and_recovery_keep_raw_identity_and_false_receipt() {
        for id in [vec![255], vec![254], "\u{fffd}".as_bytes().to_vec()] {
            use base64::Engine as _;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&id);
            let context = crate::parse_mutation_context(&serde_json::json!({
                "kind":"graph", "request_id_b64":encoded,
                "slot":17, "retry_through_slot":1040
            }))
            .unwrap();
            assert_eq!(context.kind(), crate::MutationKind::Graph);
            assert_eq!(context.request_id(), id.as_slice());
            let wire: crate::ManagedRecoveryWire = serde_json::from_value(serde_json::json!({
                "kind":"graph", "request_id_b64":encoded,
                "graph":{"slot":17,"status":"committed","applied":false,"retry_through_slot":1040}
            }))
            .unwrap();
            match wire.result().unwrap() {
                crate::ManagedRecoveryResult::Graph {
                    request_id,
                    receipt,
                } => {
                    assert_eq!(request_id, id);
                    assert_eq!(receipt.slot, 17);
                    assert!(!receipt.applied);
                }
                _ => panic!("Graph result kind changed"),
            }
        }
    }

    #[test]
    fn empty_body_matches_mgo1_go_layout() {
        assert_eq!(
            ManagedGraphCommand::default().encode_original().unwrap(),
            [
                b"MGO\x01".as_slice(),
                &[0; 8],
                &[9, 255, 255, 255, 255],
                &[255; 4],
                &[0, 0]
            ]
            .concat()
        );
    }

    #[test]
    fn raw_identity_presence_and_numeric_bits_are_distinct() {
        let mut a = ManagedGraphCommand {
            request_id: vec![255],
            ..Default::default()
        };
        let ff = a.encode_original().unwrap();
        a.request_id = vec![254];
        assert_ne!(ff, a.encode_original().unwrap());
        a.request_id = "\u{fffd}".as_bytes().to_vec();
        assert_ne!(ff, a.encode_original().unwrap());
        let nil = a.encode_original().unwrap();
        a.args = Some(BTreeMap::new());
        assert_ne!(nil, a.encode_original().unwrap());
        a.args.as_mut().unwrap().insert(
            vec![255],
            ManagedGraphValue::Float64Bits((-0.0f64).to_bits()),
        );
        let negative_zero = a.encode_original().unwrap();
        a.args
            .as_mut()
            .unwrap()
            .insert(vec![255], ManagedGraphValue::Float64Bits(0));
        assert_ne!(negative_zero, a.encode_original().unwrap());
    }

    #[test]
    fn oversized_number_and_nonfinite_are_rejected() {
        for value in [
            ManagedGraphValue::Number("x".repeat(BODY_LIMIT + 1)),
            ManagedGraphValue::Float64Bits(f64::INFINITY.to_bits()),
        ] {
            let command = ManagedGraphCommand {
                args: Some(BTreeMap::from([(b"x".to_vec(), value)])),
                ..Default::default()
            };
            assert!(command.encode_original().is_err());
        }
    }

    #[test]
    fn raw_key_integer_body_matches_go_tag_and_endian_contract() {
        let command = ManagedGraphCommand {
            request_id: vec![255],
            args: Some(BTreeMap::from([(
                vec![254],
                ManagedGraphValue::Integer(-7),
            )])),
            ..Default::default()
        };
        // MGO/1 Go layout: raw FF ID, empty Cypher, map tag9/count1,
        // raw FE key, int tag3/two's-complement -7, nil Events, absent ops.
        let expected = [
            b"MGO\x01".as_slice(),
            &[0, 0, 0, 1, 255],
            &[0, 0, 0, 0],
            &[9, 0, 0, 0, 1],
            &[0, 0, 0, 1, 254],
            &[3, 255, 255, 255, 255, 255, 255, 255, 249],
            &[255, 255, 255, 255, 0, 0],
        ]
        .concat();
        assert_eq!(command.encode_original().unwrap(), expected);
    }

    #[test]
    fn nested_presence_and_number_lexeme_are_not_serde_normalized() {
        let encode = |value| {
            ManagedGraphCommand {
                args: Some(BTreeMap::from([(b"x".to_vec(), value)])),
                ..Default::default()
            }
            .encode_original()
            .unwrap()
        };
        assert_ne!(
            encode(ManagedGraphValue::Null),
            encode(ManagedGraphValue::List(None))
        );
        assert_ne!(
            encode(ManagedGraphValue::List(None)),
            encode(ManagedGraphValue::List(Some(vec![])))
        );
        assert_ne!(
            encode(ManagedGraphValue::Map(None)),
            encode(ManagedGraphValue::Map(Some(BTreeMap::new())))
        );
        assert_ne!(
            encode(ManagedGraphValue::Number("1e0".into())),
            encode(ManagedGraphValue::Number("1".into()))
        );
        assert_ne!(
            encode(ManagedGraphValue::Integer(1)),
            encode(ManagedGraphValue::Number("1".into()))
        );
        for invalid in [" 1", "1 ", "01", "null", "1e999"] {
            assert!(!valid_number(invalid));
        }
    }
}
