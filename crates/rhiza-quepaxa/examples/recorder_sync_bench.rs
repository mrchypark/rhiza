use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rhiza_quepaxa::{
    AcceptedValue, EntryType, LogHash, Membership, Proposal, ProposalPriority, RecordRequest,
    RecorderFileStore, StoredCommand,
};
use serde::Serialize;

const CLUSTER_ID: &str = "recorder-sync-bench";
const RECORDER_ID: &str = "n1";
const EPOCH: u64 = 1;
const CONFIG_ID: u64 = 1;
const DEFAULT_OPERATIONS: usize = 500;
const DEFAULT_WARMUP: usize = 100;
const DEFAULT_PAYLOAD_BYTES: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 4 * 1024;
const WAL_HARD_FRAME_LIMIT: usize = 1_024;
const WAL_SOFT_BYTE_LIMIT: u64 = 16 * 1024 * 1024;
// The production WAL checkpoints after 1,024 frames. Keeping every invocation below that
// boundary isolates the steady append + durability barrier that the default benchmark is for.
const MAX_TOTAL_OPERATIONS: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum CommandMode {
    Inline,
    PreStored,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    operations: usize,
    warmup: usize,
    payload_bytes: usize,
    label: String,
    root: Option<PathBuf>,
    keep: bool,
    command_mode: CommandMode,
    checkpoint_diagnostic: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            operations: DEFAULT_OPERATIONS,
            warmup: DEFAULT_WARMUP,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            label: "native".into(),
            root: None,
            keep: false,
            command_mode: CommandMode::Inline,
            checkpoint_diagnostic: false,
        }
    }
}

impl Config {
    fn parse_from(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut config = Self::default();
        let mut args = args.into_iter();
        let mut operations_explicit = false;
        let mut warmup_explicit = false;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--operations" => {
                    config.operations = parse_usize("--operations", args.next())?;
                    operations_explicit = true;
                }
                "--warmup" => {
                    config.warmup = parse_usize("--warmup", args.next())?;
                    warmup_explicit = true;
                }
                "--payload-bytes" => {
                    config.payload_bytes = parse_usize("--payload-bytes", args.next())?;
                }
                "--label" => {
                    config.label = args
                        .next()
                        .ok_or_else(|| "--label requires a value".to_owned())?;
                }
                "--root" => {
                    config.root = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--root requires a path".to_owned())?,
                    ));
                }
                "--keep" => config.keep = true,
                "--command-mode" => {
                    config.command_mode = match args.next().as_deref() {
                        Some("inline") => CommandMode::Inline,
                        Some("pre-stored") => CommandMode::PreStored,
                        Some(value) => {
                            return Err(format!(
                                "invalid --command-mode {value:?}; expected inline or pre-stored"
                            ));
                        }
                        None => return Err("--command-mode requires a value".into()),
                    };
                }
                "--checkpoint-diagnostic" => config.checkpoint_diagnostic = true,
                "--help" | "-h" => return Err(usage()),
                other => return Err(format!("unknown argument {other:?}\n{}", usage())),
            }
        }
        if config.checkpoint_diagnostic {
            if warmup_explicit && config.warmup != 0 {
                return Err("--checkpoint-diagnostic requires --warmup 0".into());
            }
            if operations_explicit && config.operations != WAL_HARD_FRAME_LIMIT + 1 {
                return Err(format!(
                    "--checkpoint-diagnostic requires --operations {}",
                    WAL_HARD_FRAME_LIMIT + 1
                ));
            }
            config.warmup = 0;
            config.operations = WAL_HARD_FRAME_LIMIT + 1;
        }
        if config.operations == 0 {
            return Err("--operations must be greater than zero".into());
        }
        if config.payload_bytes == 0 {
            return Err("--payload-bytes must be greater than zero".into());
        }
        if config.payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(format!(
                "--payload-bytes must not exceed {MAX_PAYLOAD_BYTES}; this keeps the run below the WAL byte checkpoint boundary"
            ));
        }
        let total = config
            .warmup
            .checked_add(config.operations)
            .ok_or_else(|| "warmup + operations overflowed".to_owned())?;
        if !config.checkpoint_diagnostic && total > MAX_TOTAL_OPERATIONS {
            return Err(format!(
                "warmup + operations must not exceed {MAX_TOTAL_OPERATIONS}; this keeps the run before the WAL checkpoint boundary"
            ));
        }
        Ok(config)
    }
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<usize>()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

fn usage() -> String {
    "usage: recorder_sync_bench [--operations N] [--warmup N] [--payload-bytes N] [--label NAME] [--root PATH] [--keep] [--command-mode inline|pre-stored] [--checkpoint-diagnostic]".into()
}

#[derive(Debug, Serialize)]
struct Platform {
    os: &'static str,
    arch: &'static str,
    family: &'static str,
    ld_preload: bool,
}

#[derive(Debug, Serialize)]
struct LatencyNs {
    p50: Option<u64>,
    p95: Option<u64>,
    p99: Option<u64>,
    max: Option<u64>,
}

#[derive(Debug, Serialize)]
struct OperationObservation {
    operation: usize,
    call_elapsed_ns: u64,
    completed: bool,
    checkpoint_observed: bool,
}

#[derive(Debug, Serialize)]
struct CheckpointEvent {
    operation: usize,
    generation: u64,
}

#[derive(Debug, Serialize)]
struct WalObservation {
    bytes: u64,
    frames: usize,
    generations: Vec<u64>,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    checkpoints_observed: usize,
    checkpoint_avoided_observed: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    benchmark: &'static str,
    sync_variant: String,
    command_mode: CommandMode,
    checkpoint_diagnostic: bool,
    operations: usize,
    warmup: usize,
    payload_bytes: usize,
    completed: usize,
    errors: usize,
    elapsed_ns: u64,
    ops_per_second: f64,
    latency_scope: &'static str,
    latency_ns: LatencyNs,
    checkpoint_boundary_operations: Vec<OperationObservation>,
    checkpoint_events: Vec<CheckpointEvent>,
    wal: WalObservation,
    platform: Platform,
}

fn main() {
    let config = match Config::parse_from(env::args().skip(1)) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            process::exit(2);
        }
    };
    match run(config) {
        Ok((report, failed)) => {
            println!(
                "{}",
                serde_json::to_string(&report).expect("benchmark report must serialize")
            );
            if failed {
                process::exit(1);
            }
        }
        Err(message) => {
            eprintln!("recorder sync benchmark failed: {message}");
            process::exit(1);
        }
    }
}

fn run(config: Config) -> Result<(Report, bool), String> {
    let generated_root = config.root.is_none();
    let root = config.root.clone().unwrap_or_else(unique_root);
    if root.exists() {
        return Err(format!("benchmark root already exists: {}", root.display()));
    }

    let result = run_at_root(&config, &root);
    if generated_root && !config.keep {
        let _ = fs::remove_dir_all(&root);
    }
    result
}

fn run_at_root(config: &Config, root: &Path) -> Result<(Report, bool), String> {
    let membership = Membership::new(["n1", "n2", "n3"]).map_err(|error| error.to_string())?;
    let config_digest = membership.digest();
    let store = RecorderFileStore::new_with_membership(
        root,
        RECORDER_ID,
        CLUSTER_ID,
        EPOCH,
        CONFIG_ID,
        membership,
    )
    .map_err(|error| error.to_string())?;
    let payload = vec![0x5a; config.payload_bytes];
    let command = StoredCommand::new(EntryType::Command, payload);
    if config.command_mode == CommandMode::PreStored {
        store
            .store_command(command.hash(), command.clone())
            .map_err(|error| format!("pre-store setup failed: {error}"))?;
    }
    let total = config.warmup + config.operations;
    let mut requests: Vec<_> = (1..=total)
        .map(|slot| record_request(config_digest, slot as u64, &command, config.command_mode))
        .collect();
    let measured_requests = requests.split_off(config.warmup);

    for (index, request) in requests.into_iter().enumerate() {
        store
            .record_proposal(request)
            .map_err(|error| format!("warmup operation {index} failed: {error}"))?;
    }

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(config.operations);
    let mut boundary_operations = Vec::with_capacity(3);
    let mut errors = 0usize;
    for (index, request) in measured_requests.into_iter().enumerate() {
        let operation = config.warmup + index + 1;
        let operation_started = Instant::now();
        let completed = store.record_proposal(request).is_ok();
        let call_elapsed_ns = duration_ns(operation_started.elapsed().as_nanos());
        if completed {
            latencies.push(call_elapsed_ns);
        } else {
            errors += 1;
        }
        if (WAL_HARD_FRAME_LIMIT - 1..=WAL_HARD_FRAME_LIMIT + 1).contains(&operation) {
            boundary_operations.push(OperationObservation {
                operation,
                call_elapsed_ns,
                completed,
                checkpoint_observed: false,
            });
        }
    }
    let elapsed_ns = duration_ns(started.elapsed().as_nanos());
    latencies.sort_unstable();
    let completed = config.operations - errors;
    let ops_per_second = if elapsed_ns == 0 {
        0.0
    } else {
        completed as f64 * 1_000_000_000.0 / elapsed_ns as f64
    };
    let wal = observe_wal(&root.join("recorder.wal"), total)?;
    let checkpoint_events = if config.checkpoint_diagnostic && wal.checkpoints_observed == 1 {
        wal.generations
            .last()
            .copied()
            .map(|generation| CheckpointEvent {
                operation: WAL_HARD_FRAME_LIMIT + 1,
                generation,
            })
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    for observation in &mut boundary_operations {
        observation.checkpoint_observed = checkpoint_events
            .iter()
            .any(|event| event.operation == observation.operation);
    }
    let report = Report {
        benchmark: "recorder_wal_record",
        sync_variant: config.label.clone(),
        command_mode: config.command_mode,
        checkpoint_diagnostic: config.checkpoint_diagnostic,
        operations: config.operations,
        warmup: config.warmup,
        payload_bytes: config.payload_bytes,
        completed,
        errors,
        elapsed_ns,
        ops_per_second,
        latency_scope: "successful_calls_only",
        latency_ns: LatencyNs {
            p50: percentile(&latencies, 50),
            p95: percentile(&latencies, 95),
            p99: percentile(&latencies, 99),
            max: latencies.last().copied(),
        },
        checkpoint_boundary_operations: boundary_operations,
        checkpoint_events,
        wal,
        platform: Platform {
            os: env::consts::OS,
            arch: env::consts::ARCH,
            family: env::consts::FAMILY,
            ld_preload: env::var_os("LD_PRELOAD").is_some(),
        },
    };
    Ok((report, errors != 0))
}

fn record_request(
    config_digest: LogHash,
    slot: u64,
    command: &StoredCommand,
    command_mode: CommandMode,
) -> RecordRequest {
    let value =
        AcceptedValue::from_command(CLUSTER_ID, slot, EPOCH, CONFIG_ID, LogHash::ZERO, command);
    RecordRequest {
        cluster_id: CLUSTER_ID.into(),
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest,
        slot,
        step: 4,
        proposal: Proposal::new(ProposalPriority::MAX, "benchmark-proposer", slot, value),
        command: (command_mode == CommandMode::Inline).then(|| command.clone()),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (sorted.len() * percentile).div_ceil(100);
    Some(sorted[rank.saturating_sub(1).min(sorted.len() - 1)])
}

fn observe_wal(path: &Path, attempted_frames: usize) -> Result<WalObservation, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    let mut offset = 0usize;
    let mut frames = 0usize;
    let mut generations = Vec::new();
    let mut first_sequence = None;
    let mut last_sequence = None;
    while offset < bytes.len() {
        let prefix = bytes
            .get(offset..offset.saturating_add(30))
            .ok_or_else(|| "truncated WAL frame header after benchmark".to_owned())?;
        if prefix.get(..4) != Some(b"QWAL") || prefix.get(4..6) != Some(&1u16.to_be_bytes()) {
            return Err("unexpected WAL frame identity after benchmark".into());
        }
        let frame_len = usize::try_from(u64::from_be_bytes(
            prefix[6..14]
                .try_into()
                .map_err(|_| "invalid WAL frame length".to_owned())?,
        ))
        .map_err(|_| "WAL frame length overflow".to_owned())?;
        if frame_len < 30
            || offset
                .checked_add(frame_len)
                .is_none_or(|end| end > bytes.len())
        {
            return Err("invalid WAL frame extent after benchmark".into());
        }
        let generation = u64::from_be_bytes(
            prefix[14..22]
                .try_into()
                .map_err(|_| "invalid WAL generation".to_owned())?,
        );
        let sequence = u64::from_be_bytes(
            prefix[22..30]
                .try_into()
                .map_err(|_| "invalid WAL sequence".to_owned())?,
        );
        if generations.last() != Some(&generation) {
            generations.push(generation);
        }
        first_sequence.get_or_insert(sequence);
        last_sequence = Some(sequence);
        frames += 1;
        offset += frame_len;
    }
    let byte_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let checkpoint_avoided_observed = attempted_frames < WAL_HARD_FRAME_LIMIT
        && byte_count < WAL_SOFT_BYTE_LIMIT
        && frames == attempted_frames
        && generations == [1]
        && first_sequence == Some(1)
        && last_sequence == Some(attempted_frames as u64);
    let checkpoints_observed = generations
        .last()
        .copied()
        .unwrap_or(1)
        .saturating_sub(1)
        .try_into()
        .unwrap_or(usize::MAX);
    Ok(WalObservation {
        bytes: byte_count,
        frames,
        generations,
        first_sequence,
        last_sequence,
        checkpoints_observed,
        checkpoint_avoided_observed,
    })
}

fn duration_ns(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn unique_root() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    env::temp_dir().join(format!(
        "rhiza-recorder-sync-bench-{}-{timestamp}",
        process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_explicit_benchmark_shape() {
        let config = Config::parse_from(
            [
                "--operations",
                "700",
                "--warmup",
                "200",
                "--payload-bytes",
                "256",
                "--label",
                "fsync-preload",
                "--keep",
            ]
            .into_iter()
            .map(String::from),
        )
        .unwrap();

        assert_eq!(config.operations, 700);
        assert_eq!(config.warmup, 200);
        assert_eq!(config.payload_bytes, 256);
        assert_eq!(config.label, "fsync-preload");
        assert!(config.keep);
    }

    #[test]
    fn parser_enables_exact_checkpoint_boundary_diagnostic_defaults() {
        let config = Config::parse_from(
            ["--checkpoint-diagnostic", "--command-mode", "pre-stored"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();

        assert_eq!(config.operations, WAL_HARD_FRAME_LIMIT + 1);
        assert_eq!(config.warmup, 0);
        assert_eq!(config.command_mode, CommandMode::PreStored);
        assert!(config.checkpoint_diagnostic);
    }

    #[test]
    fn parser_rejects_checkpoint_diagnostic_with_hidden_warmup() {
        let error = Config::parse_from(
            ["--checkpoint-diagnostic", "--warmup", "1"]
                .into_iter()
                .map(String::from),
        )
        .unwrap_err();

        assert!(error.contains("requires --warmup 0"));
    }

    #[test]
    fn command_mode_controls_whether_record_contains_inline_command() {
        let command = StoredCommand::new(EntryType::Command, b"payload".to_vec());

        let inline = record_request(LogHash::ZERO, 1, &command, CommandMode::Inline);
        let pre_stored = record_request(LogHash::ZERO, 1, &command, CommandMode::PreStored);

        assert_eq!(inline.command, Some(command));
        assert_eq!(pre_stored.command, None);
    }

    #[test]
    fn parser_rejects_runs_that_cross_the_checkpoint_boundary() {
        let error = Config::parse_from(
            ["--operations", "901", "--warmup", "100"]
                .into_iter()
                .map(String::from),
        )
        .unwrap_err();

        assert!(error.contains("WAL checkpoint boundary"));
    }

    #[test]
    fn report_serializes_as_json() {
        let report = Report {
            benchmark: "recorder_wal_record",
            sync_variant: "native".into(),
            command_mode: CommandMode::Inline,
            checkpoint_diagnostic: false,
            operations: 2,
            warmup: 1,
            payload_bytes: 128,
            completed: 2,
            errors: 0,
            elapsed_ns: 100,
            ops_per_second: 20_000_000.0,
            latency_scope: "successful_calls_only",
            latency_ns: LatencyNs {
                p50: Some(40),
                p95: Some(60),
                p99: Some(60),
                max: Some(60),
            },
            checkpoint_boundary_operations: Vec::new(),
            checkpoint_events: Vec::new(),
            wal: WalObservation {
                bytes: 200,
                frames: 3,
                generations: vec![1],
                first_sequence: Some(1),
                last_sequence: Some(3),
                checkpoints_observed: 0,
                checkpoint_avoided_observed: true,
            },
            platform: Platform {
                os: "linux",
                arch: "x86_64",
                family: "unix",
                ld_preload: false,
            },
        };

        let value = serde_json::to_value(report).unwrap();
        assert_eq!(value["operations"], 2);
        assert_eq!(value["command_mode"], "inline");
        assert_eq!(value["latency_ns"]["p99"], 60);
        assert_eq!(value["latency_ns"]["max"], 60);
        assert_eq!(value["latency_scope"], "successful_calls_only");
        assert_eq!(value["wal"]["checkpoint_avoided_observed"], true);
        assert_eq!(value["platform"]["os"], "linux");
    }

    #[test]
    fn empty_success_set_has_no_latency_percentiles() {
        assert_eq!(percentile(&[], 99), None);
    }

    #[test]
    fn wal_observation_reports_checkpoint_generation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("recorder.wal");
        let mut frame = Vec::from(b"QWAL".as_slice());
        frame.extend_from_slice(&1u16.to_be_bytes());
        frame.extend_from_slice(&30u64.to_be_bytes());
        frame.extend_from_slice(&2u64.to_be_bytes());
        frame.extend_from_slice(&1025u64.to_be_bytes());
        fs::write(&path, frame).unwrap();

        let observation = observe_wal(&path, WAL_HARD_FRAME_LIMIT + 1).unwrap();

        assert_eq!(observation.frames, 1);
        assert_eq!(observation.generations, vec![2]);
        assert_eq!(observation.checkpoints_observed, 1);
        assert!(!observation.checkpoint_avoided_observed);
    }
}
