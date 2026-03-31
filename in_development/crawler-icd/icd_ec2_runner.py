"""
icd_ec2_runner.py — EC2 entry-point for the ICD-11 MMS full catalogue crawl
============================================================================
Designed to run on a t4g.nano / t3.nano EC2 instance (or Fargate task)
with an IAM role that grants s3:GetObject, s3:PutObject and
secretsmanager:GetSecretValue on the relevant resources.

Checkpoint strategy
-------------------
The WHO ICD-11 OAuth2 token expires after ~1 hour.  Every time icd_fetch
requests a new token (via get_access_token → on_token_refresh callback)
this runner uploads the current JSONL state file to a fixed S3 key.

Timeline of a full run (~10 h, ~17,000 category codes):
  T+0h  start   → download existing state (or fresh)
  T+1h  token 2 → checkpoint upload #1
  T+2h  token 3 → checkpoint upload #2
  ...
  T+10h done    → final upload + summary JSON

If the instance is terminated, loses network, or the API starts returning
4xx errors mid-run, the last checkpoint (at most ~1 h old) is safe in S3
and the next run resumes from there automatically.

SIGTERM / KeyboardInterrupt
---------------------------
A signal handler performs one final checkpoint upload before the process
exits, so even an EC2 spot interruption (2-minute warning via SIGTERM)
saves the last partial hour.

S3 layout
---------
  State JSONL  (same key, overwritten each checkpoint):
    s3://<S3_BUCKET>/<S3_FETCH_FOLDER>/icd11_mms/icd11_mms_codes.jsonl

  Per-run summary (timestamped, never overwritten):
    s3://<S3_BUCKET>/<S3_FETCH_FOLDER>/YYYY-MM-DD/icd_api_<ts>.json

Usage
-----
  # Run directly on EC2 (IAM role supplies AWS credentials):
  python icd_ec2_runner.py

  # Optional overrides via environment variables (all have defaults):
  ICD_ACTION=get_catalogue          # only action currently used on EC2
  ICD_CLASS_KINDS=category          # comma-separated, e.g. "category,block"
  ICD_RESET=false                   # set "true" to wipe S3 state and restart

Required environment variables:
  AWS_REGION_NAME   e.g. "eu-central-1"
  S3_BUCKET         e.g. "my-health-data-bucket"
  SECRET_NAME       e.g. "prod/App/icd"
  S3_FETCH_FOLDER   e.g. "raw/icd"
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

import icd_catalogue_fetch
from lambda_utils import get_secret, save_to_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
REGION      = os.environ["AWS_REGION_NAME"]
S3_BUCKET   = os.environ["S3_BUCKET"]
SECRET_NAME = os.environ["SECRET_NAME"]
S3_PREFIX   = os.environ["S3_FETCH_FOLDER"]

JSONL_S3_KEY = f"{S3_PREFIX}/icd11_mms/icd11_mms_codes.jsonl"
LOCAL_JSONL  = "/tmp/icd11_mms_codes.jsonl"
SOURCE_API   = "id.who.int/icd"

# ── S3 helpers ─────────────────────────────────────────────────────────────────

def _s3_client():
    return boto3.client("s3", region_name=REGION)


def download_state() -> bool:
    """
    Pull existing JSONL state from S3 to LOCAL_JSONL.
    Returns True if a previous state was found, False on a fresh start.
    """
    Path(LOCAL_JSONL).parent.mkdir(parents=True, exist_ok=True)
    try:
        _s3_client().download_file(S3_BUCKET, JSONL_S3_KEY, LOCAL_JSONL)
        size = os.path.getsize(LOCAL_JSONL)
        log.info("Resumed state from s3://%s/%s  (%d bytes)", S3_BUCKET, JSONL_S3_KEY, size)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            log.info("No existing state at s3://%s/%s — fresh crawl", S3_BUCKET, JSONL_S3_KEY)
            if Path(LOCAL_JSONL).exists():
                Path(LOCAL_JSONL).unlink()
            return False
        raise


def upload_state(reason: str = "checkpoint") -> str:
    """
    Push LOCAL_JSONL back to the fixed S3 key.
    Returns the S3 URI.  Safe to call from a signal handler.
    """
    if not Path(LOCAL_JSONL).exists():
        log.warning("upload_state(%s): %s does not exist, skipping", reason, LOCAL_JSONL)
        return ""
    size = os.path.getsize(LOCAL_JSONL)
    _s3_client().upload_file(
        LOCAL_JSONL,
        S3_BUCKET,
        JSONL_S3_KEY,
        ExtraArgs={"ContentType": "application/x-ndjson"},
    )
    uri = f"s3://{S3_BUCKET}/{JSONL_S3_KEY}"
    log.info("[%s] State uploaded → %s  (%d bytes)", reason, uri, size)
    return uri


# ── Checkpoint counter (for logging) ──────────────────────────────────────────

_checkpoint_lock   = threading.Lock()
_checkpoint_count  = 0


def _checkpoint(reason: str = "token-refresh") -> None:
    """
    Upload current state to S3.  Called on every token refresh and on exit.
    Thread-safe; swallows exceptions so the crawl is never interrupted.
    """
    global _checkpoint_count
    with _checkpoint_lock:
        _checkpoint_count += 1
        n = _checkpoint_count
    try:
        uri = upload_state(reason=f"{reason} #{n}")
        log.info("Checkpoint #%d complete → %s", n, uri)
    except Exception as exc:
        # Log but never raise — a failed checkpoint must not kill the crawl
        log.error("Checkpoint #%d FAILED (%s): %s", n, reason, exc)


# ── Signal handler (SIGTERM / SIGINT) ─────────────────────────────────────────

def _handle_exit(signum, frame):
    sig_name = signal.Signals(signum).name
    log.warning("Received %s — performing final checkpoint before exit…", sig_name)
    _checkpoint(reason=sig_name)
    log.info("Final checkpoint done. Exiting.")
    sys.exit(0)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    start_time = datetime.now(timezone.utc)

    # -- 1. Secrets (IAM role provides AWS creds; Secrets Manager holds ICD creds) --
    log.info("Fetching ICD API credentials from Secrets Manager…")
    client_id     = get_secret(SECRET_NAME, REGION, "ICD_API_CLIENT_ID")
    client_secret = get_secret(SECRET_NAME, REGION, "ICD_API_CLIENT_SECRET")

    # Inject into environment so icd_fetch module-level reads pick them up
    os.environ["ICD_API_CLIENT_ID"]     = client_id
    os.environ["ICD_API_CLIENT_SECRET"] = client_secret

    # Propagate to icd_fetch module globals (module was already imported)
    icd_catalogue_fetch.ICD_API_CLIENT_ID     = client_id
    icd_catalogue_fetch.ICD_API_CLIENT_SECRET = client_secret

    # -- 2. Parameters -----------------------------------------------------------
    class_kinds_raw = os.environ.get("ICD_CLASS_KINDS", "category")
    class_kinds     = set(k.strip() for k in class_kinds_raw.split(",") if k.strip())
    reset           = os.environ.get("ICD_RESET", "false").lower() == "true"

    log.info(
        "Config — bucket=%s  prefix=%s  class_kinds=%s  reset=%s",
        S3_BUCKET, S3_PREFIX, class_kinds, reset,
    )

    # -- 3. Optionally wipe existing state ---------------------------------------
    if reset:
        try:
            _s3_client().delete_object(Bucket=S3_BUCKET, Key=JSONL_S3_KEY)
            log.info("Reset: deleted s3://%s/%s", S3_BUCKET, JSONL_S3_KEY)
        except ClientError as exc:
            log.warning("Reset: could not delete state (may not exist): %s", exc)

    # -- 4. Download existing state ----------------------------------------------
    download_state()

    # -- 5. Register signal handlers BEFORE starting crawl ----------------------
    signal.signal(signal.SIGTERM, _handle_exit)
    signal.signal(signal.SIGINT,  _handle_exit)

    # -- 6. Wire checkpoint to token refresh ------------------------------------
    #
    # icd_fetch.on_token_refresh is called by get_access_token() every time a
    # real OAuth2 refresh happens (~hourly).  We set it here — after credentials
    # are loaded and state is downloaded — so the first token fetch (which fires
    # immediately at crawl start) does NOT trigger an unnecessary upload of an
    # empty or just-downloaded file.
    #
    # To skip the very first token acquisition (which is not a "refresh" in the
    # meaningful sense), we arm the hook only after the crawl has started.
    # We use a simple flag for this.

    _hook_armed = threading.Event()

    def _on_token_refresh() -> None:
        if _hook_armed.is_set():
            _checkpoint(reason="token-refresh")
        else:
            log.info("Initial token acquired — checkpoint hook will arm after first record.")

    icd_catalogue_fetch.on_token_refresh = _on_token_refresh

    # -- 7. Run crawl ------------------------------------------------------------
    log.info("Starting ICD-11 MMS crawl…")
    records_written = 0
    try:
        for record in icd_catalogue_fetch.iter_icd11_mms(
            class_kinds=class_kinds,
            out_path=LOCAL_JSONL,
        ):
            records_written += 1
            if records_written == 1:
                # Arm the hook once we know real records are flowing
                _hook_armed.set()
                log.info("First record received — checkpoint hook armed.")

    except Exception as exc:
        log.error("Crawl failed after %d records: %s", records_written, exc)
        log.info("Performing emergency checkpoint…")
        _checkpoint(reason="crawl-error")
        raise

    # -- 8. Final upload ---------------------------------------------------------
    log.info("Crawl complete. %d records written. Performing final checkpoint…", records_written)
    final_uri = upload_state(reason="final")

    # -- 9. Write timestamped summary to S3 --------------------------------------
    end_time  = datetime.now(timezone.utc)
    elapsed_s = (end_time - start_time).total_seconds()

    summary = {
        "fetched_at":           start_time.isoformat(),
        "completed_at":         end_time.isoformat(),
        "elapsed_seconds":      round(elapsed_s),
        "source_api":           SOURCE_API,
        "fetch_params": {
            "class_kinds":  list(class_kinds),
            "reset":        reset,
            "jsonl_state":  final_uri,
        },
        "data": {
            "records_written": records_written,
            "checkpoints":     _checkpoint_count,
            "jsonl_state_uri": final_uri,
        },
    }

    summary_uri = save_to_s3(summary, S3_BUCKET, S3_PREFIX, "icd_api", REGION, end_time)
    log.info(
        "Done — %d records, %d checkpoints, %.1f h elapsed. Summary → %s",
        records_written, _checkpoint_count, elapsed_s / 3600, summary_uri,
    )


if __name__ == "__main__":
    main()