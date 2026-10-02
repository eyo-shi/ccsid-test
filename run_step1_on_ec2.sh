#!/usr/bin/env bash
# EC2 上で Step 1 CDC パイプライン検証を実行
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DB2_CONTAINER="${DB2_CONTAINER:-db2_ebcdic}"
PUTFILE_DIR="${PUTFILE_DIR:-}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"
MODE="${MODE:-full}"

usage() {
  cat <<EOF
Usage: $0 [options]

  Step 1: Docker DB2 へ DML 投入 → NiFi PutFile JSON を検証

Environment:
  DB2_CONTAINER   Docker DB2 コンテナ名 (default: db2_ebcdic)
  PUTFILE_DIR     NiFi PutFile 出力ディレクトリ (verify/full 時必須)
  WAIT_SECONDS    NiFi 取得待機秒数 (default: 90)
  MODE            setup | dml | dml9100 | verify | full (default: full)

Examples:
  # 初回: テーブル作成
  MODE=setup $0

  # DML のみ（9001-9006 + 9100）
  MODE=dml $0

  # 9100 のみ（スナップショット完了後 → op:c/u/d 取得用）
  MODE=dml9100 $0

  # PutFile JSON 検証のみ
  PUTFILE_DIR=/path/to/putfile MODE=verify $0

  # 一括実行
  PUTFILE_DIR=/path/to/putfile MODE=full $0
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

echo "=== Step 1 CDC パイプライン検証 ==="
echo "  MODE:          ${MODE}"
echo "  DB2 container: ${DB2_CONTAINER}"
echo "  PUTFILE_DIR:   ${PUTFILE_DIR:-（未設定）}"
echo "  WAIT_SECONDS:  ${WAIT_SECONDS}"

ARGS=(
  "${SCRIPT_DIR}/step1_cdc_verify.py"
  --mode "${MODE}"
  --db2-container "${DB2_CONTAINER}"
  --output-dir "${SCRIPT_DIR}/results"
  --wait-seconds "${WAIT_SECONDS}"
)

if [[ -n "${PUTFILE_DIR}" ]]; then
  ARGS+=(--putfile-dir "${PUTFILE_DIR}")
fi

python3 "${ARGS[@]}"

echo ""
echo "結果: ${SCRIPT_DIR}/results/step1_results.md"
