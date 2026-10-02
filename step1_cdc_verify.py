#!/usr/bin/env python3
"""
Step 1: CDC パイプライン内 多言語 UTF-8 保持検証

Docker DB2 へ DML を投入し、NiFi PutFile 出力（Debezium JSON）を解析して
payload.after / before の文字列一致を確認する。

使い方:
  python3 step1_cdc_verify.py --mode setup --db2-container db2_ebcdic
  python3 step1_cdc_verify.py --mode dml --db2-container db2_ebcdic
  python3 step1_cdc_verify.py --mode verify --putfile-dir /path/to/json
  python3 step1_cdc_verify.py --mode full --db2-container db2_ebcdic --putfile-dir /path/to/json
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REGION_ROWS: list[tuple[int, str, str, str]] = [
    (9001, "JPN", "日本語テスト", "混合文字列：日本語・English・123"),
    (9002, "SYZ", "中文测试", "简体中文混合测试"),
    (9003, "KOR", "한국어테스트", "한글 혼합 테스트"),
    (9004, "DEU", "Deutsch Test", "Grüße aus München"),
    (9005, "TUR", "İstanbul", "Türkçe karakter: ğüşıöç"),
    (9006, "USA", "Hello World", "English mixed 123"),
]

DML_TEST_ID = 9100


@dataclass
class VerifyResult:
    case_id: str
    category: str
    description: str
    expected: str
    actual: str | None
    status: str  # PASS | FAIL | SKIP | WAIT
    note: str
    source_file: str | None = None


def docker_cp_to_container(container: str, local_path: Path) -> str:
    """SQL ファイルをコンテナ内 /tmp にコピーし、パスを返す。"""
    remote = f"/tmp/ccsid_verify_{uuid.uuid4().hex[:8]}.sql"
    proc = subprocess.run(
        ["docker", "cp", str(local_path), f"{container}:{remote}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"docker cp failed: {(proc.stderr or proc.stdout).strip()}")
    return remote


def run_db2_in_container(container: str, db2_cmd: str, *, db: str = "TESTDB") -> tuple[int, str]:
    """db2inst1 として db2 コマンドを実行。"""
    inner = f"db2 connect to {db} >/dev/null 2>&1; {db2_cmd}"
    cmd = [
        "docker",
        "exec",
        container,
        "bash",
        "-lc",
        f"su - db2inst1 -c {shlex.quote(inner)}",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    out = ((proc.stdout or "") + (proc.stderr or "")).strip()
    return proc.returncode, out


def run_db2_sql_file(container: str, sql_path: Path, *, db: str = "TESTDB") -> tuple[int, str]:
    """SQL ファイルをコンテナにコピーして db2 -vf で実行（; 終端）。"""
    remote = docker_cp_to_container(container, sql_path)
    try:
        # NOTE: -t は終端を @ に変えるため使わない（; 終端なら -vf）
        code, out = run_db2_in_container(container, f"db2 -vf {shlex.quote(remote)}", db=db)
        return code, out
    finally:
        subprocess.run(
            ["docker", "exec", container, "rm", "-f", remote],
            capture_output=True,
            check=False,
        )


def is_benign_db2_result(out: str, stmt: str, allow_errors: tuple[str, ...]) -> bool:
    """再実行可能な DELETE / UPDATE 等の警告は成功扱い。"""
    if any(err in out for err in allow_errors):
        return True
    upper = stmt.upper()
    if "SQL0100W" in out or "SQLSTATE=02000" in out:
        if upper.startswith(("DELETE", "UPDATE")):
            return True
    return False


def sql_escape(text: str) -> str:
    return text.replace("'", "''")


def run_db2_idempotent(container: str, sql: str, *, db: str = "TESTDB") -> tuple[bool, str]:
    """冪等実行: 主キー重複時は DELETE して再 INSERT。"""
    ok, out = run_db2_statement(container, sql, db=db)
    if ok:
        return True, out

    stmt = " ".join(sql.split()).strip()
    if stmt.upper().startswith("INSERT") and "SQL0803N" in out:
        match = re.search(r"VALUES\s*\((\d+)", stmt, flags=re.IGNORECASE)
        if match:
            row_id = match.group(1)
            run_db2_statement(
                container,
                f"DELETE FROM DB2INST1.CCSID_TEST WHERE ID = {row_id}",
                db=db,
            )
            return run_db2_statement(container, sql, db=db)
    if is_benign_db2_result(out, stmt, ()):
        return True, out
    return False, out


def run_db2_statement(
    container: str,
    sql: str,
    *,
    db: str = "TESTDB",
    allow_errors: tuple[str, ...] = (),
) -> tuple[bool, str]:
    """1 文を db2 -x で実行。allow_errors に含まれる SQLCODE/SQLSTATE は許容。"""
    stmt = " ".join(sql.split()).strip()
    if not stmt.endswith(";"):
        stmt += ";"
    code, out = run_db2_in_container(container, f"db2 {shlex.quote(stmt)}", db=db)
    if code == 0:
        return True, out
    if is_benign_db2_result(out, stmt, allow_errors):
        return True, out
    return False, out


def run_setup_phase(container: str) -> tuple[bool, str]:
    """テーブル作成 + CDC 登録（再実行可能）。"""
    logs: list[str] = []

    steps: list[tuple[str, str, tuple[str, ...]]] = [
        (
            "REMOVETABLE",
            "CALL ASNCDC.REMOVETABLE('DB2INST1', 'CCSID_TEST')",
            ("SQL0440N", "SQL0204N", "SQL0601N", "SQL0203N"),
        ),
        ("DROP", "DROP TABLE IF EXISTS DB2INST1.CCSID_TEST", ()),
        (
            "CREATE",
            """
CREATE TABLE DB2INST1.CCSID_TEST (
    ID          INT NOT NULL PRIMARY KEY,
    REGION      VARCHAR(10),
    NAME_COL    VARCHAR(100),
    PROBLEM_COL VARCHAR(500)
)
""".strip(),
            (),
        ),
        ("ADDTABLE", "CALL ASNCDC.ADDTABLE('DB2INST1', 'CCSID_TEST')", ("SQL0601N",)),
        ("REINIT", "VALUES ASNCDC.ASNCDCSERVICES('reinit', 'asncdc')", ()),
        (
            "VERIFY",
            """
SELECT SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE
FROM ASNCDC.IBMSNAP_REGISTER
WHERE SOURCE_OWNER = 'DB2INST1' AND SOURCE_TABLE = 'CCSID_TEST'
""".strip(),
            (),
        ),
    ]

    verify_out = ""
    for label, sql, allow in steps:
        logs.append(f"--- {label} ---")
        ok, out = run_db2_statement(container, sql, allow_errors=allow)
        logs.append(out)
        if label == "VERIFY":
            verify_out = out
        if not ok:
            return False, "\n".join(logs)

    if "CCSID_TEST" not in verify_out or "record(s) selected" not in verify_out.lower():
        logs.append("ERROR: IBMSNAP_REGISTER に CCSID_TEST が見つかりません")
        return False, "\n".join(logs)

    return True, "\n".join(logs)


def strip_sql_comments(sql_text: str) -> str:
    """行コメント (-- ...) を除去。"""
    lines: list[str] = []
    for line in sql_text.splitlines():
        if "--" in line:
            line = line[: line.index("--")]
        stripped = line.strip()
        if stripped:
            lines.append(stripped)
    return "\n".join(lines)


def parse_sql_statements(sql_text: str) -> list[str]:
    """SQL ファイルを ; 区切りで文に分割（CONNECT / コメント除外）。"""
    statements: list[str] = []
    cleaned = strip_sql_comments(sql_text)
    for chunk in cleaned.split(";"):
        stmt = " ".join(chunk.split()).strip()
        if not stmt:
            continue
        upper = stmt.upper()
        if upper.startswith("CONNECT TO"):
            continue
        statements.append(stmt)
    return statements


def run_seed_regions(container: str) -> tuple[bool, str]:
    """6 リージョン相当データ（9001-9006）を冪等投入。"""
    logs: list[str] = ["=== seed regions (9001-9006) ==="]

    ok, out = run_db2_idempotent(
        container,
        "DELETE FROM DB2INST1.CCSID_TEST WHERE ID >= 9001 AND ID <= 9006",
    )
    logs.append("> DELETE 9001-9006")
    logs.append(out)
    if not ok:
        return False, "\n".join(logs)

    for row_id, region, name_col, problem_col in REGION_ROWS:
        insert_sql = (
            "INSERT INTO DB2INST1.CCSID_TEST VALUES "
            f"({row_id}, '{sql_escape(region)}', '{sql_escape(name_col)}', '{sql_escape(problem_col)}')"
        )
        logs.append(f"> INSERT ID={row_id} ({region})")
        ok, out = run_db2_idempotent(container, insert_sql)
        logs.append(out)
        if not ok:
            return False, "\n".join(logs)

    ok, out = run_db2_statement(
        container,
        "SELECT ID, REGION, NAME_COL FROM DB2INST1.CCSID_TEST WHERE ID >= 9001 AND ID <= 9006 ORDER BY ID",
    )
    logs.append("> SELECT (確認)")
    logs.append(out)
    return True, "\n".join(logs)


def run_cdc_dml_9100(container: str) -> tuple[bool, str]:
    """9100 行 INSERT / UPDATE / DELETE（CDC c/u/d 用）を冪等実行。"""
    logs: list[str] = ["=== cdc dml test (9100) ==="]
    steps = [
        ("DELETE (事前)", "DELETE FROM DB2INST1.CCSID_TEST WHERE ID = 9100"),
        (
            "INSERT",
            "INSERT INTO DB2INST1.CCSID_TEST VALUES (9100, 'SYZ', '中文测试', 'INSERT確認')",
        ),
        (
            "UPDATE",
            "UPDATE DB2INST1.CCSID_TEST SET PROBLEM_COL = 'UPDATE確認' WHERE ID = 9100",
        ),
        ("DELETE (CDC op:d)", "DELETE FROM DB2INST1.CCSID_TEST WHERE ID = 9100"),
    ]
    for label, sql in steps:
        logs.append(f"> {label}")
        ok, out = run_db2_idempotent(container, sql)
        logs.append(out)
        if not ok:
            return False, "\n".join(logs)
    return True, "\n".join(logs)


def run_dml_phase(container: str, sql_dir: Path) -> tuple[bool, str]:
    """DML 一括（データ有無に関わらず冪等完了）。"""
    del sql_dir  # SQL ファイルは参照用。実行は Python 側で冪等処理。
    parts: list[str] = []
    ok, out = run_seed_regions(container)
    parts.append(out)
    if not ok:
        return False, "\n".join(parts)
    ok, out = run_cdc_dml_9100(container)
    parts.append(out)
    return ok, "\n".join(parts)


def flatten_value(val: Any) -> Any:
    """Debezium schema ラッパー {type, value} を平坦化。"""
    if isinstance(val, dict):
        if "value" in val and set(val.keys()) <= {"value", "type", "name", "field", "optional", "version"}:
            return flatten_value(val["value"])
        return {k: flatten_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [flatten_value(v) for v in val]
    return val


def normalize_row(row: Any) -> dict[str, Any]:
    flat = flatten_value(row)
    if not isinstance(flat, dict):
        return {}
    return {str(k).upper(): v for k, v in flat.items()}


def extract_payloads(obj: Any) -> list[dict[str, Any]]:
    """JSON オブジェクトから Debezium payload を再帰的に抽出。"""
    found: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        if "op" in obj and ("after" in obj or "before" in obj):
            found.append(obj)
        if "payload" in obj:
            inner = obj["payload"]
            if isinstance(inner, dict):
                found.extend(extract_payloads(inner))
            elif isinstance(inner, str):
                try:
                    found.extend(extract_payloads(json.loads(inner)))
                except json.JSONDecodeError:
                    pass
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found.extend(extract_payloads(v))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(extract_payloads(item))
    elif isinstance(obj, str):
        text = obj.strip()
        if text.startswith("{") or text.startswith("["):
            try:
                found.extend(extract_payloads(json.loads(text)))
            except json.JSONDecodeError:
                pass
    return found


def payloads_from_file(path: Path) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return []
    if not text:
        return []

    payloads: list[dict[str, Any]] = []
    try:
        payloads.extend(extract_payloads(json.loads(text)))
    except json.JSONDecodeError:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payloads.extend(extract_payloads(json.loads(line)))
            except json.JSONDecodeError:
                continue
    return payloads


def diagnose_putfile(putfile_dir: Path, events: list[tuple[Path, dict[str, Any]]]) -> None:
    """PutFile ディレクトリの診断情報を表示。"""
    if not putfile_dir.is_dir():
        print(f"ERROR: ディレクトリが存在しません: {putfile_dir.resolve()}")
        return

    files = [p for p in putfile_dir.rglob("*") if p.is_file()]
    print(f"  ファイル数: {len(files)}")
    if not files:
        print("  → putfile-dir が空です。NiFi PutFile の Directory と一致しているか確認してください。")
        return

    print("  先頭 5 ファイル:")
    for p in files[:5]:
        print(f"    - {p} ({p.stat().st_size} bytes)")

    if not events:
        print("  → JSON から Debezium payload (op) を 0 件抽出。先頭ファイルの中身を確認:")
        sample = payloads_from_file(files[0])
        if sample:
            print(f"    再解析で {len(sample)} payload 検出（collect ロジック要確認）")
        else:
            head = files[0].read_text(encoding="utf-8", errors="replace")[:400]
            print(f"    {head!r}")
        return

    ops: dict[str, int] = {}
    ids: set[int] = set()
    for _, payload in events:
        op = str(payload.get("op", "?"))
        ops[op] = ops.get(op, 0) + 1
        for side in ("after", "before"):
            row = normalize_row(payload.get(side))
            if "ID" in row:
                try:
                    ids.add(int(row["ID"]))
                except (TypeError, ValueError):
                    pass

    print(f"  抽出イベント: {len(events)} 件 / op 内訳: {dict(sorted(ops.items()))}")
    if ids:
        shown = sorted(ids)
        print(f"  検出 ID: {shown[:20]}{'...' if len(shown) > 20 else ''}")
    else:
        print("  検出 ID: なし — after/before のフィールド名を確認（debug で raw 表示）")


def row_matches(row: dict[str, Any], *, row_id: int, region: str | None = None) -> bool:
    try:
        if int(row.get("ID")) != row_id:
            return False
    except (TypeError, ValueError):
        return False
    if region is not None and str(row.get("REGION", "")) != region:
        return False
    return True


def has_mojibake(text: str) -> bool:
    return "\ufffd" in text


def collect_events(putfile_dir: Path, since_ts: float | None = None) -> list[tuple[Path, dict[str, Any]]]:
    events: list[tuple[Path, dict[str, Any]]] = []
    if not putfile_dir.is_dir():
        return events
    for path in sorted(putfile_dir.rglob("*")):
        if not path.is_file():
            continue
        if since_ts is not None and path.stat().st_mtime < since_ts:
            continue
        for payload in payloads_from_file(path):
            if "op" in payload:
                events.append((path, payload))
    return events


def find_event(
    events: list[tuple[Path, dict[str, Any]]],
    *,
    op: str | list[str],
    row_id: int,
    region: str | None = None,
    field: str | None = None,
    field_value: str | None = None,
    allow_snapshot: bool = False,
) -> tuple[Path, dict[str, Any]] | None:
    op_list = [op] if isinstance(op, str) else op
    for path, payload in reversed(events):
        pop = payload.get("op")
        if pop not in op_list:
            continue
        snapshot = payload.get("snapshot")
        if not allow_snapshot and pop == "r":
            if snapshot in (True, "true", "last", "first"):
                continue
        side = "after" if pop in {"c", "r", "u"} else "before"
        if pop == "u" and field_value is not None:
            side = "after"
        row = normalize_row(payload.get(side))
        if not row_matches(row, row_id=row_id, region=region):
            continue
        if field and field_value is not None:
            if str(row.get(field.upper(), "")) != field_value:
                continue
        return path, payload
    return None


def verify_region_rows(events: list[tuple[Path, dict[str, Any]]]) -> list[VerifyResult]:
    results: list[VerifyResult] = []
    for row_id, region, name_col, problem_col in REGION_ROWS:
        found = find_event(
            events,
            op=["c", "r"],
            row_id=row_id,
            region=region,
            allow_snapshot=True,
        )
        if not found:
            results.append(
                VerifyResult(
                    case_id=f"REG-{region}",
                    category="多言語 UTF-8 保持",
                    description=f"{region} INSERT (op:c or op:r)",
                    expected=f"NAME_COL={name_col!r}",
                    actual=None,
                    status="WAIT",
                    note="PutFile JSON 未検出 — putfile-dir / JSON 形式を --debug で確認",
                )
            )
            continue
        path, payload = found
        pop = str(payload.get("op", "?"))
        side = "after" if pop in {"c", "r"} else "before"
        after = normalize_row(payload.get(side))
        actual_name = str(after.get("NAME_COL", ""))
        actual_prob = str(after.get("PROBLEM_COL", ""))
        ok = actual_name == name_col and actual_prob == problem_col
        note = f"op:{pop} file={path.name}"
        if has_mojibake(actual_name) or has_mojibake(actual_prob):
            ok = False
            note += " / 文字化け疑い"
        results.append(
            VerifyResult(
                case_id=f"REG-{region}",
                category="多言語 UTF-8 保持",
                description=f"{region} ({'snapshot' if pop == 'r' else 'INSERT'}) op:{pop}",
                expected=f"{name_col} / {problem_col}",
                actual=f"{actual_name} / {actual_prob}",
                status="PASS" if ok else "FAIL",
                note=note,
                source_file=str(path),
            )
        )
    return results


def verify_dml_ops(events: list[tuple[Path, dict[str, Any]]]) -> list[VerifyResult]:
    results: list[VerifyResult] = []

    ins = find_event(events, op="c", row_id=DML_TEST_ID, region="SYZ")
    if ins:
        path, payload = ins
        after = normalize_row(payload.get("after"))
        actual = str(after.get("PROBLEM_COL", ""))
        ok = actual == "INSERT確認"
        results.append(
            VerifyResult(
                case_id="DML-INS",
                category="CDC DML",
                description="INSERT (ID=9100)",
                expected="INSERT確認",
                actual=actual,
                status="PASS" if ok else "FAIL",
                note=f"op:c file={path.name}",
                source_file=str(path),
            )
        )
    else:
        results.append(
            VerifyResult(
                case_id="DML-INS",
                category="CDC DML",
                description="INSERT (ID=9100)",
                expected="INSERT確認",
                actual=None,
                status="WAIT",
                note="op:c 未検出",
            )
        )

    upd = find_event(events, op="u", row_id=DML_TEST_ID, field="PROBLEM_COL", field_value="UPDATE確認")
    if upd:
        path, payload = upd
        after = normalize_row(payload.get("after"))
        actual = str(after.get("PROBLEM_COL", ""))
        ok = actual == "UPDATE確認"
        results.append(
            VerifyResult(
                case_id="DML-UPD",
                category="CDC DML",
                description="UPDATE (ID=9100)",
                expected="UPDATE確認",
                actual=actual,
                status="PASS" if ok else "FAIL",
                note=f"op:u file={path.name}",
                source_file=str(path),
            )
        )
    else:
        results.append(
            VerifyResult(
                case_id="DML-UPD",
                category="CDC DML",
                description="UPDATE (ID=9100)",
                expected="UPDATE確認",
                actual=None,
                status="WAIT",
                note="op:u 未検出",
            )
        )

    dele = find_event(events, op="d", row_id=DML_TEST_ID)
    if dele:
        path, payload = dele
        before = normalize_row(payload.get("before"))
        after = payload.get("after")
        name = str(before.get("NAME_COL", ""))
        ok = name == "中文测试" and after is None
        results.append(
            VerifyResult(
                case_id="DML-DEL",
                category="CDC DML",
                description="物理 DELETE (ID=9100)",
                expected="before=中文测试, after=null",
                actual=f"before={name!r}, after={after!r}",
                status="PASS" if ok else "FAIL",
                note=f"op:d file={path.name}",
                source_file=str(path),
            )
        )
    else:
        results.append(
            VerifyResult(
                case_id="DML-DEL",
                category="CDC DML",
                description="物理 DELETE (ID=9100)",
                expected="before=中文测试, after=null",
                actual=None,
                status="WAIT",
                note="op:d 未検出",
            )
        )

    return results


def render_markdown(results: list[VerifyResult], meta: dict[str, Any]) -> str:
    lines = [
        "# Step 1 — CDC パイプライン 多言語 UTF-8 保持検証結果",
        "",
        f"- 実行日時: {meta['timestamp']}",
        f"- ホスト: {meta['hostname']}",
        f"- モード: {meta['mode']}",
        f"- DB2 コンテナ: {meta.get('db2_container') or '—'}",
        f"- PutFile ディレクトリ: {meta.get('putfile_dir') or '—'}",
        f"- PASS / FAIL / WAIT: {meta['pass']} / {meta['fail']} / {meta['wait']}",
        "",
        "> ※ Docker DB2 (LUW) 上の UTF-8 文字列投入。5035→939 等の z/OS CCSID 変換は Step 3。",
        "",
        "## スライド転記用テーブル",
        "",
        "| # | カテゴリ | 検証内容 | 期待値 | 結果 | 備考 |",
        "|---|---|---|---|---|---|",
    ]
    for r in results:
        exp = r.expected.replace("|", "\\|")[:40]
        note = (r.note or "")[:60]
        lines.append(
            f"| {r.case_id} | {r.category} | {r.description} | {exp} | **{r.status}** | {note} |"
        )
    lines.extend(["", "## 詳細", ""])
    for r in results:
        lines.append(f"### {r.case_id} — {r.description}")
        lines.append(f"- 期待: `{r.expected}`")
        lines.append(f"- 実際: `{r.actual}`")
        lines.append(f"- ステータス: **{r.status}**")
        if r.source_file:
            lines.append(f"- ソース: `{r.source_file}`")
        if r.note:
            lines.append(f"- 備考: {r.note}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 1 CDC pipeline UTF-8 verification")
    parser.add_argument(
        "--mode",
        choices=["dml", "dml9100", "verify", "full", "setup"],
        default="full",
    )
    parser.add_argument("--db2-container", default="db2_ebcdic")
    parser.add_argument("--putfile-dir", type=Path, default=None)
    parser.add_argument("--sql-dir", type=Path, default=Path(__file__).resolve().parent / "sql")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--wait-seconds", type=int, default=90)
    parser.add_argument("--since-seconds", type=int, default=0)
    parser.add_argument(
        "--debug",
        action="store_true",
        help="PutFile 診断情報を表示（ファイル数・op 内訳・ID 一覧）",
    )
    args = parser.parse_args()

    meta: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "hostname": subprocess.run(["hostname"], capture_output=True, text=True).stdout.strip(),
        "mode": args.mode,
        "db2_container": args.db2_container
        if args.mode in {"dml", "dml9100", "full", "setup"}
        else None,
        "putfile_dir": str(args.putfile_dir) if args.putfile_dir else None,
    }

    since_ts: float | None = None
    if args.since_seconds > 0:
        since_ts = time.time() - args.since_seconds

    if args.mode == "setup":
        print("=== テーブル作成 + CDC 登録 ===")
        ok, out = run_setup_phase(args.db2_container)
        print(out)
        if not ok:
            print("ERROR: setup 失敗", file=sys.stderr)
            return 1
        print("setup 完了 — IBMSNAP_REGISTER に CCSID_TEST を確認")
        return 0

    if args.mode == "dml9100":
        print(f"=== DB2 DML 9100 のみ ({args.db2_container}) ===")
        print("※ 初回スナップショット完了後に実行 — op:c/u/d を NiFi が取得")
        ok, log = run_cdc_dml_9100(args.db2_container)
        print(log)
        if not ok:
            print("ERROR: DML 9100 失敗", file=sys.stderr)
            return 1
        print("\nDML 9100 完了。1〜2 分後に verify:")
        print("  PUTFILE_DIR=/path/to/cdc-output MODE=verify ./run_step1_on_ec2.sh")
        return 0

    if args.mode in {"dml", "full"}:
        print(f"=== DB2 DML 投入 ({args.db2_container}) ===")
        ok, log = run_dml_phase(args.db2_container, args.sql_dir)
        print(log)
        if not ok:
            print("ERROR: DB2 DML 失敗", file=sys.stderr)
            return 1
        since_ts = time.time() - 5
        if args.mode == "dml":
            print("\nDML 完了（9001-9006=スナップショット用 / 9100=増分 CDC 用）。")
            print("  1) NiFi 起動 → 9001-9006 の op:r を cdc-output で確認")
            print("  2) MODE=dml9100 ./run_step1_on_ec2.sh  → 9100 の c/u/d")
            print("  3) PUTFILE_DIR=cdc-output MODE=verify ./run_step1_on_ec2.sh")
            return 0

    if args.mode in {"verify", "full"}:
        if not args.putfile_dir:
            print("ERROR: --putfile-dir を指定してください", file=sys.stderr)
            return 1
        if args.mode == "full" and args.wait_seconds > 0:
            print(f"=== NiFi 取得待機 {args.wait_seconds} 秒 ===")
            time.sleep(args.wait_seconds)
        putfile_path = args.putfile_dir.expanduser().resolve()
        events = collect_events(putfile_path, since_ts=since_ts)
        print(f"=== PutFile JSON {len(events)} 件を解析 ({putfile_path}) ===")
        if args.debug or len(events) == 0:
            print("--- 診断 ---")
            diagnose_putfile(putfile_path, events)
        results = verify_region_rows(events) + verify_dml_ops(events)
    else:
        results = []

    pass_count = sum(1 for r in results if r.status == "PASS")
    fail_count = sum(1 for r in results if r.status == "FAIL")
    wait_count = sum(1 for r in results if r.status == "WAIT")
    meta.update({"pass": pass_count, "fail": fail_count, "wait": wait_count})

    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "step1_results.json"
    md_path = args.output_dir / "step1_results.md"
    payload = {"meta": meta, "results": [asdict(r) for r in results]}
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(results, meta), encoding="utf-8")

    print(f"\nStep 1 検証: PASS={pass_count} FAIL={fail_count} WAIT={wait_count}")
    print(f"  JSON: {json_path}")
    print(f"  Markdown: {md_path}")
    for r in results:
        mark = {"PASS": "OK", "FAIL": "NG", "WAIT": "…"}[r.status]
        print(f"  [{mark}] {r.case_id}: {r.description} → {r.status}")

    if wait_count:
        return 2
    return 1 if fail_count else 0


if __name__ == "__main__":
    sys.exit(main())
