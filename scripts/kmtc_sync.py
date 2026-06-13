#!/usr/bin/env python3
"""
KMTC ptpSchedule 전용 동기화 (v1.0)

- 입력완료 ✓ + 반입시간 없음(미입항) 차수 대상
- FCL + LCL + 해상수출입 모두 처리
- 차수의 선명&항차로 KMTC 본선 매칭 → ETD/ETA + 캘린더 표기 PATCH
- 매시간 cron 실행 (별도 workflow)
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from unipass import KMTC_PORT_MAP, fetch_kmtc_schedule, match_kmtc_vessel

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2025-09-03"
DEFAULT_DS_ID = "37249e8e-4d2e-8362-ad24-87ad69c1ce5e"


def notion_request(method, path, token, body=None):
    url = NOTION_API + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def query_target_chasu(token, ds_id):
    """입력완료 + 반입시간 없음 + 해상 + FCL/LCL 차수 query."""
    pages = []
    cursor = None
    body_template = {
        "filter": {"and": [
            {"property": "입력완료√", "checkbox": {"equals": True}},
            {"or": [
                {"property": "프로세스", "status": {"equals": "미반영"}},
                {"property": "프로세스", "status": {"is_empty": True}},
            ]},
            {"or": [
                {"property": "I/O", "select": {"equals": "해상수입"}},
                {"property": "I/O", "select": {"equals": "해상수출"}},
            ]},
        ]},
        "page_size": 100,
    }
    while True:
        body = dict(body_template)
        if cursor:
            body["start_cursor"] = cursor
        res = notion_request("POST", f"/data_sources/{ds_id}/query", token, body)
        pages.extend(res.get("results", []))
        if not res.get("has_more"):
            break
        cursor = res.get("next_cursor")
    return pages


def extract_prop(props, name, kind):
    p = props.get(name)
    if not p:
        return None
    if kind == "title":
        arr = p.get("title", [])
        return "".join(t.get("plain_text", "") for t in arr) or None
    if kind == "rich_text":
        arr = p.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in arr) or None
    if kind == "select":
        s = p.get("select")
        return s.get("name") if s else None
    if kind == "date":
        d = p.get("date")
        return d.get("start") if d else None
    return None


def update_page(token, page_id, properties):
    return notion_request("PATCH", f"/pages/{page_id}", token, {"properties": properties})


def main():
    started = datetime.now()
    notion_token = os.environ.get("NOTION_TOKEN")
    if not notion_token:
        sys.stderr.write("[ERROR] NOTION_TOKEN 미설정\n")
        sys.exit(2)
    ds_id_raw = os.environ.get("NOTION_DS_ID", DEFAULT_DS_ID).replace("-", "")
    ds_id = f"{ds_id_raw[0:8]}-{ds_id_raw[8:12]}-{ds_id_raw[12:16]}-{ds_id_raw[16:20]}-{ds_id_raw[20:32]}"

    print(f"[{started.isoformat()}] KMTC 동기화 시작 (DS: {ds_id})")
    pages = query_target_chasu(notion_token, ds_id)
    print(f"  대상 차수: {len(pages)}건 (입력완료 + 프로세스=미반영 + 해상)")

    stats = {"total": len(pages), "matched": 0, "nomatch": 0, "skipped": 0, "errored": 0}

    for i, page in enumerate(pages):
        props = page.get("properties", {})
        chasu = extract_prop(props, "차수", "title") or ""
        type_ = extract_prop(props, "TYPE", "select") or ""
        io_type = extract_prop(props, "I/O", "select") or ""
        pol = (extract_prop(props, "POL", "select") or "").upper()
        pod = (extract_prop(props, "POD", "select") or "").upper()
        vessel_str = extract_prop(props, "선명&항차", "rich_text") or ""
        etd = extract_prop(props, "ETD", "date") or ""
        eta = extract_prop(props, "ETA", "date") or ""

        if not vessel_str or not pol or not pod:
            stats["skipped"] += 1
            print(f"  [{i+1}/{len(pages)}] {chasu:20} 스킵: 선명/POL/POD 누락")
            continue

        # periodDate: ETD 또는 ETA 기준 -3일
        ref = etd or eta
        if ref and len(ref) >= 10:
            try:
                dt = datetime.strptime(ref[:10], "%Y-%m-%d") - timedelta(days=3)
                period_date = dt.strftime("%Y%m%d")
            except Exception:
                period_date = datetime.now().strftime("%Y%m%d")
        else:
            period_date = datetime.now().strftime("%Y%m%d")

        try:
            vessels = fetch_kmtc_schedule(pol, pod, period_date, 4)
            matched = match_kmtc_vessel(vessels, vessel_str)
        except Exception as e:
            stats["errored"] += 1
            print(f"  [{i+1}/{len(pages)}] {chasu:20} 오류: {e}")
            continue

        if not matched:
            stats["nomatch"] += 1
            print(f"  [{i+1}/{len(pages)}] {chasu:20} NOMATCH: {vessel_str} ({pol}->{pod})")
            continue

        # update
        payload = {}
        if matched.get("etd"):
            etd_iso = matched["etd"] + "+09:00"
            payload["ETD"] = {"date": {"start": etd_iso}}
        if matched.get("eta"):
            eta_iso = matched["eta"] + "+09:00"
            payload["ETA"] = {"date": {"start": eta_iso}}
            payload["캘린더 표기"] = {"date": {"start": eta_iso[:10]}}

        if payload:
            try:
                update_page(notion_token, page["id"], payload)
                stats["matched"] += 1
                print(f"  [{i+1}/{len(pages)}] {chasu:20} MATCH: {matched['vesselName']} {matched['voyageNumber']} ETD={matched.get('etd')} ETA={matched.get('eta')}")
            except Exception as e:
                stats["errored"] += 1
                print(f"  [{i+1}/{len(pages)}] {chasu:20} update 오류: {e}")

    elapsed = (datetime.now() - started).total_seconds()
    print(f"\n[{datetime.now().isoformat()}] 완료 ({elapsed:.1f}초)")
    print(f"  매칭: {stats['matched']}건 / NOMATCH: {stats['nomatch']}건 / 스킵: {stats['skipped']}건 / 오류: {stats['errored']}건")


if __name__ == "__main__":
    main()
