#!/usr/bin/env python3
"""
KMTC ptpSchedule 전용 동기화 (v1.8)

- 입력완료 ✓ + 프로세스 ∈ [미반영, 입항보고] OR is_empty
- FCL + LCL + 해상수출입 모두 처리
- 차수의 선명&항차로 KMTC 본선 매칭 → ETD/ETA + 캘린더 표기 PATCH
- 매시간 cron 실행 (별도 workflow)

v1.6 변경점:
- 필터에 "입항보고" 단계 추가 (본선 출항 직후 케이스 보강)
- 입항보고 단계 차수는 ETD만 갱신, ETA/캘린더는 스킵
  (유니패스 etprDt가 이미 정확한 실제 입항일을 보유, KMTC 예정값으로 덮으면 퇴행)
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from unipass import KMTC_PORT_MAP, fetch_kmtc_schedule, match_kmtc_vessel, get_port_tz

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
    """입력완료 + 프로세스(미반영|입항보고|비어있음) + 해상 차수 query."""
    pages = []
    cursor = None
    body_template = {
        "filter": {"and": [
            {"property": "입력완료√", "checkbox": {"equals": True}},
            {"or": [
                {"property": "프로세스", "status": {"equals": "미반영"}},
                {"property": "프로세스", "status": {"equals": "입항보고"}},
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
    if kind == "status":
        s = p.get("status")
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
    print(f"  대상 차수: {len(pages)}건 (입력완료 + 프로세스∈[미반영,입항보고] + 해상)")

    stats = {"total": len(pages), "matched": 0, "etd_only": 0, "nomatch": 0, "skipped": 0, "errored": 0}

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
        process = extract_prop(props, "프로세스", "status") or ""

        # 입항보고 단계 = 본선 이미 입항. ETA는 유니패스가 더 정확하므로 ETD만 갱신
        is_arrived = (process == "입항보고")

        if not vessel_str or not pol or not pod:
            stats["skipped"] += 1
            print(f"  [{i+1}/{len(pages)}] {chasu:20} 스킵: 선명/POL/POD 누락")
            continue

        # v2.0: 수출 차수는 ETD 경과 시 출항완료로 전환 (유니패스 대상 아님)
        if io_type == "해상수출" and etd and len(etd) >= 10:
            try:
                if datetime.strptime(etd[:10], "%Y-%m-%d") < datetime.now():
                    update_page(notion_token, page["id"], {"프로세스": {"status": {"name": "출항완료"}}})
                    stats["skipped"] += 1
                    print(f"  [{i+1}/{len(pages)}] {chasu:20} 출항완료 전환 (ETD {etd[:10]})")
                    continue
            except Exception as e:
                print(f"  [{i+1}/{len(pages)}] {chasu:20} 출항완료 전환 실패: {e}")

        # v1.9: ETA/ETD가 30일 이상 지난 차수는 스킵 (KMTC 스케줄은 4주 한정)
        _ref = eta or etd
        if _ref and len(_ref) >= 10:
            try:
                if (datetime.now() - datetime.strptime(_ref[:10], "%Y-%m-%d")).days > 30:
                    stats["skipped"] += 1
                    print(f"  [{i+1}/{len(pages)}] {chasu:20} 스킵: 과거 차수 ({_ref[:10]})")
                    continue
            except Exception:
                pass

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

        # v1.2: ETA/ETD 시간 부착 가드 + 수입/수출 캘린더 분기
        # - 노션 ETA/ETD가 비어있음 → datetime 그대로
        # - 노션 일자 == KMTC 일자 → datetime 부착 (시간 추가)
        # - 일자 불일치 → 무시 (옛 본선/지연 케이스 보호)
        # - 캘린더 표기: 수입=ETA 일자, 수출=ETD 일자
        is_export = (io_type == "해상수출")
        payload = {}

        # v1.4: ETD=POL timezone / ETA=POD timezone (KMTC API는 각 항구 LT 응답)
        pol_tz = get_port_tz(pol)
        pod_tz = get_port_tz(pod)

        # v1.5: 수출 차수는 datetime이어도 일자 동일 시 PATCH (timezone 정정)
        # 수입 차수는 기존 가드 유지 (사용자 수동 입력 보호)
        # v1.6: 수출은 ETD/ETA 무조건 KMTC 값으로 갱신 (본선/일자 변경 자동 반영)
        # 수입은 기존 가드 유지 (실제 입항 후 유니패스 우선)
        # ETD 처리 — 무조건 KMTC 값 갱신
        if matched.get("etd"):
            payload["ETD"] = {"date": {"start": matched["etd"] + pol_tz}}
        # ETA 처리 — 무조건 KMTC 값 갱신
        if matched.get("eta"):
            payload["ETA"] = {"date": {"start": matched["eta"] + pod_tz}}

       # 캘린더 표기: ETD(수출)/ETA(수입) payload와 항상 동기화
        if is_export and "ETD" in payload:
            payload["캘린더 표기"] = payload["ETD"]
        elif not is_export and "ETA" in payload:
            payload["캘린더 표기"] = payload["ETA"]

        if payload:
            try:
                update_page(notion_token, page["id"], payload)
                if is_arrived:
                    stats["etd_only"] += 1
                    tag = "[ETD-ONLY]"
                else:
                    stats["matched"] += 1
                    tag = "MATCH:"
                print(f"  [{i+1}/{len(pages)}] {chasu:20} {tag} {matched['vesselName']} {matched['voyageNumber']} ETD={matched.get('etd')} ETA={matched.get('eta')}")
            except Exception as e:
                stats["errored"] += 1
                print(f"  [{i+1}/{len(pages)}] {chasu:20} update 오류: {e}")

    elapsed = (datetime.now() - started).total_seconds()
    print(f"\n[{datetime.now().isoformat()}] 완료 ({elapsed:.1f}초)")
    print(f"  매칭: {stats['matched']}건 / ETD-only: {stats['etd_only']}건 / NOMATCH: {stats['nomatch']}건 / 스킵: {stats['skipped']}건 / 오류: {stats['errored']}건")


if __name__ == "__main__":
    main()
