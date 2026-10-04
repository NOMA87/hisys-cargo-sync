#!/usr/bin/env python3
"""
KMTC / HMM 스케줄 동기화 (v2.6)

- 입력완료 ✓ + 프로세스 ∈ [미반영, 입항보고] OR is_empty
- FCL + LCL + 해상수출입 모두 처리
- 차수의 선명&항차로 본선 매칭 → ETD/ETA + 캘린더 표기 PATCH
- 별도 workflow(kmtc_sync.yml)에서 cron 실행

조회 순서:
  - 선명이 "HMM "으로 시작 → HMM portSchedule 먼저, 없으면 KMTC
  - 그 외 → KMTC 먼저, 없으면 HMM (공동운항 선박 대비)

갱신 정책:
  - ETD/ETA는 항상 API 값으로 갱신 (본선/일자 변경 자동 반영)
  - 입항보고 단계 차수는 ETD-only로 집계 (ETA는 유니패스 실제 입항일이 더 정확)
  - 캘린더 표기: 수출=ETD, 수입=ETA
  - POD 터미널은 비어 있을 때만 (유니패스 실제값 우선)
  - 수출 CLS는 6666 더미값 제외

v2.6: 노션 헬퍼를 notion_api.py로 분리, 로그/날짜 처리 정리 (동작 동일)
"""
import os
import sys
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from notion_api import (
    get_notion_token, normalize_ds_id, query_data_source, extract_prop, update_page,
)
from unipass import (
    fetch_kmtc_schedule, match_kmtc_vessel, get_port_tz, match_terminal, fetch_hmm_schedule,
)

TARGET_FILTER = {"and": [
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
]}

TBA_TOKENS = ("TBA", "TBN", "TBD", "TBC")   # 선명 미정 표기
STALE_DAYS = 30                             # 이보다 오래된 ETA/ETD 차수는 스킵 (KMTC 스케줄은 4주 한정)
PERIOD_LEAD_DAYS = 3                        # periodDate = 기준일 - 3일


def parse_date(s):
    """노션 date 문자열(앞 10자리) → date, 실패 시 None."""
    if not s or len(s) < 10:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def lookup_schedule(pol, pod, ref, period_date, vessel_str):
    """HMM/KMTC 조회 순서를 선명에 따라 결정하고 첫 매칭 결과 반환.

    반환: (matched, source)  source ∈ {"HMM-FIRST", "KMTC", "HMM-FALLBACK", None}
    """
    is_hmm = vessel_str.strip().upper().startswith("HMM ")
    if is_hmm:
        matched = fetch_hmm_schedule(pol, pod, ref, vessel_str)
        if matched:
            return matched, "HMM-FIRST"
    matched = match_kmtc_vessel(fetch_kmtc_schedule(pol, pod, period_date, 4), vessel_str)
    if matched:
        return matched, "KMTC"
    if not is_hmm:
        # v2.4: KMTC에 없으면 HMM portSchedule로 fallback (HMM + 공동운항 선박)
        # fallback 실패는 오류가 아니라 NOMATCH로 집계
        try:
            matched = fetch_hmm_schedule(pol, pod, ref, vessel_str)
        except Exception as e:
            print(f"  [HMM-FALLBACK-ERR] {vessel_str}: {e}")
            matched = None
        if matched:
            return matched, "HMM-FALLBACK"
    return None, None


def build_payload(matched, props, pol, pod, is_export):
    """매칭 결과 → 노션 PATCH payload."""
    pol_tz = get_port_tz(pol)
    pod_tz = get_port_tz(pod)
    payload = {}

    if matched.get("etd"):
        payload["ETD"] = {"date": {"start": matched["etd"] + pol_tz}}
    if matched.get("eta"):
        payload["ETA"] = {"date": {"start": matched["eta"] + pod_tz}}

    # 캘린더 표기: ETD(수출)/ETA(수입) payload와 항상 동기화
    cal_key = "ETD" if is_export else "ETA"
    if cal_key in payload:
        payload["캘린더 표기"] = payload[cal_key]

    # v2.1: POD 터미널 사전 확보 (비어있을 때만 — 유니패스 실제값 우선)
    if matched.get("podTerminal") and not extract_prop(props, "POD 터미널", "select"):
        pod_terminal, _ = match_terminal(matched["podTerminal"])
        if pod_terminal:
            payload["POD 터미널"] = {"select": {"name": pod_terminal}}

    # v2.2: 수출 서류마감(CLS) 기록 — 6666 더미값 제외
    cls = matched.get("cls") or ""
    if is_export and cls and not cls.startswith("6666"):
        payload["CLS"] = {"date": {"start": cls + pol_tz}}

    return payload


def main():
    started = datetime.now()
    today = started.date()
    notion_token = get_notion_token()
    ds_id = normalize_ds_id()

    print(f"[{started.isoformat()}] KMTC 동기화 시작 (DS: {ds_id})")
    pages = query_data_source(notion_token, ds_id, TARGET_FILTER)
    total = len(pages)
    print(f"  대상 차수: {total}건 (입력완료 + 프로세스∈[미반영,입항보고] + 해상)")

    stats = {"matched": 0, "etd_only": 0, "nomatch": 0, "skipped": 0, "errored": 0}

    for i, page in enumerate(pages, 1):
        props = page.get("properties", {})
        chasu = extract_prop(props, "차수", "title") or ""
        io_type = extract_prop(props, "I/O", "select") or ""
        pol = (extract_prop(props, "POL", "select") or "").upper()
        pod = (extract_prop(props, "POD", "select") or "").upper()
        vessel_str = extract_prop(props, "선명&항차", "rich_text") or ""
        etd = extract_prop(props, "ETD", "date") or ""
        eta = extract_prop(props, "ETA", "date") or ""
        process = extract_prop(props, "프로세스", "status") or ""
        is_export = (io_type == "해상수출")
        # 입항보고 단계 = 본선 이미 입항. ETA는 유니패스가 더 정확하므로 ETD-only로 집계
        is_arrived = (process == "입항보고")

        def log(msg):
            print(f"  [{i}/{total}] {chasu:20} {msg}")

        if not vessel_str or not pol or not pod:
            stats["skipped"] += 1
            log("스킵: 선명/POL/POD 누락")
            continue

        # v2.0: 수출 차수는 ETD 경과 시 출항완료로 전환 (유니패스 대상 아님)
        etd_date = parse_date(etd)
        if is_export and etd_date and etd_date < today:
            try:
                update_page(notion_token, page["id"], {"프로세스": {"status": {"name": "출항완료"}}})
                stats["skipped"] += 1
                log(f"출항완료 전환 (ETD {etd[:10]})")
                continue
            except Exception as e:
                log(f"출항완료 전환 실패: {e}")

        # v1.9: ETA/ETD가 30일 이상 지난 차수는 스킵 (KMTC 스케줄은 4주 한정)
        stale_ref = parse_date(eta or etd)
        if stale_ref and (today - stale_ref).days > STALE_DAYS:
            stats["skipped"] += 1
            log(f"스킵: 과거 차수 ({(eta or etd)[:10]})")
            continue

        # v2.5: 선명 미정(TBA/TBN/TBD/TBC)은 API 조회 생략
        if vessel_str.strip().upper().split()[0] in TBA_TOKENS:
            stats["skipped"] += 1
            log(f"스킵: 선명 미정 ({vessel_str})")
            continue

        # periodDate: ETD 또는 ETA 기준 -3일
        ref = etd or eta
        ref_date = parse_date(ref)
        period_base = datetime.combine(ref_date, datetime.min.time()) - timedelta(days=PERIOD_LEAD_DAYS) if ref_date else datetime.now()
        period_date = period_base.strftime("%Y%m%d")

        try:
            matched, source = lookup_schedule(pol, pod, ref, period_date, vessel_str)
        except Exception as e:
            stats["errored"] += 1
            log(f"오류: {e}")
            continue

        if not matched:
            stats["nomatch"] += 1
            log(f"NOMATCH: {vessel_str} ({pol}->{pod})")
            continue
        if source != "KMTC":
            log(f"{source}: {matched.get('voyageNumber')} ETD={matched.get('etd')} ETA={matched.get('eta')}")

        payload = build_payload(matched, props, pol, pod, is_export)
        if not payload:
            continue
        try:
            update_page(notion_token, page["id"], payload)
        except Exception as e:
            stats["errored"] += 1
            log(f"update 오류: {e}")
            continue
        if is_arrived:
            stats["etd_only"] += 1
            tag = "[ETD-ONLY]"
        else:
            stats["matched"] += 1
            tag = "MATCH:"
        log(f"{tag} {matched['vesselName']} {matched['voyageNumber']} ETD={matched.get('etd')} ETA={matched.get('eta')}")

    elapsed = (datetime.now() - started).total_seconds()
    print(f"\n[{datetime.now().isoformat()}] 완료 ({elapsed:.1f}초)")
    print(f"  매칭: {stats['matched']}건 / ETD-only: {stats['etd_only']}건 / NOMATCH: {stats['nomatch']}건"
          f" / 스킵: {stats['skipped']}건 / 오류: {stats['errored']}건")


if __name__ == "__main__":
    main()
