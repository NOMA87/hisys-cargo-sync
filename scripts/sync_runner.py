#!/usr/bin/env python3
"""
v2.0 통합 자동 동기화 (Claude 토큰 0)

흐름:
  1. 노션 차수 DB query (필터 적용) — Notion API 직접 호출
  2. 각 차수의 hbl/mbl/type/io/hwaju 추출
  3. unipass.py 의 fetch_with_fallback + build_result 사용 (in-process)
  4. 기존 노션 값과 비교 → 변경된 필드만 PATCH
  5. 통계 로그 출력

환경변수 필수:
  UNIPASS_KEY      유니패스 인증키 (또는 ~/.config/unipass/key.txt)
  NOTION_TOKEN     노션 Internal Integration 토큰 (secret_xxxx)
  NOTION_DS_ID     차수 DB data source ID (collection://… 의 UUID)
                   기본값: 37249e8e-4d2e-8362-ad24-87ad69c1ce5e

cron 예시:
  # crontab -e
  0 9,14,18 * * 1-5 /usr/bin/python3 ~/unipass/sync_runner.py >> ~/.unipass.log 2>&1
"""
import json
import re
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from unipass import (
    get_api_key, fetch_with_fallback, build_result, is_invalid_bl, norm,
    QUARANTINE_HWAJU, fetch_hjit_freeday, fetch_snct_freeday, fetch_kmtc_schedule, match_kmtc_vessel
)


# === v2.17: 정합성 가드 ===
def vessel_match_ok(notion_str, hdr_ship, hdr_vyd):
    """노션 선명&항차 vs 유니패스 header 본선/항차 매칭.
    None 반환 시 검증 불가 (어느 한 쪽이 비어 있음).
    """
    if not notion_str or not hdr_ship:
        return None
    parts = notion_str.strip().split()
    if len(parts) < 2:
        return None
    notion_voy = parts[-1].upper()
    notion_name = "".join(parts[:-1]).upper()
    ship = (hdr_ship or "").replace(" ", "").upper()
    vyd = (hdr_vyd or "").replace(" ", "").upper()
    name_match = notion_name in ship or ship in notion_name
    voy_match = (notion_voy == vyd) or (vyd and (vyd in notion_voy or notion_voy in vyd))
    return name_match and voy_match


def eta_drift_days(notion_eta, header_eta):
    """노션 ETA vs 유니패스 etprDt 일자 차이 (abs days).
    None 반환 시 검증 불가.
    """
    if not notion_eta or not header_eta:
        return None
    try:
        n = datetime.strptime(notion_eta[:10], "%Y-%m-%d").date()
        h = datetime.strptime(header_eta[:10], "%Y-%m-%d").date()
        return abs((n - h).days)
    except Exception:
        return None


NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2025-09-03"

DEFAULT_DS_ID = "37249e8e-4d2e-8362-ad24-87ad69c1ce5e"


def get_notion_token():
    tok = os.environ.get("NOTION_TOKEN")
    if not tok:
        sys.stderr.write(
            "[ERROR] NOTION_TOKEN 환경변수가 없습니다.\n"
            "  노션 → Settings → Connections → integrations에서 발급\n"
            "  export NOTION_TOKEN='secret_xxxx...'\n"
        )
        sys.exit(2)
    return tok


def notion_request(method, path, token, body=None):
    url = NOTION_API + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Notion API {method} {path} → {e.code}: {body}")


def query_chasu_db(token, ds_id, page_size=100, backfill=False):
    """차수 DB query (페이지네이션 처리). 필터/정렬 적용.

    v2.5 backfill 모드:
        BACKFILL=1 환경변수 시 '프로세스 ≠ 반출완료' 조건 제외 → 모든 수입 차수 query.
        반입시간/반출시간만 PATCH (다른 필드는 미변경).
    """
    pages = []
    start_cursor = None
    filter_and = [
        {"property": "입력완료√", "checkbox": {"equals": True}},
        {"or": [
            {"property": "I/O", "select": {"equals": "해상수입"}},
            {"property": "I/O", "select": {"equals": "항공수입"}},
        ]},
    ]
    if not backfill:
        # 평소엔 반출완료 제외 (이미 끝난 차수는 다시 안 봄)
        filter_and.insert(1, {"property": "프로세스", "status": {"does_not_equal": "반출완료"}})
    body_template = {
        "filter": {"and": filter_and},
        "sorts": [{"property": "최종 편집 일시", "direction": "descending"}],
        "page_size": page_size,
    }
    while True:
        body = dict(body_template)
        if start_cursor:
            body["start_cursor"] = start_cursor
        result = notion_request("POST", f"/data_sources/{ds_id}/query", token, body)
        pages.extend(result.get("results", []))
        if not result.get("has_more"):
            break
        start_cursor = result.get("next_cursor")
    return pages


def extract_prop(page_props, name, kind):
    """노션 page properties에서 값 추출."""
    p = page_props.get(name)
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
    if kind == "checkbox":
        return p.get("checkbox", False)
    if kind == "relation":
        return [r.get("id") for r in p.get("relation", [])]
    return None


def get_hwaju_name(token, relation_ids):
    """화주 페이지 ID로 title 가져오기. 캐시."""
    if not relation_ids:
        return ""
    pid = relation_ids[0]
    cached = get_hwaju_name._cache.get(pid)
    if cached is not None:
        return cached
    try:
        page = notion_request("GET", f"/pages/{pid}", token)
        # 일반적으로 첫 title property가 화주명
        for name, prop in page.get("properties", {}).items():
            if prop.get("type") == "title":
                arr = prop.get("title", [])
                title = "".join(t.get("plain_text", "") for t in arr)
                get_hwaju_name._cache[pid] = title
                return title
    except Exception as e:
        sys.stderr.write(f"[WARN] 화주 fetch 실패 {pid}: {e}\n")
    get_hwaju_name._cache[pid] = ""
    return ""
get_hwaju_name._cache = {}


def parse_chasu_page(page, token):
    """차수 page → unipass 호출용 dict."""
    props = page.get("properties", {})
    eta = extract_prop(props, "ETA", "date")
    bl_yy = (eta or "")[:4] or str(datetime.now().year)
    hwaju_relation = extract_prop(props, "화주", "relation") or []
    return {
        "pageId": page["id"],
        "차수": extract_prop(props, "차수", "title") or "",
        "hbl": extract_prop(props, "HBL No.", "rich_text") or "",
        "mbl": extract_prop(props, "MBL No.", "rich_text") or "",
        "cargmt": extract_prop(props, "화물관리번호", "rich_text") or "",
        "io": extract_prop(props, "I/O", "select") or "",
        "type": extract_prop(props, "TYPE", "select") or "",
        "blYy": bl_yy,
        "eta": eta,
        "hwaju": get_hwaju_name(token, hwaju_relation),
        "current": {
            "프로세스": extract_prop(props, "프로세스", "status"),
            "ETA": eta,
            "수입신고번호": extract_prop(props, "수입신고번호", "rich_text"),
            "수입신고수리일": extract_prop(props, "수입신고수리일", "date"),
            "화물관리번호": extract_prop(props, "화물관리번호", "rich_text"),
            "검역신고번호": extract_prop(props, "검역신고번호", "rich_text"),
            "검역완료일": extract_prop(props, "검역완료일", "date"),
            "반입시간": extract_prop(props, "반입시간", "date"),
            "반출시간": extract_prop(props, "반출시간", "date"),
            "POL": extract_prop(props, "POL", "select"),
            "POD": extract_prop(props, "POD", "select"),
            "선명&항차": extract_prop(props, "선명&항차", "rich_text"),
            "ETD": extract_prop(props, "ETD", "date"),
            "POD 터미널": extract_prop(props, "POD 터미널", "select"),
            "CFS 창고": extract_prop(props, "CFS 창고", "rich_text"),
            "검사대상": extract_prop(props, "검사대상", "checkbox"),
            "비고": extract_prop(props, "비고", "rich_text"),
            "컨테이너 번호": extract_prop(props, "컨테이너 번호", "rich_text"),
        }
    }



def extract_container_nos_from_remark(remark):
    """비고 텍스트에서 컨테이너 번호 추출 (CNTR: XXX 또는 4alpha+7digit 패턴)."""
    if not remark:
        return None
    # 4 alpha + 7 digit 패턴 (ISO 6346 컨테이너 번호) 전체 매칭
    nums = re.findall(r"\b[A-Z]{4}\d{7}\b", remark.upper())
    return ", ".join(sorted(set(nums))) if nums else None


def build_diff(current, result, today_iso, backfill=False):
    """기존 값(current) vs 매핑 결과(result) 비교 → properties payload.

    v2.5 backfill 모드: 반입시간/반출시간만 PATCH (다른 필드는 안 건드림).
    """
    payload = {}

    def set_text(field, new_val):
        if new_val is None:
            return
        if (current.get(field) or "") != new_val:
            payload[field] = {"rich_text": [{"text": {"content": new_val}}]}

    def set_date(field, new_val):
        if not new_val:
            return
        cur_val = current.get(field) or ""
        # v2.15: 기존이 datetime이고 일자가 동일하면 PATCH skip (KMTC가 부착한 시간 보존)
        if cur_val and len(cur_val) > 10 and new_val[:10] == cur_val[:10]:
            return
        if cur_val != new_val:
            payload[field] = {"date": {"start": new_val}}

    def set_status(field, new_val):
        if not new_val or new_val == "미반영":
            return
        if current.get(field) != new_val:
            payload[field] = {"status": {"name": new_val}}

    def set_select(field, new_val):
        if not new_val:
            return
        if current.get(field) != new_val:
            payload[field] = {"select": {"name": new_val}}

    def set_checkbox(field, new_val):
        if current.get(field) != new_val:
            payload[field] = {"checkbox": new_val}

    # backfill 모드: 반입시간/반출시간만 PATCH (다른 필드 건드리지 않음)
    if backfill:
        set_date("반입시간", result.get("inboundAt"))
        set_date("반출시간", result.get("outboundAt"))
        set_text("컨테이너 번호", result.get("containerNos"))
        set_date("컨테이너 반출기한", result.get("hjitDeadline"))
        return payload

    set_status("프로세스", result.get("process"))
    # ETA 분담 (v2.15): 유니패스 = etprDt 일자만, 시간은 KMTC sync가 부착
    eta_val = result.get("eta")  # etprDt 일자
    set_date("ETA", eta_val)
    if eta_val:
        set_date("캘린더 표기", eta_val[:10])
    set_text("화물관리번호", result.get("cargMtNo"))
    set_text("수입신고번호", result.get("importDeclNo"))
    set_date("수입신고수리일", result.get("customsClearedAt"))
    set_text("검역신고번호", result.get("quarantineDeclNo"))
    set_date("검역완료일", result.get("quarantineAt"))
    set_date("반입시간", result.get("inboundAt"))
    set_date("반출시간", result.get("outboundAt"))
    set_select("POD 터미널", result.get("podTerminal"))
    set_text("CFS 창고", result.get("cfsWarehouse"))
    set_text("컨테이너 번호", result.get("containerNos"))
    set_date("컨테이너 반출기한", result.get("hjitDeadline"))

    if result.get("isManaged"):
        set_checkbox("검사대상", True)
        # 비고 append (이미 있으면 skip)
        marker = f"[관리대상화물] 검사 가능성 - {today_iso}"
        existing = current.get("비고") or ""
        if "[관리대상화물]" not in existing:
            new_remark = (existing + "\n" + marker).strip() if existing else marker
            payload["비고"] = {"rich_text": [{"text": {"content": new_remark}}]}

    return payload


def update_page(token, page_id, properties):
    return notion_request("PATCH", f"/pages/{page_id}", token, {"properties": properties})


def main():
    started = time.time()
    today_iso = datetime.now().strftime("%Y-%m-%d")
    notion_token = get_notion_token()
    api_key = get_api_key()
    ds_id = os.environ.get("NOTION_DS_ID", DEFAULT_DS_ID).replace("-", "")
    # UUID 형태로 복원
    ds_id = f"{ds_id[0:8]}-{ds_id[8:12]}-{ds_id[12:16]}-{ds_id[16:20]}-{ds_id[20:32]}"

    # v2.5: BACKFILL 모드 (모든 수입 차수 대상으로 반입/반출시간만 채움)
    backfill = os.environ.get("BACKFILL", "").lower() in ("1", "true", "yes")
    mode_label = "BACKFILL 모드 (반출완료 포함, 반입/반출시간만)" if backfill else "일반 모드"
    print(f"[{datetime.now().isoformat()}] 동기화 시작 (DS: {ds_id}) [{mode_label}]")

    pages = query_chasu_db(notion_token, ds_id, backfill=backfill)
    print(f"  대상 차수: {len(pages)}건")

    stats = {"total": len(pages), "updated": 0, "no_change": 0,
             "skipped": 0, "errored": 0, "managed": 0, "no_response": 0}
    unmatched_sheds = {}
    managed_chasu = []

    for i, page in enumerate(pages):
        case = parse_chasu_page(page, notion_token)
        if is_invalid_bl(case["hbl"]) or is_invalid_bl(case["mbl"]):
            stats["skipped"] += 1
            print(f"  [{i+1}/{len(pages)}] {case['차수']:20} 스킵: HBL 또는 MBL 공란/TBA")
            continue

        try:
            parsed, _, _, _, _ = fetch_with_fallback(
                api_key, case["blYy"], case["hbl"], case["mbl"],
                case["io"], case["type"], cargmt=case["cargmt"], hwaju=case["hwaju"]
            )
            result = build_result(parsed, case["hwaju"], case["io"])
        except Exception as e:
            sys.stderr.write(f"[ERR] {case['차수']} ({case['hbl']}): {e}\n")
            stats["errored"] += 1
            continue

        if result.get("skip"):
            reason = result.get("reason", "")
            stats["skipped"] += 1
            if "응답 헤더 없음" in reason or "적핟목록" in reason:
                stats["no_response"] += 1
            print(f"  [{i+1}/{len(pages)}] {case['차수']:20} 스킵: {reason[:60]}")

            # v2.16: skip 케이스에서도 비고 컨테이너 추출 + HJIT 반출기한 자동 조회
            try:
                cntr_fallback = extract_container_nos_from_remark(case["current"].get("비고"))
                pod_terminal_cur = case["current"].get("POD 터미널") or ""
                fb_payload = {}
                if cntr_fallback and cntr_fallback != (case["current"].get("컨테이너 번호") or ""):
                    fb_payload["컨테이너 번호"] = {"rich_text": [{"text": {"content": cntr_fallback}}]}
                # HJIT 반출기한 (POD 터미널=HJIT + 컨테이너 있음 + 미반출)
                if cntr_fallback and "HJIT" in pod_terminal_cur and not case["current"].get("반출시간"):
                    first_cntr = cntr_fallback.split(",")[0].strip()
                    try:
                        _d = fetch_hjit_freeday(first_cntr)
                        if _d:
                            fb_payload["컨테이너 반출기한"] = {"date": {"start": _d}}
                    except Exception as _e:
                        print(f"  [HJIT-FALLBACK-ERR] {case['차수']}: {_e}")
                # v2.18: SNCT 반출기한 (POD 터미널=SNCT)
                if cntr_fallback and "SNCT" in pod_terminal_cur and not case["current"].get("반출시간"):
                    first_cntr = cntr_fallback.split(",")[0].strip()
                    try:
                        _d = fetch_snct_freeday(first_cntr)
                        if _d:
                            fb_payload["컨테이너 반출기한"] = {"date": {"start": _d}}
                    except Exception as _e:
                        print(f"  [SNCT-FALLBACK-ERR] {case['차수']}: {_e}")
                if fb_payload:
                    update_page(notion_token, case["pageId"], fb_payload)
                    print(f"  [SKIP-FALLBACK] {case['차수']}: {list(fb_payload.keys())} 갱신")
            except Exception as _e:
                print(f"  [SKIP-FALLBACK-ERR] {case['차수']}: {_e}")
            continue

        # 컨테이너 번호 fallback: 유니패스에 없으면 비고에서 파싱
        if not result.get("containerNos"):
            result["containerNos"] = extract_container_nos_from_remark(case["current"].get("비고"))

        # HJIT 자동 조회 (POD=HJIT + 미반출 + 컨테이너 번호 있음)
        pod_terminal = (result.get("podTerminal") or "") or (case["current"].get("POD 터미널") or "")
        _is_hjit = "HJIT" in pod_terminal
        _has_cntr = bool(result.get("containerNos"))
        _no_outbound = not case["current"].get("반출시간")
        if _is_hjit and _no_outbound and _has_cntr:
            first_cntr = result["containerNos"].split(",")[0].strip()
            print(f"  [HJIT-DEBUG] {case['차수']}: try fetch cntr={first_cntr} proxy={'set' if os.environ.get('HJIT_PROXY_URL') else 'EMPTY'}")
            try:
                _d = fetch_hjit_freeday(first_cntr)
                result["hjitDeadline"] = _d
                print(f"  [HJIT-DEBUG] {case['차수']}: result={_d}")
            except Exception as _e:
                print(f"  [HJIT-ERROR] {case['차수']}: {type(_e).__name__}: {_e}")
        elif _is_hjit:
            print(f"  [HJIT-SKIP] {case['차수']}: hjit={_is_hjit} no_outbound={_no_outbound} has_cntr={_has_cntr}")

        # v2.18: SNCT 무료장치일 자동 조회 (POD 터미널 = SNCT + 미반출 + 컨테이너 있음)
        _is_snct = "SNCT" in pod_terminal
        if _is_snct and _no_outbound and _has_cntr:
            first_cntr = result["containerNos"].split(",")[0].strip()
            print(f"  [SNCT-DEBUG] {case['차수']}: try fetch cntr={first_cntr} proxy={'set' if os.environ.get('SNCT_PROXY_URL') else 'EMPTY'}")
            try:
                _d = fetch_snct_freeday(first_cntr)
                if _d:
                    result["hjitDeadline"] = _d  # 같은 노션 필드(컨테이너 반출기한)에 저장
                print(f"  [SNCT-DEBUG] {case['차수']}: result={_d}")
            except Exception as _e:
                print(f"  [SNCT-ERROR] {case['차수']}: {type(_e).__name__}: {_e}")

        # v2.17: 정합성 가드 — 본선 불일치 + ETA drift 30일+ 검사
        # 어긋나면 ETA/프로세스/통관 필드는 PATCH 보류, 컨테이너/HJIT는 통과
        vessel_ok = vessel_match_ok(
            case["current"].get("선명&항차"),
            result.get("shipNm"),
            result.get("vydf"),
        )
        drift = eta_drift_days(case["current"].get("ETA"), result.get("eta"))
        guard_warn = []
        if vessel_ok is False:
            guard_warn.append(
                f"본선 불일치: notion='{case['current'].get('선명&항차')}' vs 유니패스='{result.get('shipNm')} {result.get('vydf') or ''}'"
            )
        if drift is not None and drift >= 30:
            guard_warn.append(
                f"ETA drift {drift}일: notion={case['current'].get('ETA')} vs etprDt={result.get('eta')}"
            )
        if guard_warn:
            # 마스킹: 통관/검역/ETA 필드 제거 (혹시 다른 화물 데이터일 수 있음)
            for _k in ("process", "eta", "shipArrivalAt", "cargMtNo", "importDeclNo",
                       "customsClearedAt", "quarantineDeclNo", "quarantineAt",
                       "inboundAt", "outboundAt"):
                result[_k] = None
            # 비고에 경고 표시 (1회만 추가)
            existing_remark = case["current"].get("비고") or ""
            mark = f"[검증 실패 {today_iso}] " + " / ".join(guard_warn)
            if "[검증 실패" not in existing_remark:
                case["current"]["비고"] = (existing_remark + "\n" + mark).strip()
                # 비고는 build_diff에서 덮지 않으므로 별도 PATCH 필요
                try:
                    update_page(notion_token, case["pageId"], {
                        "비고": {"rich_text": [{"text": {"content": case["current"]["비고"]}}]}
                    })
                    print(f"  [GUARD] {case['차수']}: {' | '.join(guard_warn)}")
                except Exception as _e:
                    print(f"  [GUARD-ERR] {case['차수']}: {_e}")

        # 변경된 필드만 update
        diff = build_diff(case["current"], result, today_iso, backfill=backfill)
        if not diff:
            stats["no_change"] += 1
            print(f"  [{i+1}/{len(pages)}] {case['차수']:20} 변경 없음")
            continue

        try:
            update_page(notion_token, case["pageId"], diff)
            stats["updated"] += 1
            changed = ", ".join(diff.keys())
            print(f"  [{i+1}/{len(pages)}] {case['차수']:20} 업데이트: {changed}")
        except Exception as e:
            sys.stderr.write(f"[ERR] update {case['차수']}: {e}\n")
            stats["errored"] += 1
            continue

        if result.get("isManaged"):
            stats["managed"] += 1
            managed_chasu.append(case['차수'])

        # 매핑 실패 shedNm 수집
        if not result.get("podTerminal") and result.get("cfsWarehouse"):
            shed = result["cfsWarehouse"]
            unmatched_sheds.setdefault(shed, []).append(case["차수"])

        time.sleep(0.3)  # 노션 rate limit 회피 (3 req/sec)

    elapsed = time.time() - started
    print(f"\n[{datetime.now().isoformat()}] 완료 ({elapsed:.1f}초)")
    print(f"  업데이트: {stats['updated']}건")
    print(f"  변경 없음: {stats['no_change']}건")
    print(f"  스킵: {stats['skipped']}건 (응답 없음 {stats['no_response']})")
    print(f"  오류: {stats['errored']}건")
    if managed_chasu:
        print(f"  ⚠️ 관리대상화물 {stats['managed']}건: {', '.join(managed_chasu)}")
    if unmatched_sheds:
        print(f"  매핑 실패 shedNm:")
        for shed, cs in sorted(unmatched_sheds.items(), key=lambda x: -len(x[1])):
            print(f"    - {shed} ({len(cs)}건)")


if __name__ == "__main__":
    main()
