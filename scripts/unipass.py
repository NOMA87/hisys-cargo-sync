#!/usr/bin/env python3
"""
유니패스 cargCsclPrgsInfoQry 호출 + 응답 매핑

검색키 분기 (v1.1):
    - 해상수입 + TYPE=FCL  → MBL로 검색
    - 해상수입 + TYPE=LCL  → HBL로 검색
    - 항공수입             → HBL 우선, 빈 응답이면 MBL fallback
    - 해상수입 + TYPE 미지정 → MBL 우선, 빈 응답이면 HBL fallback (안전 기본값)

사용법:
    python3 unipass.py \
        --hbl PENAVICOTZ202600047 \
        --mbl MAEU268709729 \
        --bl-yy 2026 \
        --hwaju "하이시스 로지텍" \
        --io 해상수입 \
        --type FCL

출력 (JSON, stdout):
    {
      "skip": false,
      "reason": "정상 매핑",
      "process": "반출완료",
      "eta": "2026-04-18",
      "cargMtNo": "26MAEUI054I11410001",
      "importDeclNo": "4431326400354M",
      "customsClearedAt": "2026-04-23",
      "quarantineDeclNo": "11-2CF...",
      "quarantineAt": "2026-04-20",
      "isManaged": false,
      "_searchKey": "MBL",   # 실제 사용된 검색키
      "_searchValue": "MAEU268709729"
    }

환경변수:
    UNIPASS_KEY  -  유니패스 인증키 (또는 ~/.config/unipass/key.txt)
"""
import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

ENDPOINT = "https://unipass.customs.go.kr:38010/ext/rest/cargCsclPrgsInfoQry/retrieveCargCsclPrgsInfo"

# 매핑 화이트리스트 (정규화 키 → 노션 프로세스 + priority)
STAGE_MAP = {
    "입항보고수리":              {"name": "입항보고",     "priority": 1},
    "입항적재화물목록심사완료":  {"name": "입항보고",     "priority": 1},
    "하선신고수리":              {"name": "하선신고수리", "priority": 2},  # 해상
    "하기신고수리":              {"name": "하선신고수리", "priority": 2},  # 항공 (v2.0)
    "수입신고":                  {"name": "수입신고",     "priority": 7},
    "수입(사용소비)심사진행":    {"name": "심사진행",     "priority": 8},
    "수입(사용소비)결재통보":    {"name": "결재통보",     "priority": 9},
    "수입신고수리":              {"name": "통관완료",     "priority": 10},
    # v2.0: 검역 단계 세분화 (#9)
    "검역신청":                  {"name": "검역대기",     "priority": 5},
    "검사/검역식품의약품(불합격)": {"name": "검역대기",  "priority": 5},
}

SUPPORTED_IO = {"해상수입", "항공수입"}

# 식품검역 대상 화주 (노션 화주 DB의 마커 또는 화주명) — v2.0 (#5)
# 추후 노션 화주 DB에 "식품검역대상" 체크박스 추가 시 그걸로 대체 가능
QUARANTINE_HWAJU = {"하이시스 로지텍", "하이시스로지텍"}

# MBL 우선 검색 화주 (LCL인데 HBL로 검색 안 되는 케이스 — 자향 등)
HWAJU_MBL_FIRST = {"자향"}

# v2.0: 빈 응답/no-data 사전 필터 (#4)
INVALID_BL_VALUES = {"TBA", "TBD", "TBN", "", "N/A", "NA", "-"}

# v2.0: 호출 재시도 설정 (#1)
RETRY_COUNT = 3
RETRY_DELAY_SEC = 2

# shedNm(보세창고/터미널) → 노션 POD 터미널 옵션 매칭 (정규화 키워드 기반)
# 우선순위 순서대로 첫 매칭 채택. 약어와 풀네임 둘 다 검사.
TERMINAL_MAP = [
    # (포함 키워드 리스트, 노션 옵션명) — 키워드는 norm() 후 검사
    # 우선순위: 더 구체적인 키워드를 먼저 (예: "한진부산"이 "한진"보다 먼저)
    # === 인천신항 ===
    (["HJIT", "한진인천"],         "한진인천컨테이너터미널(HJIT)"),
    (["SNCT", "선광신컨테이너"],   "선광신컨테이너터미널(SNCT)"),
    # === 인천구항 ===
    (["E1CT", "E1컨테이너"],       "E1컨테이너터미널(E1CT)"),
    # 주의: "인천컨테이너터미널"은 "한진인천컨테이너터미널"에도 포함되므로 키워드에서 제외
    (["PSA인천", "ICT", "PSA"], "PSA인천컨테이너터미널(ICT)"),
    (["IFT", "인천국제여객", "인천여객"], "인천 국제여객터미널(IFT)"),
    # === 인천경인 ===
    (["HSIT", "SM상선경인", "SM상선"], "SM상선 경인터미널(HSIT)"),
    # === 부산신항 (구체적 키워드 먼저) ===
    (["HJNC", "한진부산컨테이너", "한진부산"], "한진부산컨테이너터미널(HJNC)"),
    (["HMMPSA", "에이치엠엠피에스에이"], "HMM PSA 신항만(HMM PSA)"),
    (["BNCT", "비엔씨티"],         "비엔씨티(BNCT)"),
    (["HPNT", "현대부산신항"],     "현대부산신항만(HPNT)"),
    (["PNCT", "평택동방"],         "평택동방아이포트(PNCT)"),  # PNC보다 먼저
    (["PCTC", "한진평택", "평택컨테이너"], "한진평택컨테이너터미널(PCTC)"),
    (["PNIT", "부산신항국제"],     "부산신항국제터미널(PNIT)"),
    (["PNC", "부산신항만"],        "부산신항만 주식회사(PNC)"),
    (["BCT"],                     "부산컨테이너터미널(BCT)"),
    (["DGT", "동원글로벌"],        "동원글로벌터미널(DGT)"),
    (["BNMT", "부산신항다목적"],   "부산신항다목적터미널(BNMT)"),
    # === 부산북항 (한국허치슨 → 허치슨부산 → BPTC 순) ===
    (["HGCT", "한국허치슨"],       "신감만 한국허치슨(HGCT)"),
    (["HBCT", "허치슨부산"],       "허치슨부산터미널(HBCT)"),
    (["TOC", "인터지스"],          "인터지스 7부두 보세창고(TOC)"),
    # 신선대가 "신선대감만터미널" 같은 케이스에 우선 매치되도록 위로
    (["신선대"],                  "부산항터미널 신선대(BPTC)"),
    (["BPTC감만", "감만"],         "부산항터미널 감만(BPTC)"),
    (["BPTC", "BPT"],             "부산항터미널 신선대(BPTC)"),
    (["BIFT", "부산항국제여객", "국제여객터미널지정장치장"], "부산항국제여객터미널(BIFT)"),
    (["IFPC", "인천항국제여객"],   "인천항국제여객부두(IFPC)"),
    # === 광양 ===
    (["GWCT", "광양서부"],         "광양서부컨테이너터미널(GWCT)"),
    (["KIT", "한국국제터미널"],    "한국국제터미널(KIT)"),
    # === 군산 ===
    (["IGCT", "군산컨테이너"],     "군산 컨테이너터미널(IGCT)"),
    # === 울산 ===
    (["UNCT", "유엔씨티"],         "유엔씨티(UNCT)"),
    (["JUCT", "정일울산"],         "정일울산 컨테이너터미널(JUCT)"),
    # === 항공 ===
    # 주의: "인천항공동물류보세창고"는 인천항(해상)의 보세창고이지 인천공항이 아님 → CFS로 떨어뜨림
    (["인천공항"], "인천공항화물터미널(ICN)"),
]


def norm(s):
    return re.sub(r"\s+", "", s or "")


def match_terminal(shed_nm):
    """
    shedNm 텍스트 → (podTerminal, cfsWarehouse) 반환.

    - CY 터미널 매칭되면 podTerminal 옵션명, cfsWarehouse=None
    - 매칭 실패 시 podTerminal=None, cfsWarehouse=원문 (CFS/기타로 추정)

    매칭 우선순위는 TERMINAL_MAP의 코드 순서대로 (자동 정렬 안 함).
    "신감만 한국허치슨" vs "감만" 같은 충돌 방지를 위해 더 구체적인 키워드를 위에 둘 것.
    """
    if not shed_nm:
        return None, None
    n = norm(shed_nm).upper()
    for keywords, option_name in TERMINAL_MAP:
        for kw in keywords:
            if norm(kw).upper() in n:
                return option_name, None
    # CY 매칭 실패 — CFS이거나 미등록 터미널
    return None, shed_nm.strip()


# v2.0 (#4): TBA/빈 BL 사전 필터
def is_invalid_bl(value):
    return not value or norm(value).upper() in {norm(v).upper() for v in INVALID_BL_VALUES}


def get_api_key():
    """
    유니패스 인증키 반환.
    UNIPASS_PROXY_URL이 설정된 경우 vercel proxy가 인증키 처리하므로
    빈 문자열 반환해도 됨 (proxy 모드).
    """
    # proxy 모드: 인증키는 vercel 환경변수에서 처리
    if os.environ.get("UNIPASS_PROXY_URL", "").strip():
        return ""

    key = os.environ.get("UNIPASS_KEY")
    if key:
        return key.strip()
    path = Path.home() / ".config" / "unipass" / "key.txt"
    if path.exists():
        return path.read_text().strip()
    sys.stderr.write(
        "[ERROR] 유니패스 인증키를 찾을 수 없습니다.\n"
        "  방법 1: export UNIPASS_KEY='발급받은키'\n"
        "  방법 2: mkdir -p ~/.config/unipass && echo '발급받은키' > ~/.config/unipass/key.txt\n"
        "  방법 3 (해외 IP): export UNIPASS_PROXY_URL='https://your-proxy.vercel.app/api/proxy' + PROXY_TOKEN\n"
    )
    sys.exit(2)


def call_unipass(api_key, bl_yy, hbl=None, mbl=None, cargmt=None, retries=RETRY_COUNT):
    """
    hbl/mbl/cargmt 중 하나 사용. 우선순위: cargmt > mbl > hbl.

    v2.0 (#1): 빈 응답(< 200B) 또는 헤더 없음 시 자동 재시도 (최대 retries회).
    재시도 간격은 RETRY_DELAY_SEC * 시도횟수 (점증).

    v2.1 (GitHub Actions 등 해외 IP 환경 지원):
        환경변수 UNIPASS_PROXY_URL이 설정되면 vercel proxy로 호출
        (예: https://hisys-unipass-proxy-1hng.vercel.app/api/proxy)
        proxy는 PROXY_TOKEN 환경변수 또는 ~/.config/unipass/proxy_token.txt 사용.
    """
    import time
    proxy_url = os.environ.get("UNIPASS_PROXY_URL", "").strip()

    if proxy_url:
        # Vercel proxy 사용 — crkyCn은 vercel 환경변수에서 자동 주입
        params = {"blYy": bl_yy}
        token_path = Path.home() / ".config" / "unipass" / "proxy_token.txt"
        proxy_token = os.environ.get("PROXY_TOKEN", "").strip()
        if not proxy_token and token_path.exists():
            proxy_token = token_path.read_text().strip()
        if proxy_token:
            params["token"] = proxy_token
        if cargmt:
            params["cargMtNo"] = cargmt
        elif mbl:
            params["mblNo"] = mbl
        elif hbl:
            params["hblNo"] = hbl
        else:
            raise ValueError("hbl, mbl, or cargmt required")
        url = proxy_url + "?" + urllib.parse.urlencode(params)
    else:
        # 직접 호출 (한국 IP 환경)
        params = {"crkyCn": api_key, "blYy": bl_yy}
        if cargmt:
            params["cargMtNo"] = cargmt
        elif mbl:
            params["mblNo"] = mbl
        elif hbl:
            params["hblNo"] = hbl
        else:
            raise ValueError("hbl, mbl, or cargmt required")
        url = ENDPOINT + "?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={
        "Accept": "*/*",
        "User-Agent": "hisys-cargo-sync/2.1",
    })
    last_text = ""
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8")
            last_text = text
            # 정상 응답 (헤더 포함) 이면 즉시 반환
            if "<cargCsclPrgsInfoQryVo>" in text:
                return text
            # 빈 응답 또는 헤더 없음 — 재시도
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY_SEC * (attempt + 1))
        except Exception as e:
            last_text = f"ERROR: {e}"
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY_SEC * (attempt + 1))
            else:
                raise
    return last_text


def parse_response(xml_text):
    if not xml_text or len(xml_text.strip()) < 50:
        return {"_empty": True}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        return {"_error": f"XML parse: {e}"}
    ntce = root.findtext("ntceInfo") or ""
    if ntce.strip():
        # 다건 응답이거나 오류
        if ntce.strip().startswith("[N00]"):
            return {"_multi": True, "_ntce": ntce}
        return {"_error": ntce}

    header_el = root.find("cargCsclPrgsInfoQryVo")
    if header_el is None:
        return {"_empty": True}

    header = {child.tag: (child.text or "") for child in header_el}
    history = []
    for item in root.findall("cargCsclPrgsInfoDtlQryVo"):
        history.append({child.tag: (child.text or "") for child in item})
    return {"header": header, "history": history}


def yyyymmdd_to_iso(s):
    if not s or len(s) < 8:
        return None
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def map_process(history, hwaju):
    """진행이력 → 프로세스 옵션명"""
    candidates = []
    has_inbound = False
    has_pass = False

    inbound_count = sum(
        1 for h in history if norm(h.get("cargTrcnRelaBsopTpcd", "")) == "반입신고"
    )

    for item in history:
        t = norm(item.get("cargTrcnRelaBsopTpcd", ""))
        c = norm(item.get("rlbrCn", ""))

        if t in STAGE_MAP:
            candidates.append(STAGE_MAP[t])
            continue

        if t == "반입신고":
            has_inbound = True
            if "입항반입" in c and inbound_count >= 2:
                candidates.append({"name": "터미널반입", "priority": 3})
            else:
                candidates.append({"name": "반입완료", "priority": 4})
            continue

        if "검사/검역식품의약품(합격)" in t:
            has_pass = True
            continue

        if t == "반출신고":
            if "보세운송반출" in c:
                continue  # LCL 중간단계 무시
            candidates.append({"name": "반출완료", "priority": 11})
            continue

    # 검역 분기 (식품화주 마커 기반) — v2.0 (#5)
    # QUARANTINE_HWAJU 셋에 포함되거나 hwaju가 "식품" 키워드 포함 시
    is_food_hwaju = hwaju in QUARANTINE_HWAJU or "식품" in (hwaju or "")
    if is_food_hwaju:
        if has_pass:
            candidates.append({"name": "검역완료", "priority": 6})
        elif has_inbound:
            candidates.append({"name": "검역대기", "priority": 5})

    if not candidates:
        return None
    candidates.sort(key=lambda x: x["priority"], reverse=True)
    return candidates[0]["name"]


def find_history(history, predicate):
    for h in history:
        if predicate(h):
            return h
    return None


def build_result(parsed, hwaju, io_type):
    if io_type not in SUPPORTED_IO:
        return {
            "skip": True,
            "reason": f"수입 차수 아님 (I/O={io_type})",
            "process": "미반영",
            "eta": None, "cargMtNo": None,
            "importDeclNo": None, "customsClearedAt": None,
            "quarantineDeclNo": None, "quarantineAt": None,
            "isManaged": False,
        }

    if parsed.get("_error"):
        return {"skip": True, "reason": "API 오류: " + parsed["_error"]}
    if parsed.get("_empty"):
        return {"skip": True, "reason": "응답 헤더 없음 (적하목록 미접수)", "process": "미반영"}
    if parsed.get("_multi"):
        return {"skip": True, "reason": "다건 응답 - 보조키(MBL/cargMtNo) 필요"}

    header = parsed["header"]
    history = parsed["history"]

    process = map_process(history, hwaju) or "미반영"

    import_decl = find_history(history, lambda h: norm(h.get("cargTrcnRelaBsopTpcd", "")) == "수입신고수리")
    quar = find_history(history, lambda h: "검사/검역식품의약품(합격)" in norm(h.get("cargTrcnRelaBsopTpcd", "")))

    # 터미널 매핑 (LCL/항공 분리 로직)
    # - LCL: 진행이력에 "반입신고" 행이 2개 이상 → "입항반입" 단계 shedNm = CY (POD 후보),
    #        다른 반입신고 행 shedNm = CFS (LCL 보세창고)
    # - 항공: shedNm이 항공 보세창고 운영사 → POD 매칭 + CFS에 원문 동시 기록
    # - FCL: 헤더 shedNm = CY (기존 동작)
    inbound_count = sum(
        1 for h in history if norm(h.get("cargTrcnRelaBsopTpcd", "")) == "반입신고"
    )
    cy_row = next(
        (h for h in history
         if norm(h.get("cargTrcnRelaBsopTpcd", "")) == "반입신고"
         and "입항반입" in norm(h.get("rlbrCn", ""))),
        None
    )
    cfs_row = next(
        (h for h in history
         if norm(h.get("cargTrcnRelaBsopTpcd", "")) == "반입신고"
         and "입항반입" not in norm(h.get("rlbrCn", ""))),
        None
    )

    header_shed = header.get("shedNm") or ""
    pod_terminal = None
    cfs_warehouse = None

    # 통합 로직: 입항반입(CY) 행을 항상 우선 시도, 헤더는 fallback
    pod_terminal = None
    cfs_warehouse = None
    if cy_row and cy_row.get("shedNm"):
        pod_terminal, _ = match_terminal(cy_row.get("shedNm"))
    if not pod_terminal:
        shed = header_shed
        if not shed and history:
            for h in history:
                if h.get("shedNm"):
                    shed = h.get("shedNm")
                    break
        pod_terminal, _ = match_terminal(shed)

    if inbound_count >= 2 and cfs_row:
        # LCL: 보세운송반입 단계 shedNm을 CFS로
        cfs_warehouse = (cfs_row.get("shedNm") or "").strip() or None
    elif io_type == "항공수입" and pod_terminal:
        # 항공: 보세창고 운영사 원문도 CFS에 기록
        shed = header_shed
        if not shed and history:
            for h in history:
                if h.get("shedNm"):
                    shed = h.get("shedNm"); break
        cfs_warehouse = shed.strip() if shed else None
    elif not pod_terminal:
        # POD 매칭 실패 시 헤더 shedNm 원문을 CFS에 기록
        shed = header_shed
        if not shed and history:
            for h in history:
                if h.get("shedNm"):
                    shed = h.get("shedNm"); break
        cfs_warehouse = shed.strip() if shed else None

    shed_nm = header_shed or (cy_row.get("shedNm", "") if cy_row else "")

    return {
        "skip": False,
        "reason": "정상 매핑" if process != "미반영" else "매핑 가능 단계 없음",
        "process": process,
        "eta": yyyymmdd_to_iso(header.get("etprDt", "")),
        "cargMtNo": header.get("cargMtNo") or None,
        "importDeclNo": (import_decl.get("dclrNo") if import_decl else None) or None,
        "customsClearedAt": yyyymmdd_to_iso((import_decl.get("prcsDttm", "")[:8] if import_decl else "")),
        "quarantineDeclNo": (quar.get("dclrNo") if quar else None) or None,
        "quarantineAt": yyyymmdd_to_iso((quar.get("prcsDttm", "")[:8] if quar else "")),
        "isManaged": header.get("mtTrgtCargYnNm", "") == "Y",
        "shipNm": header.get("shipNm") or None,
        "vydf": header.get("vydf") or None,
        "shedNm": shed_nm or None,
        "podTerminal": pod_terminal,
        "cfsWarehouse": cfs_warehouse,
    }


def decide_search_order(io, type_, hwaju=""):
    """
    검색키 우선순위 결정.

    Returns:
        list of (key_name, key_label) tuples in order to try.
        e.g. [("hbl", "HBL")] or [("mbl", "MBL"), ("hbl", "HBL")]

    정책:
        - 자향 등 HWAJU_MBL_FIRST 화주: 항상 MBL → HBL
        - FCL: MBL → HBL
        - LCL: HBL → MBL (실패 시 fallback)
        - 항공: HBL → MBL
        - TYPE 미지정 해상수입: MBL → HBL
    """
    t = (type_ or "").upper().strip()
    io = (io or "").strip()
    is_mbl_first = any(h in (hwaju or "") for h in HWAJU_MBL_FIRST)
    if io == "해상수입":
        if is_mbl_first:
            return [("mbl", "MBL"), ("hbl", "HBL")]
        if t == "FCL":
            return [("mbl", "MBL"), ("hbl", "HBL")]
        if t == "LCL":
            return [("hbl", "HBL"), ("mbl", "MBL")]  # LCL도 fallback 추가
        return [("mbl", "MBL"), ("hbl", "HBL")]
    if io == "항공수입":
        return [("hbl", "HBL"), ("mbl", "MBL")]
    return [("hbl", "HBL"), ("mbl", "MBL")]


def fetch_with_fallback(api_key, bl_yy, hbl, mbl, io, type_, cargmt=None, hwaju="", debug=False):
    """
    검색 우선순위에 따라 호출하고 첫 valid 응답 반환.

    v2.0 (#1, #4): TBA/빈 BL 사전 필터, 같은 키 재시도(call_unipass에서 처리)
    v2.0 (#3): 다건 응답 시 cargmt 자동 시도 (있으면)
    v2.2: 화주별 검색키 우선순위 (자향 등 MBL 우선)
    """
    order = decide_search_order(io, type_, hwaju=hwaju)
    attempts = []
    multi_seen = False

    for key, label in order:
        value = mbl if key == "mbl" else hbl
        if is_invalid_bl(value):
            attempts.append({"key": label, "value": value, "skip": "TBA/빈 BL"})
            continue
        try:
            xml = call_unipass(api_key, bl_yy,
                               hbl=value if key == "hbl" else None,
                               mbl=value if key == "mbl" else None)
        except Exception as e:
            attempts.append({"key": label, "value": value, "error": str(e)})
            continue
        parsed = parse_response(xml)
        attempts.append({
            "key": label, "value": value, "len": len(xml),
            "valid": "header" in parsed,
            "empty": parsed.get("_empty", False),
            "multi": parsed.get("_multi", False),
        })
        if "header" in parsed:
            return parsed, label, value, attempts, (xml if debug else None)
        if parsed.get("_multi"):
            multi_seen = True
            continue

    # v2.0 (#3): 다건 응답이었거나 모든 시도 실패 — cargMtNo 시도
    if cargmt and not is_invalid_bl(cargmt):
        try:
            xml = call_unipass(api_key, bl_yy, cargmt=cargmt)
            parsed = parse_response(xml)
            attempts.append({
                "key": "CARGMT", "value": cargmt, "len": len(xml),
                "valid": "header" in parsed,
            })
            if "header" in parsed:
                return parsed, "CARGMT", cargmt, attempts, (xml if debug else None)
        except Exception as e:
            attempts.append({"key": "CARGMT", "value": cargmt, "error": str(e)})

    # 모든 시도 실패
    if multi_seen:
        return {"_multi": True, "_ntce": "다건 응답 - 보조키(cargMtNo) 필요"}, None, None, attempts, None
    return {"_empty": True}, None, None, attempts, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hbl", default="", help="HBL 번호")
    ap.add_argument("--mbl", default="", help="MBL 번호")
    ap.add_argument("--cargmt", default="", help="화물관리번호 (다건 응답 시 보조키)")
    ap.add_argument("--bl-yy", required=True, help="BL 년도 (예: 2026)")
    ap.add_argument("--hwaju", default="", help="화주명 (식품검역 분기용)")
    ap.add_argument("--io", required=True, help="I/O 값 (예: 해상수입)")
    ap.add_argument("--type", dest="type_", default="", help="TYPE 값 (FCL/LCL/AIR/특송 등)")
    ap.add_argument("--debug", action="store_true", help="원본 XML/이력 함께 출력")
    args = ap.parse_args()

    api_key = get_api_key()

    # v2.0 (#4): TBA 사전 필터 — HBL/MBL 모두 invalid면 skip
    if is_invalid_bl(args.hbl) and is_invalid_bl(args.mbl) and is_invalid_bl(args.cargmt):
        print(json.dumps({
            "skip": True,
            "reason": f"HBL/MBL/cargMt 모두 invalid (TBA 등): hbl={args.hbl!r} mbl={args.mbl!r}",
        }, ensure_ascii=False))
        sys.exit(0)

    parsed, used_key, used_value, attempts, debug_xml = fetch_with_fallback(
        api_key, args.bl_yy, args.hbl, args.mbl, args.io, args.type_,
        cargmt=args.cargmt, debug=args.debug
    )

    result = build_result(parsed, args.hwaju, args.io)
    result["_searchKey"] = used_key
    result["_searchValue"] = used_value
    if args.debug:
        result["_attempts"] = attempts
        if debug_xml:
            result["_debug_xml_head"] = debug_xml[:500]
        if "history" in parsed:
            result["_debug_stages"] = [
                {"t": h.get("cargTrcnRelaBsopTpcd"), "c": h.get("rlbrCn"), "dt": h.get("prcsDttm")}
                for h in parsed["history"]
            ]

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
