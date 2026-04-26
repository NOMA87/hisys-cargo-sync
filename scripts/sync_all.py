#!/usr/bin/env python3
"""
v2.0 (#7): 일괄 동기화 스크립트.

unipass.py가 단건 조회라면 sync_all.py는 차수 list를 입력받아 일괄 처리.

입력 (JSON 파일 또는 stdin):
[
  {"pageId": "...", "차수": "...", "hbl": "...", "mbl": "...",
   "io": "해상수입", "type": "FCL", "blYy": "2026",
   "hwaju": "...", "cargmt": "..."},
  ...
]

출력 (JSON, stdout):
{
  "stats": {
    "total": N, "success": N, "skipped": N, "errored": N,
    "managed": N,  # 관리대상화물
    "no_response": N
  },
  "results": [{"pageId": ..., "chasu": ..., "result": {...}}, ...],
  "unmatched_sheds": [...]   # v2.0 (#6) 매칭 실패 shedNm 모음
}

사용법:
    python3 sync_all.py --input cases.json
    cat cases.json | python3 sync_all.py
"""
import argparse
import json
import sys
import os
import time

# unipass.py를 같은 디렉토리에서 import
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from unipass import (
    get_api_key, fetch_with_fallback, build_result, is_invalid_bl
)


def process_one(case, api_key, debug=False):
    """단건 처리 → 결과 dict."""
    if is_invalid_bl(case.get("hbl")) and is_invalid_bl(case.get("mbl")):
        return {
            "pageId": case.get("pageId"),
            "chasu": case.get("chasu"),
            "skip": True,
            "reason": "HBL/MBL 모두 invalid (TBA 등)",
        }
    parsed, used_key, used_value, attempts, _ = fetch_with_fallback(
        api_key, case.get("blYy", ""),
        case.get("hbl", ""), case.get("mbl", ""),
        case.get("io", ""), case.get("type", ""),
        cargmt=case.get("cargmt", ""),
        debug=debug,
    )
    result = build_result(parsed, case.get("hwaju", ""), case.get("io", ""))
    result["_searchKey"] = used_key
    result["_searchValue"] = used_value
    if debug:
        result["_attempts"] = attempts
    return {
        "pageId": case.get("pageId"),
        "chasu": case.get("chasu"),
        "result": result,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="", help="JSON 파일 경로 (없으면 stdin)")
    ap.add_argument("--throttle", type=float, default=0.5,
                    help="호출 간 sleep 초 (기본 0.5초). API 레이트 제한 회피")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            cases = json.load(f)
    else:
        cases = json.load(sys.stdin)

    api_key = get_api_key()
    results = []
    stats = {"total": len(cases), "success": 0, "skipped": 0,
             "errored": 0, "managed": 0, "no_response": 0,
             "multi": 0}
    unmatched_sheds = {}  # shedNm → [차수 list]

    for i, case in enumerate(cases):
        try:
            out = process_one(case, api_key, debug=args.debug)
        except Exception as e:
            out = {"pageId": case.get("pageId"), "chasu": case.get("chasu"),
                   "error": str(e)}
        results.append(out)

        r = out.get("result", {})
        if out.get("error"):
            stats["errored"] += 1
        elif out.get("skip"):
            stats["skipped"] += 1
        elif r.get("skip"):
            stats["skipped"] += 1
            reason = r.get("reason", "")
            if "응답 헤더 없음" in reason or "적핟목록" in reason:
                stats["no_response"] += 1
            elif "다건" in reason:
                stats["multi"] += 1
        else:
            stats["success"] += 1
            if r.get("isManaged"):
                stats["managed"] += 1
            # v2.0 (#6): 매칭 실패 shedNm 수집
            if not r.get("podTerminal") and r.get("cfsWarehouse"):
                shed = r["cfsWarehouse"]
                unmatched_sheds.setdefault(shed, []).append(case.get("chasu"))

        # progress (stderr)
        sys.stderr.write(f"[{i+1}/{len(cases)}] {case.get('chasu')} ... done\n")
        sys.stderr.flush()

        if args.throttle > 0 and i < len(cases) - 1:
            time.sleep(args.throttle)

    output = {
        "stats": stats,
        "results": results,
        "unmatched_sheds": [
            {"shed": s, "count": len(cs), "차수": cs}
            for s, cs in sorted(unmatched_sheds.items(), key=lambda x: -len(x[1]))
        ],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
