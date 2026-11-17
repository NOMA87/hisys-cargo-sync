#!/usr/bin/env python3
"""Maersk DCSA Commercial Schedules 연결 점검.

사용:
  python scripts/maersk_probe.py                      # 부산(KRPUS) 향후 7일 기항 요약
  python scripts/maersk_probe.py --vessel "MAERSK SEBAROK 545N" --pol CNSHA --pod KRPUS
환경변수: MAERSK_API_KEY
"""
import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from unipass import _maersk_get, fetch_maersk_schedule, maersk_enabled, _best_timestamp  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vessel", help="선명 항차 (예: 'MAERSK SEBAROK 545N')")
    ap.add_argument("--pol", default="CNSHA")
    ap.add_argument("--pod", default="KRPUS")
    ap.add_argument("--port", default="KRPUS", help="--vessel 미지정 시 기항 조회할 UN/LOCODE")
    ap.add_argument("--days", type=int, default=7)
    a = ap.parse_args()

    if not maersk_enabled():
        print("MAERSK_API_KEY 미설정"); sys.exit(1)

    if a.vessel:
        ref = datetime.now().strftime("%Y-%m-%d")
        r = fetch_maersk_schedule(a.pol, a.pod, ref, a.vessel)
        print("RESULT:", r)
        sys.exit(0 if r else 2)

    today = datetime.now()
    svcs = _maersk_get("/vessel-schedules", {
        "UNLocationCode": a.port,
        "startDate": today.strftime("%Y-%m-%d"),
        "endDate": (today + timedelta(days=a.days)).strftime("%Y-%m-%d"),
        "limit": 10,
    })
    if not svcs:
        print("응답 없음 (키/권한/파라미터 확인)"); sys.exit(2)
    print(f"서비스 {len(svcs)}개")
    for svc in svcs:
        print(f"- {svc.get('carrierServiceCode')} {svc.get('carrierServiceName')}")
        for vs in svc.get("vesselSchedules") or []:
            v = vs.get("vessel") or {}
            for c in vs.get("transportCalls") or []:
                loc = c.get("location") or {}
                if (loc.get("UNLocationCode") or "").upper() != a.port.upper():
                    continue
                print(f"    {v.get('name'):24} exp={c.get('carrierExportVoyageNumber'):8} "
                      f"ARRI={_best_timestamp(c.get('timestamps'),'ARRI')} "
                      f"DEPA={_best_timestamp(c.get('timestamps'),'DEPA')} "
                      f"@{loc.get('facilitySMDGCode') or loc.get('locationName')}")


if __name__ == "__main__":
    main()
