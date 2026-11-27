#!/usr/bin/env python3
"""
COSCO SHIPPING Lines COP(개방 플랫폼) 스케줄 어댑터 (v1.0)

HMAC-SHA1 서명 방식 (COP SDK python/cop.py 와 동일):
  X-Coscon-Date          GMT  "%a, %d %b %Y %H:%M:%S GMT"
  X-Coscon-Content-Md5   md5(uuid)  — 임의값, 서명에만 사용
  X-Coscon-Digest        "SHA-256=" + base64(sha256(body))
  X-Coscon-Authorization hmac username="<APP KEY>",algorithm="hmac-sha1",
                         headers="X-Coscon-Date X-Coscon-Digest X-Coscon-Content-Md5 request-line",
                         signature="<base64 HMAC-SHA1(secret, signStr)>"
  signStr = "X-Coscon-Date: {date}\nX-Coscon-Digest: {digest}\nX-Coscon-Content-Md5: {md5}\n{METHOD} {path} HTTP/1.1"

엔드포인트 (요청 path 는 /service 로 시작):
  GET  https://elines.coscoshipping.com/ebbase/public/general/findCityDistrictByPrefix?prefix=Busan  (공개, 인증 불필요)
       → data.content[] {cityUuid, cityLocName, county, state, country, unloCode, cityCode}
  POST /service/locations/cities   (COP 도시검색 — 별도 구독 필요, 미구독 시 403 → e-Lines 공개검색으로 대체)
  POST /service/info/schedule      {pickup:"B", delivery:"B", estimateDate:"D", originCity, originCityUuid,
                                    destinationCity, destinationCityUuid, fromDate:"YYYY-MM-DD", dateLength:<주>}
       → data.content.data[] {firstPol, lastPod, transitTime, schedules[] {vessel, voyage, dir, extVoyage, pol, etd, pod, eta, cutOff, podFacilityCode, service}}

환경변수:
  COSCO_APP_KEY, COSCO_SECRET_KEY  (필수 — GitHub Secrets)
  COSCO_ENV = prod(기본) | uat      (uat → api-pp.lines.coscoshipping.com)

매칭 규칙:
  - 선명(공백/기호 제거, 대문자) 부분일치 + 항차(extVoyage 또는 voyage+dir) 부분일치
  - 항차가 없으면 기준일(ref_date)에 가장 가까운 ETD
  - 환적 경로면 첫 구간 ETD, 마지막 구간 ETA. POD 터미널 = 마지막 구간 podFacilityCode

반환 형식은 fetch_hmm_schedule / fetch_sinokor_schedule 과 동일:
  {vesselName, voyageNumber, etd, eta, podTerminal, cls, source:"COSCO"}
  (etd/eta 는 현지시각 'YYYY-MM-DDTHH:MM:SS', tz 미부착 — 호출측에서 get_port_tz 로 부착)

사용법:
  COSCO_APP_KEY=… COSCO_SECRET_KEY=… python3 cosco.py --vessel "COSCO SHIPPING ARIES 051E" --pol CNSHA --pod KRPUS [--ref 2026-11-27]
  python3 cosco.py --city KRPUS            # 도시 UUID 조회만
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

COSCO_BASE = {
    "prod": "https://api.lines.coscoshipping.com",
    "uat": "https://api-pp.lines.coscoshipping.com",
}
USER_AGENT = "hisys-cargo-sync/cosco-1.0"
SCHEDULE_WEEKS = 4          # dateLength (주)
FROM_LEAD_DAYS = 10         # fromDate = 기준일 - 10일

_CITY_CACHE = {}            # UN/LOCODE → (uuid, cityString)
_SCHED_CACHE = {}           # (pol, pod, fromDate) → routes

# UN/LOCODE → (COSCO locationName, countryName)
# countryName 은 COP 응답의 countryName 과 완전일치해야 함 (대소문자 무시)
COSCO_CITY = {
    # 한국
    "KRPUS": ("Busan", "South Korea"),
    "KRINC": ("Incheon", "South Korea"),
    "KRPTK": ("Pyeongtaek", "South Korea"),
    "KRKAN": ("Gwangyang", "South Korea"),
    "KRUSN": ("Ulsan", "South Korea"),
    # 중국
    "CNSHA": ("Shanghai", "China"),
    "CNNGB": ("Ningbo", "China"),
    "CNQIN": ("Qingdao", "China"),
    "CNTXG": ("Tianjin", "China"),
    "CNXMN": ("Xiamen", "China"),
    "CNSZX": ("Shenzhen", "China"),
    "CNYTN": ("Yantian", "China"),
    "CNSHK": ("Shekou", "China"),
    "CNDLC": ("Dalian", "China"),
    "CNTAO": ("Qingdao", "China"),
    "CNNSA": ("Nansha", "China"),
    "CNCAN": ("Guangzhou", "China"),
    "CNLYG": ("Lianyungang", "China"),
    "CNYNT": ("Yantai", "China"),
    "CNWEI": ("Weihai", "China"),
    "CNFOC": ("Fuzhou", "China"),
    "CNTAG": ("Taicang", "China"),
    "CNNTG": ("Nantong", "China"),
    "CNZJG": ("Zhangjiagang", "China"),
    # 동남아/기타
    "VNSGN": ("Ho Chi Minh City", "Vietnam"),
    "VNHPH": ("Haiphong", "Vietnam"),
    "THLCH": ("Laem Chabang", "Thailand"),
    "THBKK": ("Bangkok", "Thailand"),
    "MYPKG": ("Port Klang", "Malaysia"),
    "SGSIN": ("Singapore", "Singapore"),
    "IDJKT": ("Jakarta", "Indonesia"),
    "PHMNL": ("Manila", "Philippines"),
    "TWKHH": ("Kaohsiung", "Taiwan, China"),
    "TWKEL": ("Keelung", "Taiwan, China"),
    "TWTPE": ("Taipei", "Taiwan, China"),
    "HKHKG": ("Hong Kong", "Hong Kong, China"),
    "JPTYO": ("Tokyo", "Japan"),
    "JPYOK": ("Yokohama", "Japan"),
    "JPUKB": ("Kobe", "Japan"),
    "JPOSA": ("Osaka", "Japan"),
    "JPNGO": ("Nagoya", "Japan"),
    "INNSA": ("Nhava Sheva", "India"),
    "INMUN": ("Mundra", "India"),
    "AEJEA": ("Jebel Ali", "United Arab Emirates"),
    # 미주/유럽
    "USLAX": ("Los Angeles", "United States"),
    "USLGB": ("Long Beach", "United States"),
    "USNYC": ("New York", "United States"),
    "USSEA": ("Seattle", "United States"),
    "USOAK": ("Oakland", "United States"),
    "USSAV": ("Savannah", "United States"),
    "USHOU": ("Houston", "United States"),
    "CAVAN": ("Vancouver", "Canada"),
    "NLRTM": ("Rotterdam", "Netherlands"),
    "DEHAM": ("Hamburg", "Germany"),
    "BEANR": ("Antwerp", "Belgium"),
    "GBFXT": ("Felixstowe", "United Kingdom"),
}
PORT_ALIAS = {
    "KRKWA": "KRKAN", "CNXAM": "CNXMN", "CNNBG": "CNNGB",
    "CNXGA": "CNTXG", "CNTAC": "CNTAG", "CNYAT": "CNYNT",
}


def cosco_enabled():
    return bool(os.environ.get("COSCO_APP_KEY", "").strip()
                and os.environ.get("COSCO_SECRET_KEY", "").strip())


def _base():
    env = os.environ.get("COSCO_ENV", "prod").strip().lower()
    return COSCO_BASE.get(env, COSCO_BASE["prod"])


def _norm(s):
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


def _std(un_code):
    c = (un_code or "").strip().upper()
    return PORT_ALIAS.get(c, c)


def hmac_headers(method, path, body_str, app_key=None, secret=None):
    """COP HMAC 서명 헤더 생성. path 는 '/service/...' 전체 경로."""
    app_key = app_key or os.environ.get("COSCO_APP_KEY", "").strip()
    secret = secret or os.environ.get("COSCO_SECRET_KEY", "").strip()
    date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    content_md5 = hashlib.md5(uuid.uuid1().hex.encode("utf-8")).hexdigest()
    digest = "SHA-256=" + base64.b64encode(hashlib.sha256(body_str.encode("utf-8")).digest()).decode()
    sign_str = (f"X-Coscon-Date: {date}\n"
                f"X-Coscon-Digest: {digest}\n"
                f"X-Coscon-Content-Md5: {content_md5}\n"
                f"{method} {path} HTTP/1.1")
    sig = base64.b64encode(hmac.new(secret.encode("utf-8"), sign_str.encode("utf-8"), "sha1").digest()).decode()
    auth = (f'hmac username="{app_key}",algorithm="hmac-sha1",'
            f'headers="X-Coscon-Date X-Coscon-Digest X-Coscon-Content-Md5 request-line",'
            f'signature="{sig}"')
    return {
        "X-Coscon-Date": date,
        "X-Coscon-Digest": digest,
        "X-Coscon-Content-Md5": content_md5,
        "X-Coscon-Authorization": auth,
    }


def _call(method, path, body=None, timeout=30):
    """서명된 COP 호출 → JSON dict. HTTP 오류는 예외로 올림."""
    if not cosco_enabled():
        raise RuntimeError("COSCO_APP_KEY / COSCO_SECRET_KEY 미설정")
    body_str = json.dumps(body, ensure_ascii=False, separators=(",", ":")) if body is not None else ""
    headers = hmac_headers(method, path, body_str)
    headers.update({
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    })
    req = urllib.request.Request(_base() + path, data=body_str.encode("utf-8") if body is not None else None,
                                 headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        raise RuntimeError(f"COSCO HTTP {e.code} {path}: {detail}") from None
    try:
        return json.loads(raw)
    except ValueError:
        raise RuntimeError(f"COSCO 비JSON 응답 {path}: {raw[:200]}")


ELINES_CITY_URL = "https://elines.coscoshipping.com/ebbase/public/general/findCityDistrictByPrefix"


def _elines_cities(prefix):
    """COSCO e-Lines 공개 도시검색 (인증 불필요) → rows[{cityUuid, cityLocName, county, state, country, unloCode, cityCode}]"""
    url = ELINES_CITY_URL + "?" + urllib.parse.urlencode({"prefix": prefix})
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=25) as r:
        res = json.loads(r.read().decode("utf-8", "replace"))
    return ((res.get("data") or {}).get("content")) or []


def _country_ok(row_country, want):
    rc, w = _norm(row_country), _norm(want)
    return w in rc or rc in w or (w == "KOREA" and "KOREA" in rc)


def lookup_city(un_code):
    """UN/LOCODE → (uuid, 'City,County,State,Country,UNLOCODE'). 실패 시 (None, None).

    1) e-Lines 공개 검색(findCityDistrictByPrefix) — unloCode 일치 > cityCode(뒤 3자리)+국가 일치 > 도시명 완전일치
    2) 실패 시 COP /service/locations/cities (구독된 경우에만)
    """
    un = _std(un_code)
    if un in _CITY_CACHE:
        return _CITY_CACHE[un]
    if un not in COSCO_CITY:
        print(f"  [COSCO-PORT-UNKNOWN] {un} (COSCO_CITY 추가 필요)", flush=True)
        _CITY_CACHE[un] = (None, None)
        return _CITY_CACHE[un]
    name, country = COSCO_CITY[un]
    pick = None
    try:
        rows = _elines_cities(name)
        rows = [r for r in rows if not r.get("districtUuid") and _country_ok(r.get("country"), country)]
        pick = (next((r for r in rows if _norm(r.get("unloCode")) == un), None)
                or next((r for r in rows if _norm(r.get("cityCode")) == un[2:] and _norm(r.get("cityLocName")) == _norm(name)), None)
                or next((r for r in rows if _norm(r.get("cityCode")) == un[2:]), None)
                or next((r for r in rows if _norm(r.get("cityLocName")) == _norm(name)), None))
    except Exception as e:
        print(f"  [COSCO-CITY-ERR] e-Lines {un}: {e}", flush=True)
    if pick:
        uid = str(pick["cityUuid"])
        city_str = ",".join([pick.get("cityLocName") or name, (pick.get("county") or "").strip() or (pick.get("cityLocName") or name),
                             (pick.get("state") or "").strip(), (pick.get("country") or country).strip(), un])
        _CITY_CACHE[un] = (uid, city_str)
        return _CITY_CACHE[un]
    # fallback: COP 도시검색 API (구독 필요 — 미구독 시 403 "You cannot consume this service")
    try:
        res = _call("POST", "/service/locations/cities", {"locationName": name, "countryName": country})
        rows = res.get("data") if isinstance(res.get("data"), list) else []
        exact = [r for r in rows if _norm(r.get("cityName")) == _norm(name)]
        pick = next((r for r in exact if not r.get("cityDistrictName")), None) or (exact[0] if exact else None)
    except Exception as e:
        print(f"  [COSCO-CITY-ERR] COP {un}: {str(e)[:120]}", flush=True)
        pick = None
    if not pick:
        print(f"  [COSCO-NOCITY] {un} {name}/{country}", flush=True)
        _CITY_CACHE[un] = (None, None)
        return _CITY_CACHE[un]
    uid = str(pick.get("locationId") or pick.get("cityId"))
    city_str = ",".join([pick.get("cityName") or name, pick.get("countyName") or pick.get("cityName") or name,
                         pick.get("stateName") or "", pick.get("countryName") or country, un])
    _CITY_CACHE[un] = (uid, city_str)
    return _CITY_CACHE[un]


def fetch_routes(pol_un, pod_un, from_date, weeks=SCHEDULE_WEEKS):
    """경로 목록(data.content.data[]) 반환. 실행 중 캐싱."""
    key = (pol_un, pod_un, from_date)
    if key in _SCHED_CACHE:
        return _SCHED_CACHE[key]
    o_uuid, o_city = lookup_city(pol_un)
    d_uuid, d_city = lookup_city(pod_un)
    if not o_uuid or not d_uuid:
        _SCHED_CACHE[key] = []
        return []
    body = {
        "pickup": "B", "delivery": "B", "estimateDate": "D",
        "originCity": o_city, "originCityUuid": o_uuid,
        "destinationCity": d_city, "destinationCityUuid": d_uuid,
        "fromDate": from_date, "dateLength": str(weeks),
    }
    res = _call("POST", "/service/info/schedule", body)
    if str(res.get("code")) not in ("0", "200"):
        raise RuntimeError(f"COSCO schedule code={res.get('code')} {res.get('message')}")
    routes = (((res.get("data") or {}).get("content") or {}).get("data")) or []
    _SCHED_CACHE[key] = routes
    return routes


def _iso(s):
    """'2018-12-21' / '2018-12-21 12:00' → 'YYYY-MM-DDTHH:MM:SS'."""
    m = re.match(r"(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}))?", s or "")
    if not m:
        return None
    return f"{m.group(1)}T{m.group(2) or '00:00'}:00"


def split_vessel(vessel_name_str):
    parts = (vessel_name_str or "").strip().split()
    if len(parts) >= 2 and re.search(r"\d", parts[-1]):
        return " ".join(parts[:-1]), parts[-1].upper()
    return " ".join(parts), ""


def _leg_voyages(leg):
    out = set()
    for v in (leg.get("extVoyage"), leg.get("newExtVoyage"),
              f"{leg.get('voyage') or ''}{leg.get('dir') or ''}", leg.get("voyage")):
        if v:
            out.add(_norm(v))
    return out


def fetch_cosco_schedule(pol_un, pod_un, ref_date, vessel_name_str):
    """POL 출항(ETD)·POD 도착(ETA) 조회. 매칭 실패 시 None."""
    if not cosco_enabled():
        return None
    name, voy = split_vessel(vessel_name_str)
    if not name:
        return None
    pol_un, pod_un = _std(pol_un), _std(pod_un)
    if isinstance(ref_date, datetime):
        ref_date = ref_date.date()
    base = ref_date or datetime.now(timezone.utc).date()
    from_date = (base - timedelta(days=FROM_LEAD_DAYS)).strftime("%Y-%m-%d")
    routes = fetch_routes(pol_un, pod_un, from_date)
    if not routes:
        return None

    n_name, n_voy = _norm(name), _norm(voy)
    cands = []   # (leg, route)
    for route in routes:
        legs = route.get("schedules") or []
        if not legs:
            continue
        first = legs[0]
        if n_name not in _norm(first.get("vessel")):
            continue
        if n_voy and not any(n_voy in v or v in n_voy for v in _leg_voyages(first) if v):
            continue
        cands.append((first, route))
    if not cands:
        seen = ", ".join(
            f"{(r.get('schedules') or [{}])[0].get('vessel','')} "
            f"{(r.get('schedules') or [{}])[0].get('extVoyage','')}({(r.get('schedules') or [{}])[0].get('etd','')})"
            for r in routes[:12])
        print(f"  [COSCO-NOMATCH] {name} {voy} @ {pol_un}->{pod_un} 후보: {seen or '없음'}", flush=True)
        return None

    def _dist(item):
        d = _iso(item[0].get("etd"))
        if not d:
            return 10**6
        return abs((datetime.strptime(d[:10], "%Y-%m-%d").date() - base).days)

    first, route = min(cands, key=_dist)
    last = (route.get("schedules") or [first])[-1]
    return {
        "vesselName": (first.get("vessel") or name).strip(),
        "voyageNumber": first.get("extVoyage") or f"{first.get('voyage') or ''}{first.get('dir') or ''}" or voy,
        "etd": _iso(first.get("etd")),
        "eta": _iso(last.get("eta")),
        "podTerminal": last.get("podFacilityCode") or "",
        "cls": _iso(first.get("cutOff")) or "",
        "service": first.get("service") or "",
        "transship": len(route.get("schedules") or []) > 1,
        "source": "COSCO",
    }


def main():
    ap = argparse.ArgumentParser(description="COSCO COP 스케줄 조회")
    ap.add_argument("--vessel", help="선명 항차 (예: 'COSCO SHIPPING ARIES 051E')")
    ap.add_argument("--pol", help="UN/LOCODE (예: CNSHA)")
    ap.add_argument("--pod", help="UN/LOCODE (예: KRPUS)")
    ap.add_argument("--ref", help="기준일 YYYY-MM-DD (기본 오늘)")
    ap.add_argument("--city", help="도시 UUID 조회만 (UN/LOCODE)")
    ap.add_argument("--raw", action="store_true", help="경로 원본 JSON 출력")
    a = ap.parse_args()
    if not cosco_enabled():
        sys.exit("COSCO_APP_KEY / COSCO_SECRET_KEY 환경변수 필요")
    if a.city:
        print(lookup_city(a.city))
        return
    if not (a.vessel and a.pol and a.pod):
        ap.error("--vessel --pol --pod 필요 (또는 --city)")
    ref = datetime.strptime(a.ref, "%Y-%m-%d").date() if a.ref else datetime.now(timezone.utc).date()
    if a.raw:
        from_date = (ref - timedelta(days=FROM_LEAD_DAYS)).strftime("%Y-%m-%d")
        print(json.dumps(fetch_routes(_std(a.pol), _std(a.pod), from_date), ensure_ascii=False, indent=1)[:6000])
    r = fetch_cosco_schedule(a.pol, a.pod, ref, a.vessel)
    print(json.dumps(r, ensure_ascii=False, indent=2) if r else "NOMATCH")


if __name__ == "__main__":
    main()
