#!/usr/bin/env python3
"""
장금상선(Sinokor) e-Service 모선별 스케줄 어댑터 (v1.0)

선사 API가 없어 ebiz.sinokor.co.kr 의 모선별 스케줄 페이지를 그대로 읽는다.
로그인 불필요, 해외 IP에서도 조회됨 (GitHub Actions 러너에서 확인).

엔드포인트:
  POST /Schedule/GetVslList   body: Prefix=<선명 앞부분>   → JSON [{Text:"[PCSZ] PACIFIC SHENZHEN", Value:"PCSZ"}]
  GET  /Schedule/VesselSchedule?vsl=<코드>&yyyymm=YYYY-MM  → HTML (<table id="tblList">)
       행: Vessel/Voyage | Port | Wharf | Arrival | Day | Berth | Day | Departure | Day | Remark

매칭 규칙:
  - 선명 → 선박코드 (GetVslList, 공백 무시 완전일치)
  - POL 기항: 포트명 키워드 일치 + 항차 완전일치 (예: 2636E) → ETD = Departure
    * 장금은 레그마다 항차가 바뀜 (상해 2636E → 평택 2637W). POL 항차만 정확히 맞추고
      POD는 "ETD 이후 첫 기항"으로 잡는다.
  - POD 기항: 포트명 키워드 일치 + Arrival >= ETD 중 가장 빠른 것 → ETA, Wharf → podTerminal
  - 항차가 없으면 기준일(ref_date)에 가장 가까운 POL 출항을 사용

반환 형식은 fetch_hmm_schedule / fetch_maersk_schedule 과 동일:
  {vesselName, voyageNumber, etd, eta, podTerminal, cls:"", source:"SINOKOR"}
  (etd/eta 는 현지시각 'YYYY-MM-DDTHH:MM:SS', tz 미부착 — 호출측에서 get_port_tz 로 부착)

사용법:
  python3 sinokor.py --vessel "PACIFIC SHENZHEN 2636E" --pol CNSHA --pod KRPTK [--ref 2026-11-24]
"""
import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from html import unescape

SINOKOR_BASE = "https://ebiz.sinokor.co.kr"
USER_AGENT = "hisys-cargo-sync/sinokor-1.0"
_VSL_CACHE = {}     # prefix → [(code, name)]
_PAGE_CACHE = {}    # (code, yyyymm) → rows

# UN/LOCODE → 장금 포트명에 포함되는 키워드 (대문자, 공백 제거 후 비교)
SINOKOR_PORT_KEYWORDS = {
    # 한국
    "KRPTK": ("PYONGTAEK", "PYEONGTAEK"),
    "KRINC": ("INCHEON", "INCHON"),
    "KRPUS": ("BUSAN", "PUSAN"),
    "KRKAN": ("KWANGYANG", "GWANGYANG"),
    "KRUSN": ("ULSAN",),
    "KRKUV": ("KUNSAN", "GUNSAN"),
    "KRMAS": ("MASAN",),
    "KRPOH": ("POHANG",),
    # 중국
    "CNSHA": ("SHANGHAI",),
    "CNNGB": ("NINGBO",),
    "CNTAG": ("TAICANG",),
    "CNTZO": ("TAIZHOU",),
    "CNXMN": ("XIAMEN",),
    "CNQIN": ("QINGDAO",),
    "CNTAO": ("QINGDAO",),
    "CNTXG": ("XINGANG", "TIANJIN"),
    "CNTSN": ("XINGANG", "TIANJIN"),
    "CNDAL": ("DALIAN",),
    "CNYTN": ("YANTIAN",),
    "CNSHK": ("SHEKOU",),
    "CNSZX": ("SHEKOU", "SHENZHEN"),
    "CNNSA": ("NANSHA",),
    "CNLYG": ("LIANYUNGANG",),
    "CNYNT": ("YANTAI",),
    "CNWEI": ("WEIHAI",),
    "CNRZH": ("RIZHAO",),
    "CNFOC": ("FUZHOU",),
    "CNZOS": ("ZHOUSHAN",),
    "CNWNZ": ("WENZHOU",),
    "CNNTG": ("NANTONG",),
    "CNZJG": ("ZHANGJIAGANG",),
    "CNJIA": ("JIANGYIN",),
    "CNCGS": ("CHANGSHU",),
    "CNHUA": ("HUANGPU",),
    "CNDCB": ("DACHANBAY", "DACHAN"),
    "CNSWA": ("SHANTOU",),
    "CNHAK": ("HAIKOU",),
    "CNJIU": ("JIUJIANG",),
    "CNWUH": ("WUHAN",),
    "CNLZU": ("LANSHAN",),
    # 일본
    "JPOSA": ("OSAKA",),
    "JPUKB": ("KOBE",),
    "JPTYO": ("TOKYO",),
    "JPYOK": ("YOKOHAMA",),
    "JPNGO": ("NAGOYA",),
    "JPHKT": ("HAKATA",),
    "JPMOJ": ("MOJI",),
    "JPSMZ": ("SHIMIZU",),
    "JPTKY": ("TOKUYAMA",),
    "JPIWK": ("IWAKUNI",),
    "JPKNZ": ("KANAZAWA",),
    "JPTOY": ("TOYAMA",),
    "JPNII": ("NIIGATA",),
    "JPSKT": ("SAKATA",),
    "JPMIZ": ("MIZUSHIMA",),
    "JPHIJ": ("HIROSHIMA",),
    "JPSBS": ("SHIBUSHI",),
    "JPNAH": ("NAHA",),
    "JPKIJ": ("NIIGATA",),
    # 동남아/기타
    "VNSGN": ("HOCHIMINH", "CATLAI", "SAIGON"),
    "VNHPH": ("HAIPHONG",),
    "VNDAD": ("DANANG",),
    "THBKK": ("BANGKOK",),
    "THLCH": ("LAEMCHABANG",),
    "HKHKG": ("HONGKONG",),
    "TWKHH": ("KAOHSIUNG",),
    "TWKEL": ("KEELUNG",),
    "TWTXG": ("TAICHUNG",),
    "PHMNL": ("MANILA",),
    "IDJKT": ("JAKARTA",),
    "IDSUB": ("SURABAYA",),
    "MYPKG": ("PORTKLANG", "PORTKELANG"),
    "MYPEN": ("PENANG",),
    "SGSIN": ("SINGAPORE",),
    "RUVVO": ("VLADIVOSTOK",),
    "RUVYP": ("VOSTOCHNY",),
    "INNSA": ("NHAVASHEVA", "JNPT"),
    "INMAA": ("CHENNAI",),
    "KHPNH": ("PHNOMPENH",),
    "MMRGN": ("YANGON",),
}


def _norm(s):
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


def _http(url, data=None, timeout=25):
    headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    body = None
    if data is not None:
        body = urllib.parse.urlencode(data).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        headers["X-Requested-With"] = "XMLHttpRequest"
    req = urllib.request.Request(url, data=body, headers=headers)
    time.sleep(0.3)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def split_vessel(vessel_name_str):
    """'PACIFIC SHENZHEN 2636E' → ('PACIFIC SHENZHEN', '2636E'). 항차 없으면 ('...', '')."""
    parts = (vessel_name_str or "").strip().split()
    if len(parts) >= 2 and re.search(r"\d", parts[-1]):
        return " ".join(parts[:-1]), parts[-1].upper()
    return " ".join(parts), ""


def lookup_vessel_code(name):
    """선명 → 장금 선박코드. 공백 무시 완전일치, 없으면 None."""
    name = (name or "").strip()
    if not name:
        return None
    prefix = name.split()[0]
    if prefix not in _VSL_CACHE:
        try:
            raw = _http(SINOKOR_BASE + "/Schedule/GetVslList", {"Prefix": prefix})
            items = json.loads(raw)
        except Exception as e:
            print(f"  [SNK-ERR] GetVslList {prefix}: {e}", flush=True)
            items = []
        out = []
        for it in items or []:
            text = it.get("Text") or ""
            m = re.match(r"\[([A-Z0-9]+)\]\s*(.+)$", text.strip())
            if m:
                out.append((m.group(1), m.group(2).strip()))
        _VSL_CACHE[prefix] = out
    want = _norm(name)
    for code, nm in _VSL_CACHE[prefix]:
        if _norm(nm) == want:
            return code
    # 부분일치 fallback (예: 'HUA HANG HAN YA 6' vs 'HUAHANG HANYA 6')
    for code, nm in _VSL_CACHE[prefix]:
        n = _norm(nm)
        if want in n or n in want:
            return code
    return None


_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
_TAG_RE = re.compile(r"<[^>]+>")


def fetch_vessel_rows(code, yyyymm):
    """모선별 스케줄 페이지 → 기항 행 리스트."""
    key = (code, yyyymm)
    if key in _PAGE_CACHE:
        return _PAGE_CACHE[key]
    url = f"{SINOKOR_BASE}/Schedule/VesselSchedule?vsl={urllib.parse.quote(code)}&yyyymm={yyyymm}"
    rows = []
    try:
        html = _http(url)
        i = html.find('id="tblListBody"')
        seg = html[i:] if i >= 0 else html
        j = seg.find("</tbody>")
        seg = seg[:j] if j >= 0 else seg
        for tr in _TR_RE.findall(seg):
            cells = [unescape(_TAG_RE.sub("", c)).strip() for c in _TD_RE.findall(tr)]
            if len(cells) < 8 or "/" not in cells[0]:
                continue
            vsl, _, voy = cells[0].rpartition("/")
            rows.append({
                "vesselName": vsl.strip(),
                "voyage": voy.strip().upper(),
                "port": cells[1],
                "wharf": cells[2],
                "arrival": cells[3],
                "berth": cells[5],
                "departure": cells[7],
                "remark": cells[9] if len(cells) > 9 else "",
            })
        print(f"  [SNK-OK] {code} {yyyymm} rows={len(rows)}", flush=True)
    except Exception as e:
        print(f"  [SNK-ERR] VesselSchedule {code} {yyyymm}: {e}", flush=True)
    _PAGE_CACHE[key] = rows
    return rows


# 노션 표기 변형 → 표준 UN/LOCODE (unipass.PORT_ALIAS 와 동일)
PORT_ALIAS = {
    "KRKWA": "KRKAN", "CNXAM": "CNXMN", "CNNBG": "CNNGB",
    "CNXGA": "CNTXG", "CNTAC": "CNTAG", "CNYAT": "CNYNT",
}


def _std(un_code):
    c = (un_code or "").strip().upper()
    return PORT_ALIAS.get(c, c)


def _port_match(port_text, un_code):
    kws = SINOKOR_PORT_KEYWORDS.get(_std(un_code))
    if not kws:
        return False
    p = _norm(port_text)
    return any(_norm(k) in p for k in kws)


def _iso(s):
    """'2026-11-26 22:00' → '2026-11-26T22:00:00' (없으면 None)."""
    s = (s or "").strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})", s)
    if m:
        return f"{m.group(1)}T{m.group(2)}:00"
    m = re.match(r"(\d{4}-\d{2}-\d{2})$", s)
    return f"{m.group(1)}T00:00:00" if m else None


def _months_around(ref_date):
    """기준일 전월·당월·익월 yyyymm 리스트 (장금 페이지는 월 단위)."""
    try:
        base = datetime.strptime((ref_date or "")[:10], "%Y-%m-%d")
    except Exception:
        base = datetime.now()
    first = base.replace(day=1)
    prev = (first - timedelta(days=1)).replace(day=1)
    nxt = (first + timedelta(days=32)).replace(day=1)
    return [prev.strftime("%Y-%m"), first.strftime("%Y-%m"), nxt.strftime("%Y-%m")], base


def fetch_sinokor_schedule(pol_un, pod_un, ref_date, vessel_name_str):
    """POL 출항(ETD)·POD 도착(ETA) 조회. 매칭 실패 시 None."""
    name, voy = split_vessel(vessel_name_str)
    if not name:
        return None
    code = lookup_vessel_code(name)
    if not code:
        print(f"  [SNK-NOVSL] {name} (GetVslList 미등록)", flush=True)
        return None
    pol_un, pod_un = _std(pol_un), _std(pod_un)
    if pol_un not in SINOKOR_PORT_KEYWORDS or pod_un not in SINOKOR_PORT_KEYWORDS:
        print(f"  [SNK-PORT-UNKNOWN] {pol_un}->{pod_un} (SINOKOR_PORT_KEYWORDS 추가 필요)", flush=True)
        return None

    months, base = _months_around(ref_date)
    seen, rows = set(), []
    for ym in months:
        for r in fetch_vessel_rows(code, ym):
            k = (r["voyage"], r["port"], r["arrival"], r["departure"])
            if k not in seen:
                seen.add(k)
                rows.append(r)
    if not rows:
        return None

    pol_rows = [r for r in rows if _port_match(r["port"], pol_un) and _iso(r["departure"])]
    if voy:
        exact = [r for r in pol_rows if r["voyage"] == voy]
        if not exact:
            cands = ", ".join(f"{r['voyage']}({r['departure'][:10]})" for r in pol_rows[:12])
            print(f"  [SNK-NOVOY] {name} {voy} @ {pol_un} 후보: {cands or '없음'}", flush=True)
            return None
        pol_rows = exact
    if not pol_rows:
        return None
    # 항차 미지정: 기준일에 가장 가까운 출항
    pol_row = min(pol_rows, key=lambda r: abs(
        datetime.strptime(_iso(r["departure"])[:10], "%Y-%m-%d") - base))
    etd = _iso(pol_row["departure"])

    eta, wharf = None, ""
    pod_rows = [r for r in rows if _port_match(r["port"], pod_un) and _iso(r["arrival"])]
    after = sorted((r for r in pod_rows if _iso(r["arrival"]) >= etd), key=lambda r: _iso(r["arrival"]))
    if after:
        eta, wharf = _iso(after[0]["arrival"]), after[0]["wharf"]
    return {
        "vesselName": pol_row["vesselName"],
        "voyageNumber": pol_row["voyage"],
        "etd": etd,
        "eta": eta,
        "podTerminal": wharf,
        "cls": "",
        "source": "SINOKOR",
    }


def main():
    ap = argparse.ArgumentParser(description="장금상선 모선별 스케줄 조회")
    ap.add_argument("--vessel", required=True, help="선명 항차 (예: 'PACIFIC SHENZHEN 2636E')")
    ap.add_argument("--pol", required=True, help="UN/LOCODE (예: CNSHA)")
    ap.add_argument("--pod", required=True, help="UN/LOCODE (예: KRPTK)")
    ap.add_argument("--ref", default="", help="기준일 YYYY-MM-DD (기본 오늘)")
    args = ap.parse_args()
    out = fetch_sinokor_schedule(args.pol, args.pod, args.ref or datetime.now().strftime("%Y-%m-%d"),
                                 args.vessel)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
