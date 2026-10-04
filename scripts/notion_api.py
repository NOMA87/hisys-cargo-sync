#!/usr/bin/env python3
"""
노션 API 공용 헬퍼 (sync_runner.py / kmtc_sync.py 공유)

- notion_request      : REST 호출 (HTTP 오류 시 응답 본문 포함 RuntimeError)
- query_data_source   : data source query + 페이지네이션
- extract_prop        : page properties → 값 추출
- update_page         : properties PATCH
- normalize_ds_id     : 하이픈 유무 무관하게 UUID 형태로 정규화
"""
import json
import os
import sys
import urllib.error
import urllib.request

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


def normalize_ds_id(raw=None):
    """NOTION_DS_ID(또는 인자)를 8-4-4-4-12 UUID 형태로 복원."""
    s = (raw or os.environ.get("NOTION_DS_ID", DEFAULT_DS_ID)).replace("-", "")
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


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
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Notion API {method} {path} → {e.code}: {detail}")


def query_data_source(token, ds_id, filter_, sorts=None, page_size=100):
    """data source query 전체 페이지 수집."""
    body_template = {"filter": filter_, "page_size": page_size}
    if sorts:
        body_template["sorts"] = sorts
    pages = []
    cursor = None
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


def _plain_text(arr):
    return "".join(t.get("plain_text", "") for t in arr) or None


def extract_prop(props, name, kind):
    """노션 page properties에서 값 추출. 없거나 미지원 kind면 None."""
    p = props.get(name)
    if not p:
        return None
    if kind in ("title", "rich_text"):
        return _plain_text(p.get(kind, []))
    if kind in ("select", "status"):
        s = p.get(kind)
        return s.get("name") if s else None
    if kind == "date":
        d = p.get("date")
        return d.get("start") if d else None
    if kind == "checkbox":
        return p.get("checkbox", False)
    if kind == "relation":
        return [r.get("id") for r in p.get("relation", [])]
    return None


def update_page(token, page_id, properties):
    return notion_request("PATCH", f"/pages/{page_id}", token, {"properties": properties})
