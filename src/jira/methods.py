import os
import re
import json
import httpx
import aiofiles
import asyncio
import glob

from datetime import datetime
from pathlib import Path
from typing import Sequence, Mapping, Any, Optional
from src.configuration.config import Settings

settings = Settings()

_token = settings.JIRA_TOKEN
_username = settings.JIRA_USERNAME
_headers = {
  "Accept": "application/json"
}
_auth = httpx.BasicAuth(username=_username,
                        password=_token)
_FILENAME_RE = re.compile(r"filename\*?=([^;]+)", re.I)


async def get_issues_self() -> list[str]:
    url = settings.JIRA_URL_SEARCH_ISSUES
    query_params = {
        "jql": 'assignee = currentUser() AND statusCategory != Done ORDER BY statusCategory, status, updated DESC',
        "maxResults": "100",
        "fields": "renderedFields,description,attachment, status",
        "reconcileIssues": ""
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url=url,
                                        params=query_params,
                                        headers=_headers,
                                        auth=_auth,
                                        timeout=30)

            if response.status_code != 200:
                raise httpx.HTTPError(str(response.status_code))

    except httpx.ConnectTimeout:
        raise Exception("Timeout connecting to JIRA")

    issues_list = response.json()['issues']
    issues_self = [i['self'] for i in issues_list if i['fields']['status']['name'] != 'On pause / Blocked']
    return  issues_self


async def get_issue_data(
        url: str
) -> dict:
    """
    Возвращает объект Jira.
    dict = {id: str, key: str, title: str, description: str, attachmentCount: int, attachment: list[dict],
    issue_link: str}
    """

    query_params = {
        "fields": "description, attachment, summary",
        'expand': 'renderedFields',
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url=url,
                                        params=query_params,
                                        headers=_headers,
                                        auth=_auth)

            if response.status_code != 200:
                raise httpx.HTTPError(message=str(response.status_code))

    except httpx.ConnectTimeout:
        raise Exception("Timeout connecting to JIRA")

    result = json.loads(response.text)

    issue_id = result.get('id')
    issue_key = result.get('key')
    summary = result['fields'].get('summary')
    description = result.get('renderedFields').get('description')
    attachment_count = len(result.get('fields').get('attachment', []))
    attachment = result.get('fields').get('attachment', [])
    issue_link = settings.JIRA_URL_ISSUE_LINK.format(key=issue_key)

    data = {
        'id': issue_id,
        'key': issue_key,
        'title': summary,
        'description': description,
        'attachmentCount': attachment_count,
        'attachment': attachment,
        'issue_link': issue_link
    }
    return  data


def _safe_filename(name: str, fallback: str) -> str:
    name = (name or "").strip().strip('"').strip("'")
    return os.path.basename(name) or fallback


async def _download_with_retries(
    client: httpx.AsyncClient,
    url: str,
    dest_path: Path,
    auth: Optional[httpx.Auth | tuple],
    *,
    max_retries: int = 4,
) -> None:
    attempt = 0
    while True:
        attempt += 1
        try:
            async with client.stream("GET", url, auth=auth, follow_redirects=True) as resp:
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    raise httpx.HTTPStatusError("retryable", request=resp.request, response=resp)
                resp.raise_for_status()

                if os.path.exists(path=dest_path):
                    if dest_path.is_dir():
                        files = glob.glob(os.path.join(dest_path, '*'))
                        for f in files:
                            if os.path.isfile(f):
                                os.remove(f)

                dest_path.parent.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(dest_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(1024 * 1024):
                        if chunk:
                            await f.write(chunk)
                return
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.RemoteProtocolError,
                httpx.NetworkError, httpx.HTTPStatusError) as e:
            retryable = isinstance(e, (httpx.ConnectTimeout, httpx.ReadTimeout,
                                       httpx.RemoteProtocolError, httpx.NetworkError))
            if isinstance(e, httpx.HTTPStatusError):
                code = e.response.status_code
                retryable = (code == 429) or (500 <= code < 600)
            if not retryable or attempt >= max_retries:
                raise Exception('Max retries reached')
            delay = min(8.0, 0.5 * (2 ** (attempt - 1)))
            delay = delay / 2 + delay / 2 * (int.from_bytes(os.urandom(1), "big") / 255)
            await asyncio.sleep(delay)


async def get_issue_attachments(
    attachments: Sequence[Mapping[str, Any]],
    issue_key: str | int
) -> list[Path]:
    if attachments is None:
        return []
    base_url = settings.JIRA_URL_ATTACHMENT_ISSUES
    out_dir = Path("downloads") / str(issue_key)
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    try:
        timeout = httpx.Timeout(connect=10.0, read=120.0, write=120.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            for att in attachments:
                att_id = str(att["id"])
                url = base_url.format(id=att_id)

                f_name = _safe_filename(att.get("filename"), fallback=f"{att_id}.bin")
                dest = out_dir / f_name

                await _download_with_retries(client=client, url=url, dest_path=dest, auth=_auth)
                saved.append(dest)

    except Exception as e:
        raise Exception(f"Error occurred: {e}")

    return saved


def _parse_jira_dt(s: str) -> datetime:
    """
    Примеры JIRA: '2025-12-01T18:22:33.456+0000' или '2025-12-01T18:22:33+0000'
    Преобразуем '+0000' -> '+00:00' и парсим через fromisoformat.
    """
    if not s:
        return datetime.min.replace(tzinfo=None)
    # добавим двоеточие в смещение таймзоны
    if len(s) >= 5 and (s[-5] in "+-") and s[-3] != ":":
        s = s[:-2] + ":" + s[-2:]
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # запасной вариант с миллисекундами/без
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(s.replace(":", "", 1), fmt)  # редко нужно
            except ValueError:
                pass
        return datetime.min


async def get_issue_comments(issue_id: str | int) -> list[dict[str, Any]]:
    """
    Возвращает список комментариев, отсортированный по created (ASC).
    Элемент списка: {id, issue_id, author, created, description, _created_dt}
    """
    base_url = settings.JIRA_URL_GET_COMMENTS.format(id=str(issue_id))
    start_at, max_results = 0, 100
    rows: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        while True:
            params = {
                "expand": "renderedBody",
                "startAt": start_at,
                "maxResults": max_results
            }
            resp = await client.get(base_url,
                                    params=params,
                                    headers=_headers,
                                    auth=_auth)
            resp.raise_for_status()
            data = resp.json()

            comments = data.get("comments") or []
            if not comments:
                return []
            for c in comments:
                created = c.get("created") or ""
                rows.append({
                    "id": str(c.get("id", "")),
                    "issue_id": issue_id,
                    "author": (c.get("author") or {}).get("displayName") or "",
                    "created": created,
                    "description": c.get("renderedBody"),
                    "_created_dt": _parse_jira_dt(created),  # для сортировки
                })

            total = int(data.get("total", 0))
            start_at += len(comments)
            if start_at >= total or not comments:
                break

    rows.sort(key=lambda x: x["_created_dt"])
    for r in rows:
        r.pop("_created_dt", None)
    return rows

