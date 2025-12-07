import re
from redis.asyncio import Redis
from typing import Optional, Any


ID_RE = re.compile(r"^issue:(\d+)$")

UPSERT_J_ISSUE_LUA = """
local key = KEYS[1]
local new_desc = ARGV[1]
local new_att  = ARGV[2] or ""
local changed = {}
if redis.call('EXISTS', key) == 0 then
  local now = redis.call('TIME')[1]
  redis.call('HSET', key, 'created_at', now)
end
local old_desc = redis.call('HGET', key, 'h_description')
local old_att  = redis.call('HGET', key, 'h_attachment') or ""
if (not old_desc) or (old_desc ~= new_desc) then
  redis.call('HSET', key, 'h_description', new_desc)
  table.insert(changed, 'desc')
end
if old_att ~= new_att then
  redis.call('HSET', key, 'h_attachment', new_att)
  table.insert(changed, 'attach')
end
if #changed > 0 then
  redis.call('HSET', key, 'updated_at', redis.call('TIME')[1])
end
return changed
"""

UPSERT_LINK_LUA = """
-- KEYS[1] = issue_link:{j_issue}
-- ARGV[1] = j_issue (as string)
-- ARGV[2] = p_issue (as string)
local key = KEYS[1]
local j = ARGV[1]
local p = ARGV[2]
local is_new = 0
if redis.call('EXISTS', key) == 0 then
  local now = redis.call('TIME')[1]
  redis.call('HSET', key, 'created_at', now)
  is_new = 1
end
redis.call('HSET', key, 'j_issue', j, 'p_issue', p)
return is_new
"""

UPSERT_COMMENT_LUA = """
local key = KEYS[1]
local j = ARGV[1]
local p = ARGV[2]
local p_issue = ARGV[3]
local desc = ARGV[4]
local is_new = 0

if redis.call('EXISTS', key) == 0 then
  local now = redis.call('TIME')[1]
  redis.call('HSET', key, 'created_at', now)
  is_new = 1
end

local old_desc = redis.call('HGET', key, 'h_description') or ""
if old_desc ~= desc then
  redis.call('HSET', key, 'h_description', desc)
end

redis.call('HSET', key, 'comment_j_id', j, 'comment_p_id', p, 'p_issue_id', p_issue)

return is_new
"""

async def prepare_scripts(r: Redis, lua_scripts: str):
    return r.register_script(lua_scripts)


async def get_issue(r: Redis, issue_id: int) -> Optional[dict[str, str]]:
    """
    Объект jira.
    Возвращает dict{issue_id: int, h_description: str, h_attachment: str, created_at: str}.
    """
    data = await r.hgetall(f"issue:{issue_id}")
    if not data:
        return None
    out: dict[str, Any] = {"issue_id": issue_id}
    if "h_description" in data:   out["h_description"]   = data["h_description"]
    if "h_attachment" in data:   out["h_attachment"]   = data["h_attachment"]
    if "created_at" in data: out["created_at"] = data["created_at"]
    return out


async def upsert_issue_hash(
    r: Redis,
    issue_id: int,
    h_description: str,
    h_attachment: str | None
) -> bool:
    """
    Записывает/обновляет связку. Возвращает True, если запись новая (created), иначе False.
    """
    upsert_lua = await prepare_scripts(r, UPSERT_J_ISSUE_LUA)
    key = f"issue:{issue_id}"
    is_new = await upsert_lua(keys=[key], args=[h_description, h_attachment or ""])
    return bool(is_new)


async def upsert_issue_link(
        r: Redis,
        j_issue: int,
        p_issue: int
):
    """
    Связка jira_id:planfix_id
    Записывает/обновляет связку. Возвращает True, если запись новая (created), иначе False.
    """
    upsert_lua = await prepare_scripts(r, UPSERT_LINK_LUA)
    key = f"issue_link:{j_issue}"
    await upsert_lua(keys=[key], args=[str(int(j_issue)), str(int(p_issue))])


async def get_issue_link(r: Redis, j_issue: int) -> Optional[dict[str, str]]:
    """
    Связка jira_id:planfix_id
    Возвращает dict{j_issue: int, p_issue: int}.
    """
    data = await r.hgetall(f"issue_link:{j_issue}")
    if not data:
        return None
    out: dict[str, Any] = {"j_issue": j_issue}
    if "p_issue" in data:   out["p_issue"]   = int(data["p_issue"])
    if "created_at" in data: out["created_at"] = data["created_at"]
    return out


async def list_issue_ids(r: Redis, batch: int = 500) -> list[int]:
    """
    Объект jira.
    Возвращает list[id].
    """
    ids: list[int] = []
    async for key in r.scan_iter(match="issue:[0-9]*", count=batch):
        m = ID_RE.match(key)
        if m:
            ids.append(int(m.group(1)))
    return ids


async def upsert_comment(
        r: Redis,
        comment_j_id: int,
        comment_p_id: int | str,
        p_issue_id: int | str,
        h_description: str
):
    """
    Связка comment_j_id:comment_p_id:p_issue_id
    Записывает/обновляет связку комментариев. Возвращает True, если запись новая (created), иначе False.
    """
    upsert_lua = await prepare_scripts(r, UPSERT_COMMENT_LUA)  # получаем Lua-скрипт
    key = f"comment_link:{comment_j_id}"

    # Запускаем скрипт
    is_new = await upsert_lua(keys=[key], args=[str(comment_j_id), str(comment_p_id), str(p_issue_id), h_description])

    return bool(is_new)


async def get_comment(r: Redis, comment_j_id: int) -> Optional[dict[str, str]]:
    """
    Возвращает dict{comment_j_id: int, comment_p_id: int, p_issue_id: int, h_description: str}.
    """
    data = await r.hgetall(f"comment_link:{comment_j_id}")
    if not data:
        return None
    out: dict[str, Any] = {"comment_j_id": comment_j_id}

    if "comment_p_id" in data:   out["comment_p_id"]   = int(data["comment_p_id"])
    if "p_issue_id" in data  :   out["p_issue_id"]     = int(data["p_issue_id"])
    if "h_description" in data : out["h_description"]  = data["h_description"]
    return out
