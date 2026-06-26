# ============================================================
# COMPLETE api/index.py – NexThread for Vercel
# ============================================================
import os
import json
import asyncio
import bcrypt
import jwt
import uuid
import html
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Request, UploadFile, File, Form, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, EmailStr, Field
import aiofiles

# ---------- ENVIRONMENT ----------
IS_VERCEL = os.environ.get("VERCEL") == "1" or os.environ.get("NOW_REGION") is not None

BASE_DIR = Path(__file__).resolve().parent.parent

if IS_VERCEL:
    DATA_DIR = Path("/tmp") / "nexthread_data"
    UPLOAD_DIR = Path("/tmp") / "nexthread_uploads"
    LOG_DIR = Path("/tmp") / "nexthread_logs"
else:
    DATA_DIR = BASE_DIR / "data"
    UPLOAD_DIR = BASE_DIR / "uploads"
    LOG_DIR = BASE_DIR / "logs"

for d in [DATA_DIR, UPLOAD_DIR / "avatars", UPLOAD_DIR / "attachments", UPLOAD_DIR / "temp", LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

SECRET_KEY = os.environ.get("SECRET_KEY", "supersecretkeychangeinproduction")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7
MAX_UPLOAD_SIZE = 10 * 1024 * 1024
ALLOWED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".pdf", ".txt"}
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_PERIOD = 60

# ---------- UTILITY ----------
def generate_id() -> str:
    return str(uuid.uuid4())

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))

def create_access_token(data: dict) -> str:
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    data.update({"exp": expire})
    return jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)

def decode_access_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except:
        return None

def sanitize_html(text: str) -> str:
    return html.escape(text)

# ---------- FILE STORAGE ----------
_file_locks: Dict[str, asyncio.Lock] = {}

def _get_lock(filename: str) -> asyncio.Lock:
    if filename not in _file_locks:
        _file_locks[filename] = asyncio.Lock()
    return _file_locks[filename]

async def _read_json_file(filename: str, default: Any = None) -> Any:
    path = DATA_DIR / filename
    async with _get_lock(filename):
        if not await aiofiles.os.path.exists(path):
            return default if default is not None else []
        try:
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                content = await f.read()
                return json.loads(content) if content else (default if default is not None else [])
        except:
            backup = DATA_DIR / f"{filename}.backup"
            if await aiofiles.os.path.exists(backup):
                async with aiofiles.open(backup, "r", encoding="utf-8") as f:
                    return json.loads(await f.read())
            return default if default is not None else []

async def _write_json_file(filename: str, data: Any) -> None:
    path = DATA_DIR / filename
    backup = DATA_DIR / f"{filename}.backup"
    async with _get_lock(filename):
        if await aiofiles.os.path.exists(path):
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                old = await f.read()
            async with aiofiles.open(backup, "w", encoding="utf-8") as f:
                await f.write(old)
        tmp = DATA_DIR / f"{filename}.tmp"
        async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, default=str))
        await aiofiles.os.replace(tmp, path)

async def _append_to_json_file(filename: str, item: Any) -> None:
    data = await _read_json_file(filename, [])
    if not isinstance(data, list):
        data = []
    data.append(item)
    await _write_json_file(filename, data)

async def _update_in_json_file(filename: str, item_id: str, update_data: dict) -> bool:
    data = await _read_json_file(filename, [])
    if not isinstance(data, list):
        return False
    for i, item in enumerate(data):
        if item.get("id") == item_id:
            data[i] = {**item, **update_data}
            await _write_json_file(filename, data)
            return True
    return False

async def _delete_from_json_file(filename: str, item_id: str) -> bool:
    data = await _read_json_file(filename, [])
    if not isinstance(data, list):
        return False
    new_data = [item for item in data if item.get("id") != item_id]
    if len(new_data) != len(data):
        await _write_json_file(filename, new_data)
        return True
    return False

async def _find_in_json_file(filename: str, **kwargs) -> Optional[dict]:
    data = await _read_json_file(filename, [])
    if not isinstance(data, list):
        return None
    for item in data:
        match = True
        for k, v in kwargs.items():
            if item.get(k) != v:
                match = False
                break
        if match:
            return item
    return None

async def _find_all_in_json_file(filename: str, **kwargs) -> List[dict]:
    data = await _read_json_file(filename, [])
    if not isinstance(data, list):
        return []
    results = []
    for item in data:
        match = True
        for k, v in kwargs.items():
            if item.get(k) != v:
                match = False
                break
        if match:
            results.append(item)
    return results

# ---------- MODELS ----------
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=30)
    email: EmailStr
    password: str = Field(..., min_length=6)

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class ThreadCreate(BaseModel):
    title: str = Field(..., min_length=3, max_length=200)
    content: str = Field(..., min_length=1)
    tags: List[str] = []

class ThreadUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=3, max_length=200)
    content: Optional[str] = Field(None, min_length=1)
    is_pinned: Optional[bool] = None

class CommentCreate(BaseModel):
    content: str = Field(..., min_length=1)
    parent_id: Optional[str] = None

class CommentUpdate(BaseModel):
    content: str = Field(..., min_length=1)

class VoteCreate(BaseModel):
    value: int = Field(..., ge=-1, le=1)

class MessageCreate(BaseModel):
    content: str = Field(..., min_length=1)
    receiver_id: str

class ReportCreate(BaseModel):
    target_type: str = Field(..., pattern="^(thread|comment|user)$")  # FIXED
    target_id: str
    reason: str = Field(..., min_length=3, max_length=500)

# ---------- SERVICES ----------
async def create_user(username: str, email: str, password: str) -> dict:
    if await _find_in_json_file("users.json", email=email):
        raise HTTPException(400, "Email already registered")
    if await _find_in_json_file("users.json", username=username):
        raise HTTPException(400, "Username taken")
    user = {
        "id": generate_id(),
        "username": username,
        "email": email,
        "hashed_password": hash_password(password),
        "avatar": None,
        "bio": None,
        "is_verified": False,
        "is_developer": False,
        "is_moderator": False,
        "is_admin": False,
        "karma": 0,
        "created_at": now_iso()
    }
    await _append_to_json_file("users.json", user)
    await _append_to_json_file("settings.json", {
        "user_id": user["id"],
        "theme": "dark",
        "notifications_enabled": True,
        "email_notifications": True,
        "created_at": now_iso()
    })
    return user

async def authenticate_user(email: str, password: str) -> Optional[dict]:
    user = await _find_in_json_file("users.json", email=email)
    if user and verify_password(password, user["hashed_password"]):
        return user
    return None

async def get_user_by_id(user_id: str) -> Optional[dict]:
    return await _find_in_json_file("users.json", id=user_id)

async def get_user_by_username(username: str) -> Optional[dict]:
    return await _find_in_json_file("users.json", username=username)

async def get_user_public(user_id: str) -> Optional[dict]:
    user = await get_user_by_id(user_id)
    if not user:
        return None
    return {k: v for k, v in user.items() if k != "hashed_password"}

async def update_user(user_id: str, data: dict) -> bool:
    allowed = {"username", "bio", "avatar"}
    update = {k: v for k, v in data.items() if k in allowed}
    if not update:
        return False
    return await _update_in_json_file("users.json", user_id, update)

async def follow_user(follower_id: str, following_id: str) -> dict:
    if follower_id == following_id:
        raise HTTPException(400, "Cannot follow yourself")
    if await _find_in_json_file("follows.json", follower_id=follower_id, following_id=following_id):
        raise HTTPException(400, "Already following")
    follow = {"id": generate_id(), "follower_id": follower_id, "following_id": following_id, "created_at": now_iso()}
    await _append_to_json_file("follows.json", follow)
    return follow

async def unfollow_user(follower_id: str, following_id: str) -> bool:
    follows = await _find_all_in_json_file("follows.json", follower_id=follower_id, following_id=following_id)
    if not follows:
        return False
    for f in follows:
        await _delete_from_json_file("follows.json", f["id"])
    return True

async def get_followers(user_id: str) -> List[dict]:
    follows = await _find_all_in_json_file("follows.json", following_id=user_id)
    result = []
    for f in follows:
        u = await get_user_public(f["follower_id"])
        if u:
            result.append(u)
    return result

async def get_following(user_id: str) -> List[dict]:
    follows = await _find_all_in_json_file("follows.json", follower_id=user_id)
    result = []
    for f in follows:
        u = await get_user_public(f["following_id"])
        if u:
            result.append(u)
    return result

async def is_following(follower_id: str, following_id: str) -> bool:
    return await _find_in_json_file("follows.json", follower_id=follower_id, following_id=following_id) is not None

async def create_thread(user_id: str, title: str, content: str, tags: List[str] = None) -> dict:
    thread = {
        "id": generate_id(),
        "user_id": user_id,
        "title": title,
        "content": content,
        "tags": tags or [],
        "is_pinned": False,
        "is_locked": False,
        "upvotes": 0,
        "downvotes": 0,
        "comment_count": 0,
        "view_count": 0,
        "created_at": now_iso(),
        "updated_at": now_iso()
    }
    await _append_to_json_file("threads.json", thread)
    return thread

async def get_thread(thread_id: str) -> Optional[dict]:
    thread = await _find_in_json_file("threads.json", id=thread_id)
    if thread:
        await _update_in_json_file("threads.json", thread_id, {"view_count": thread.get("view_count", 0) + 1})
        thread["view_count"] = thread.get("view_count", 0) + 1
    return thread

async def get_threads(limit: int = 20, offset: int = 0, tag: str = None) -> List[dict]:
    threads = await _read_json_file("threads.json", [])
    if not isinstance(threads, list):
        return []
    if tag:
        threads = [t for t in threads if tag in t.get("tags", [])]
    threads.sort(key=lambda x: (not x.get("is_pinned", False), x.get("created_at", "")), reverse=True)
    return threads[offset:offset + limit]

async def update_thread(thread_id: str, user_id: str, data: dict) -> bool:
    thread = await get_thread(thread_id)
    if not thread:
        return False
    if thread["user_id"] != user_id:
        user = await get_user_by_id(user_id)
        if not user or not (user.get("is_moderator") or user.get("is_admin")):
            raise HTTPException(403, "Not authorized")
    update_data = {k: v for k, v in data.items() if k in ["title", "content", "is_pinned", "is_locked"]}
    if not update_data:
        return False
    update_data["updated_at"] = now_iso()
    return await _update_in_json_file("threads.json", thread_id, update_data)

async def delete_thread(thread_id: str, user_id: str) -> bool:
    thread = await get_thread(thread_id)
    if not thread:
        return False
    if thread["user_id"] != user_id:
        user = await get_user_by_id(user_id)
        if not user or not (user.get("is_moderator") or user.get("is_admin")):
            raise HTTPException(403, "Not authorized")
    comments = await _find_all_in_json_file("comments.json", thread_id=thread_id)
    for c in comments:
        await _delete_from_json_file("comments.json", c["id"])
    return await _delete_from_json_file("threads.json", thread_id)

async def create_comment(user_id: str, thread_id: str, content: str, parent_id: str = None) -> dict:
    thread = await get_thread(thread_id)
    if not thread:
        raise HTTPException(404, "Thread not found")
    if thread.get("is_locked"):
        raise HTTPException(403, "Thread is locked")
    comment = {
        "id": generate_id(),
        "user_id": user_id,
        "thread_id": thread_id,
        "parent_id": parent_id,
        "content": content,
        "upvotes": 0,
        "downvotes": 0,
        "is_deleted": False,
        "created_at": now_iso(),
        "updated_at": now_iso()
    }
    await _append_to_json_file("comments.json", comment)
    await _update_in_json_file("threads.json", thread_id, {"comment_count": thread.get("comment_count", 0) + 1})
    return comment

async def get_comments(thread_id: str, limit: int = 50, offset: int = 0) -> List[dict]:
    comments = await _find_all_in_json_file("comments.json", thread_id=thread_id)
    if not isinstance(comments, list):
        return []
    comments.sort(key=lambda x: x.get("created_at", ""))
    return comments[offset:offset + limit]

async def get_comment(comment_id: str) -> Optional[dict]:
    return await _find_in_json_file("comments.json", id=comment_id)

async def get_comment_tree(thread_id: str) -> List[dict]:
    comments = await _find_all_in_json_file("comments.json", thread_id=thread_id)
    if not isinstance(comments, list):
        return []
    map = {c["id"]: {**c, "replies": []} for c in comments}
    roots = []
    for c in comments:
        if c.get("parent_id"):
            parent = map.get(c["parent_id"])
            if parent:
                parent["replies"].append(map[c["id"]])
        else:
            roots.append(map[c["id"]])
    return roots

async def update_comment(comment_id: str, user_id: str, content: str) -> bool:
    comment = await get_comment(comment_id)
    if not comment:
        return False
    if comment["user_id"] != user_id:
        user = await get_user_by_id(user_id)
        if not user or not (user.get("is_moderator") or user.get("is_admin")):
            raise HTTPException(403, "Not authorized")
    return await _update_in_json_file("comments.json", comment_id, {"content": content, "updated_at": now_iso()})

async def delete_comment(comment_id: str, user_id: str) -> bool:
    comment = await get_comment(comment_id)
    if not comment:
        return False
    if comment["user_id"] != user_id:
        user = await get_user_by_id(user_id)
        if not user or not (user.get("is_moderator") or user.get("is_admin")):
            raise HTTPException(403, "Not authorized")
    return await _update_in_json_file("comments.json", comment_id, {"is_deleted": True, "content": "[deleted]"})

async def create_vote(user_id: str, target_type: str, target_id: str, value: int) -> dict:
    if target_type not in ["thread", "comment"]:
        raise HTTPException(400, "Invalid target type")
    existing = await _find_in_json_file("votes.json", user_id=user_id, target_type=target_type, target_id=target_id)
    if existing:
        if existing["value"] == value:
            raise HTTPException(400, "Already voted")
        await _update_in_json_file("votes.json", existing["id"], {"value": value})
        await _update_karma(target_type, target_id, value - existing["value"])
        return {"id": existing["id"], "value": value}
    vote = {"id": generate_id(), "user_id": user_id, "target_type": target_type, "target_id": target_id, "value": value, "created_at": now_iso()}
    await _append_to_json_file("votes.json", vote)
    await _update_karma(target_type, target_id, value)
    return vote

async def _update_karma(target_type: str, target_id: str, delta: int) -> None:
    if target_type == "thread":
        thread = await get_thread(target_id)
        if thread:
            if delta > 0:
                await _update_in_json_file("threads.json", target_id, {"upvotes": thread.get("upvotes", 0) + delta})
            else:
                await _update_in_json_file("threads.json", target_id, {"downvotes": thread.get("downvotes", 0) - delta})
            user = await get_user_by_id(thread["user_id"])
            if user:
                await _update_in_json_file("users.json", user["id"], {"karma": user.get("karma", 0) + delta})
    elif target_type == "comment":
        comment = await get_comment(target_id)
        if comment:
            if delta > 0:
                await _update_in_json_file("comments.json", target_id, {"upvotes": comment.get("upvotes", 0) + delta})
            else:
                await _update_in_json_file("comments.json", target_id, {"downvotes": comment.get("downvotes", 0) - delta})
            user = await get_user_by_id(comment["user_id"])
            if user:
                await _update_in_json_file("users.json", user["id"], {"karma": user.get("karma", 0) + delta})

async def get_vote(user_id: str, target_type: str, target_id: str) -> Optional[dict]:
    return await _find_in_json_file("votes.json", user_id=user_id, target_type=target_type, target_id=target_id)

async def create_bookmark(user_id: str, thread_id: str) -> dict:
    if await _find_in_json_file("bookmarks.json", user_id=user_id, thread_id=thread_id):
        raise HTTPException(400, "Already bookmarked")
    bookmark = {"id": generate_id(), "user_id": user_id, "thread_id": thread_id, "created_at": now_iso()}
    await _append_to_json_file("bookmarks.json", bookmark)
    return bookmark

async def delete_bookmark(user_id: str, thread_id: str) -> bool:
    bookmark = await _find_in_json_file("bookmarks.json", user_id=user_id, thread_id=thread_id)
    if not bookmark:
        return False
    return await _delete_from_json_file("bookmarks.json", bookmark["id"])

async def get_bookmarks(user_id: str) -> List[dict]:
    bookmarks = await _find_all_in_json_file("bookmarks.json", user_id=user_id)
    result = []
    for b in bookmarks:
        thread = await get_thread(b["thread_id"])
        if thread:
            result.append({"bookmark": b, "thread": thread})
    return result

async def create_notification(user_id: str, notification_type: str, message: str, data: dict = None) -> dict:
    notif = {"id": generate_id(), "user_id": user_id, "type": notification_type, "message": message, "data": data or {}, "is_read": False, "created_at": now_iso()}
    await _append_to_json_file("notifications.json", notif)
    return notif

async def get_notifications(user_id: str, limit: int = 20) -> List[dict]:
    notifs = await _find_all_in_json_file("notifications.json", user_id=user_id)
    if not isinstance(notifs, list):
        return []
    notifs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return notifs[:limit]

async def mark_notification_read(notification_id: str, user_id: str) -> bool:
    notif = await _find_in_json_file("notifications.json", id=notification_id)
    if not notif or notif["user_id"] != user_id:
        return False
    return await _update_in_json_file("notifications.json", notification_id, {"is_read": True})

async def mark_all_notifications_read(user_id: str) -> int:
    notifs = await _find_all_in_json_file("notifications.json", user_id=user_id)
    count = 0
    for n in notifs:
        if not n.get("is_read"):
            await _update_in_json_file("notifications.json", n["id"], {"is_read": True})
            count += 1
    return count

async def create_message(sender_id: str, receiver_id: str, content: str) -> dict:
    if sender_id == receiver_id:
        raise HTTPException(400, "Cannot send to yourself")
    receiver = await get_user_by_id(receiver_id)
    if not receiver:
        raise HTTPException(404, "Receiver not found")
    msg = {"id": generate_id(), "sender_id": sender_id, "receiver_id": receiver_id, "content": content, "is_read": False, "created_at": now_iso()}
    await _append_to_json_file("messages.json", msg)
    sender = await get_user_by_id(sender_id)
    await create_notification(receiver_id, "message", f"New message from {sender['username']}", {"sender_id": sender_id, "message_id": msg["id"]})
    return msg

async def get_messages(user_id: str, other_user_id: str, limit: int = 50) -> List[dict]:
    messages = await _read_json_file("messages.json", [])
    if not isinstance(messages, list):
        return []
    filtered = [m for m in messages if (m["sender_id"] == user_id and m["receiver_id"] == other_user_id) or (m["sender_id"] == other_user_id and m["receiver_id"] == user_id)]
    filtered.sort(key=lambda x: x.get("created_at", ""))
    return filtered[-limit:]

async def get_conversations(user_id: str) -> List[dict]:
    messages = await _read_json_file("messages.json", [])
    if not isinstance(messages, list):
        return []
    users = set()
    for m in messages:
        if m["sender_id"] == user_id:
            users.add(m["receiver_id"])
        elif m["receiver_id"] == user_id:
            users.add(m["sender_id"])
    result = []
    for uid in users:
        user = await get_user_public(uid)
        if user:
            last_msg = None
            for m in reversed(messages):
                if (m["sender_id"] == user_id and m["receiver_id"] == uid) or (m["sender_id"] == uid and m["receiver_id"] == user_id):
                    last_msg = m
                    break
            unread = len([m for m in messages if m["sender_id"] == uid and m["receiver_id"] == user_id and not m.get("is_read")])
            result.append({"user": user, "last_message": last_msg, "unread_count": unread})
    result.sort(key=lambda x: x["last_message"].get("created_at", "") if x["last_message"] else "", reverse=True)
    return result

async def mark_message_read(message_id: str, user_id: str) -> bool:
    msg = await _find_in_json_file("messages.json", id=message_id)
    if not msg or msg["receiver_id"] != user_id:
        return False
    return await _update_in_json_file("messages.json", message_id, {"is_read": True})

async def search(query: str, limit: int = 20) -> dict:
    q = query.lower()
    threads = await _read_json_file("threads.json", [])
    users = await _read_json_file("users.json", [])
    thread_results = [t for t in threads if q in t.get("title", "").lower() or q in t.get("content", "").lower()]
    user_results = []
    for u in users:
        if q in u.get("username", "").lower():
            user_results.append({k: v for k, v in u.items() if k != "hashed_password"})
    return {"threads": thread_results[:limit], "users": user_results[:limit]}

async def create_report(reporter_id: str, target_type: str, target_id: str, reason: str) -> dict:
    report = {"id": generate_id(), "reporter_id": reporter_id, "target_type": target_type, "target_id": target_id, "reason": reason, "status": "pending", "created_at": now_iso()}
    await _append_to_json_file("reports.json", report)
    return report

async def get_reports(status: str = None) -> List[dict]:
    reports = await _read_json_file("reports.json", [])
    if not isinstance(reports, list):
        return []
    if status:
        reports = [r for r in reports if r.get("status") == status]
    reports.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return reports

async def update_report_status(report_id: str, status: str, moderator_id: str) -> bool:
    moderator = await get_user_by_id(moderator_id)
    if not moderator or not (moderator.get("is_moderator") or moderator.get("is_admin")):
        raise HTTPException(403, "Not authorized")
    if status not in ["pending", "reviewed", "dismissed"]:
        raise HTTPException(400, "Invalid status")
    return await _update_in_json_file("reports.json", report_id, {"status": status})

async def promote_user(user_id: str, role: str, admin_id: str) -> bool:
    admin = await get_user_by_id(admin_id)
    if not admin or not admin.get("is_admin"):
        raise HTTPException(403, "Not authorized")
    mapping = {"moderator": "is_moderator", "admin": "is_admin", "developer": "is_developer", "verified": "is_verified"}
    if role not in mapping:
        raise HTTPException(400, "Invalid role")
    return await _update_in_json_file("users.json", user_id, {mapping[role]: True})

async def demote_user(user_id: str, role: str, admin_id: str) -> bool:
    admin = await get_user_by_id(admin_id)
    if not admin or not admin.get("is_admin"):
        raise HTTPException(403, "Not authorized")
    mapping = {"moderator": "is_moderator", "admin": "is_admin", "developer": "is_developer", "verified": "is_verified"}
    if role not in mapping:
        raise HTTPException(400, "Invalid role")
    if role == "admin" and user_id == admin_id:
        raise HTTPException(400, "Cannot demote yourself")
    return await _update_in_json_file("users.json", user_id, {mapping[role]: False})

async def get_system_stats() -> dict:
    users = await _read_json_file("users.json", [])
    threads = await _read_json_file("threads.json", [])
    comments = await _read_json_file("comments.json", [])
    return {"total_users": len(users) if isinstance(users, list) else 0,
            "total_threads": len(threads) if isinstance(threads, list) else 0,
            "total_comments": len(comments) if isinstance(comments, list) else 0}

# ---------- AUTH ----------
async def get_current_user(request: Request) -> dict:
    auth = request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Not authenticated")
    token = auth.split(" ")[1]
    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(401, "Invalid token")
    user = await get_user_by_id(payload.get("sub"))
    if not user:
        raise HTTPException(401, "User not found")
    return user

# ---------- WEBSOCKET ----------
class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, WebSocket] = {}
        self.user_map: Dict[str, str] = {}
    async def connect(self, ws: WebSocket, user_id: str) -> str:
        await ws.accept()
        cid = generate_id()
        self.active[cid] = ws
        self.user_map[user_id] = cid
        return cid
    def disconnect(self, cid: str, user_id: str = None):
        if cid in self.active:
            del self.active[cid]
        if user_id and user_id in self.user_map:
            del self.user_map[user_id]
    async def send_message(self, user_id: str, message: dict):
        cid = self.user_map.get(user_id)
        if cid and cid in self.active:
            try:
                await self.active[cid].send_json(message)
            except:
                pass

manager = ConnectionManager()

# ---------- APP ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    for f in ["users.json", "threads.json", "comments.json", "votes.json", "notifications.json", "messages.json", "sessions.json", "reports.json", "bookmarks.json", "settings.json", "follows.json", "logs.json"]:
        if not await aiofiles.os.path.exists(DATA_DIR / f):
            await _write_json_file(f, [])
    yield

app = FastAPI(title="NexThread", lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])

_rate_limit_cache: Dict[str, List[float]] = {}
async def check_rate_limit(identifier: str) -> bool:
    now = datetime.utcnow().timestamp()
    if identifier not in _rate_limit_cache:
        _rate_limit_cache[identifier] = []
    _rate_limit_cache[identifier] = [t for t in _rate_limit_cache[identifier] if now - t < RATE_LIMIT_PERIOD]
    if len(_rate_limit_cache[identifier]) >= RATE_LIMIT_REQUESTS:
        return False
    _rate_limit_cache[identifier].append(now)
    return True

# ---------- ROUTES ----------
@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": now_iso()}

@app.post("/api/auth/register")
async def register(data: UserCreate):
    if not await check_rate_limit(f"reg_{data.email}"):
        raise HTTPException(429, "Too many requests")
    user = await create_user(data.username, data.email, data.password)
    token = create_access_token({"sub": user["id"]})
    return {"token": token, "user": {k: v for k, v in user.items() if k != "hashed_password"}}

@app.post("/api/auth/login")
async def login(data: UserLogin):
    if not await check_rate_limit(f"login_{data.email}"):
        raise HTTPException(429, "Too many requests")
    user = await authenticate_user(data.email, data.password)
    if not user:
        raise HTTPException(401, "Invalid credentials")
    token = create_access_token({"sub": user["id"]})
    return {"token": token, "user": {k: v for k, v in user.items() if k != "hashed_password"}}

@app.get("/api/auth/me")
async def me(current_user: dict = Depends(get_current_user)):
    return {k: v for k, v in current_user.items() if k != "hashed_password"}

@app.get("/api/users")
async def list_users(limit: int = 20, offset: int = 0):
    users = await _read_json_file("users.json", [])
    if not isinstance(users, list):
        return []
    users = users[offset:offset+limit]
    return [{k: v for k, v in u.items() if k != "hashed_password"} for u in users]

@app.get("/api/users/{user_id}")
async def get_user(user_id: str):
    user = await get_user_public(user_id)
    if not user:
        raise HTTPException(404, "User not found")
    return user

@app.get("/api/users/username/{username}")
async def get_user_by_username(username: str):
    user = await get_user_by_username(username)
    if not user:
        raise HTTPException(404, "User not found")
    return {k: v for k, v in user.items() if k != "hashed_password"}

@app.put("/api/users/me")
async def update_me(username: Optional[str] = Form(None), bio: Optional[str] = Form(None), current_user: dict = Depends(get_current_user)):
    data = {}
    if username is not None:
        existing = await get_user_by_username(username)
        if existing and existing["id"] != current_user["id"]:
            raise HTTPException(400, "Username taken")
        data["username"] = username
    if bio is not None:
        data["bio"] = sanitize_html(bio)[:500]
    if not data:
        raise HTTPException(400, "No fields")
    await update_user(current_user["id"], data)
    return {"message": "Updated"}

@app.post("/api/users/me/avatar")
async def upload_avatar(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    if file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(400, "File too large")
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, "File type not allowed")
    filename = f"{current_user['id']}_{generate_id()[:8]}{ext}"
    path = UPLOAD_DIR / "avatars" / filename
    async with aiofiles.open(path, "wb") as f:
        await f.write(await file.read())
    avatar_url = f"/uploads/avatars/{filename}"
    await update_user(current_user["id"], {"avatar": avatar_url})
    return {"avatar": avatar_url}

@app.post("/api/users/{user_id}/follow")
async def follow(user_id: str, current_user: dict = Depends(get_current_user)):
    result = await follow_user(current_user["id"], user_id)
    await create_notification(user_id, "follow", f"{current_user['username']} followed you", {"follower_id": current_user["id"]})
    return result

@app.delete("/api/users/{user_id}/follow")
async def unfollow(user_id: str, current_user: dict = Depends(get_current_user)):
    if not await unfollow_user(current_user["id"], user_id):
        raise HTTPException(404, "Not following")
    return {"message": "Unfollowed"}

@app.get("/api/users/{user_id}/followers")
async def followers(user_id: str):
    return await get_followers(user_id)

@app.get("/api/users/{user_id}/following")
async def following(user_id: str):
    return await get_following(user_id)

@app.get("/api/users/me/followers")
async def my_followers(current_user: dict = Depends(get_current_user)):
    return await get_followers(current_user["id"])

@app.get("/api/users/me/following")
async def my_following(current_user: dict = Depends(get_current_user)):
    return await get_following(current_user["id"])

@app.post("/api/threads")
async def create_thread_route(title: str = Form(...), content: str = Form(...), tags: Optional[str] = Form(None), current_user: dict = Depends(get_current_user)):
    if not await check_rate_limit(f"thread_{current_user['id']}"):
        raise HTTPException(429, "Too many threads")
    thread = await create_thread(current_user["id"], title, content, [t.strip() for t in tags.split(",")] if tags else [])
    return thread

@app.get("/api/threads")
async def list_threads(limit: int = 20, offset: int = 0, tag: Optional[str] = None):
    return await get_threads(limit, offset, tag)

@app.get("/api/threads/{thread_id}")
async def get_thread_route(thread_id: str):
    thread = await get_thread(thread_id)
    if not thread:
        raise HTTPException(404, "Thread not found")
    author = await get_user_public(thread["user_id"])
    return {**thread, "author": author}

@app.put("/api/threads/{thread_id}")
async def update_thread_route(thread_id: str, title: Optional[str] = Form(None), content: Optional[str] = Form(None), is_pinned: Optional[bool] = Form(None), is_locked: Optional[bool] = Form(None), current_user: dict = Depends(get_current_user)):
    data = {}
    if title is not None: data["title"] = title
    if content is not None: data["content"] = content
    if is_pinned is not None: data["is_pinned"] = is_pinned
    if is_locked is not None: data["is_locked"] = is_locked
    if not data:
        raise HTTPException(400, "No fields")
    if not await update_thread(thread_id, current_user["id"], data):
        raise HTTPException(404, "Thread not found")
    return {"message": "Updated"}

@app.delete("/api/threads/{thread_id}")
async def delete_thread_route(thread_id: str, current_user: dict = Depends(get_current_user)):
    if not await delete_thread(thread_id, current_user["id"]):
        raise HTTPException(404, "Thread not found")
    return {"message": "Deleted"}

@app.post("/api/threads/{thread_id}/comments")
async def create_comment_route(thread_id: str, content: str = Form(...), parent_id: Optional[str] = Form(None), current_user: dict = Depends(get_current_user)):
    if not await check_rate_limit(f"comment_{current_user['id']}"):
        raise HTTPException(429, "Too many comments")
    comment = await create_comment(current_user["id"], thread_id, content, parent_id)
    thread = await get_thread(thread_id)
    if thread and thread["user_id"] != current_user["id"]:
        await create_notification(thread["user_id"], "comment", f"{current_user['username']} commented on your thread", {"thread_id": thread_id, "comment_id": comment["id"]})
    return comment

@app.get("/api/threads/{thread_id}/comments")
async def list_comments(thread_id: str, limit: int = 50, offset: int = 0):
    return await get_comments(thread_id, limit, offset)

@app.get("/api/threads/{thread_id}/comments/tree")
async def comment_tree(thread_id: str):
    return await get_comment_tree(thread_id)

@app.put("/api/comments/{comment_id}")
async def update_comment_route(comment_id: str, content: str = Form(...), current_user: dict = Depends(get_current_user)):
    if not await update_comment(comment_id, current_user["id"], content):
        raise HTTPException(404, "Comment not found")
    return {"message": "Updated"}

@app.delete("/api/comments/{comment_id}")
async def delete_comment_route(comment_id: str, current_user: dict = Depends(get_current_user)):
    if not await delete_comment(comment_id, current_user["id"]):
        raise HTTPException(404, "Comment not found")
    return {"message": "Deleted"}

@app.post("/api/votes")
async def create_vote_route(target_type: str = Form(...), target_id: str = Form(...), value: int = Form(...), current_user: dict = Depends(get_current_user)):
    if value not in [-1, 1]:
        raise HTTPException(400, "Value must be -1 or 1")
    return await create_vote(current_user["id"], target_type, target_id, value)

@app.get("/api/votes/{target_type}/{target_id}")
async def get_vote_route(target_type: str, target_id: str, current_user: dict = Depends(get_current_user)):
    vote = await get_vote(current_user["id"], target_type, target_id)
    return {"value": vote["value"] if vote else 0}

@app.post("/api/bookmarks/{thread_id}")
async def bookmark(thread_id: str, current_user: dict = Depends(get_current_user)):
    return await create_bookmark(current_user["id"], thread_id)

@app.delete("/api/bookmarks/{thread_id}")
async def unbookmark(thread_id: str, current_user: dict = Depends(get_current_user)):
    if not await delete_bookmark(current_user["id"], thread_id):
        raise HTTPException(404, "Bookmark not found")
    return {"message": "Removed"}

@app.get("/api/bookmarks")
async def get_my_bookmarks(current_user: dict = Depends(get_current_user)):
    return await get_bookmarks(current_user["id"])

@app.get("/api/notifications")
async def get_my_notifications(limit: int = 20, current_user: dict = Depends(get_current_user)):
    return await get_notifications(current_user["id"], limit)

@app.put("/api/notifications/{notification_id}/read")
async def mark_read(notification_id: str, current_user: dict = Depends(get_current_user)):
    if not await mark_notification_read(notification_id, current_user["id"]):
        raise HTTPException(404, "Not found")
    return {"message": "Marked read"}

@app.put("/api/notifications/read-all")
async def mark_all_read(current_user: dict = Depends(get_current_user)):
    count = await mark_all_notifications_read(current_user["id"])
    return {"message": f"Marked {count} read"}

@app.post("/api/messages")
async def send_message(receiver_id: str = Form(...), content: str = Form(...), current_user: dict = Depends(get_current_user)):
    if not await check_rate_limit(f"msg_{current_user['id']}"):
        raise HTTPException(429, "Too many messages")
    msg = await create_message(current_user["id"], receiver_id, content)
    await manager.send_message(receiver_id, {"type": "message", "data": msg})
    return msg

@app.get("/api/messages/conversations")
async def conversations(current_user: dict = Depends(get_current_user)):
    return await get_conversations(current_user["id"])

@app.get("/api/messages/{user_id}")
async def messages_with(user_id: str, limit: int = 50, current_user: dict = Depends(get_current_user)):
    return await get_messages(current_user["id"], user_id, limit)

@app.put("/api/messages/{message_id}/read")
async def mark_msg_read(message_id: str, current_user: dict = Depends(get_current_user)):
    if not await mark_message_read(message_id, current_user["id"]):
        raise HTTPException(404, "Message not found")
    return {"message": "Marked read"}

@app.get("/api/search")
async def search_route(q: str, limit: int = 20):
    if len(q) < 2:
        return {"threads": [], "users": []}
    return await search(q, limit)

@app.post("/api/reports")
async def report(target_type: str = Form(...), target_id: str = Form(...), reason: str = Form(...), current_user: dict = Depends(get_current_user)):
    if not await check_rate_limit(f"report_{current_user['id']}"):
        raise HTTPException(429, "Too many reports")
    return await create_report(current_user["id"], target_type, target_id, reason)

@app.get("/api/reports")
async def list_reports(status: Optional[str] = None, current_user: dict = Depends(get_current_user)):
    if not current_user.get("is_moderator") and not current_user.get("is_admin"):
        raise HTTPException(403, "Not authorized")
    return await get_reports(status)

@app.put("/api/reports/{report_id}")
async def update_report(report_id: str, status: str = Form(...), current_user: dict = Depends(get_current_user)):
    if not await update_report_status(report_id, status, current_user["id"]):
        raise HTTPException(404, "Report not found")
    return {"message": "Updated"}

@app.get("/api/admin/stats")
async def stats(current_user: dict = Depends(get_current_user)):
    if not current_user.get("is_admin"):
        raise HTTPException(403, "Not authorized")
    return await get_system_stats()

@app.post("/api/admin/users/{user_id}/promote")
async def promote(user_id: str, role: str = Form(...), current_user: dict = Depends(get_current_user)):
    if not await promote_user(user_id, role, current_user["id"]):
        raise HTTPException(404, "User not found")
    return {"message": f"Promoted to {role}"}

@app.post("/api/admin/users/{user_id}/demote")
async def demote(user_id: str, role: str = Form(...), current_user: dict = Depends(get_current_user)):
    if not await demote_user(user_id, role, current_user["id"]):
        raise HTTPException(404, "User not found")
    return {"message": f"Demoted from {role}"}

@app.get("/uploads/{folder}/{filename}")
async def serve_upload(folder: str, filename: str):
    path = UPLOAD_DIR / folder / filename
    if not await aiofiles.os.path.exists(path):
        raise HTTPException(404, "File not found")
    return FileResponse(path)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return
    payload = decode_access_token(token)
    if not payload:
        await websocket.close(code=1008)
        return
    user = await get_user_by_id(payload.get("sub"))
    if not user:
        await websocket.close(code=1008)
        return
    cid = await manager.connect(websocket, user["id"])
    try:
        await websocket.send_json({"type": "connected", "user_id": user["id"]})
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
            elif data.get("type") == "message":
                receiver_id = data.get("receiver_id")
                content = data.get("content")
                if receiver_id and content:
                    msg = await create_message(user["id"], receiver_id, content)
                    await manager.send_message(receiver_id, {"type": "message", "data": msg})
                    await websocket.send_json({"type": "message_sent", "data": msg})
            elif data.get("type") == "typing":
                receiver_id = data.get("receiver_id")
                if receiver_id:
                    await manager.send_message(receiver_id, {"type": "typing", "sender_id": user["id"]})
    except WebSocketDisconnect:
        manager.disconnect(cid, user["id"])
    except:
        manager.disconnect(cid, user["id"])

# ---------- SERVE FRONTEND ----------
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    static_dir = BASE_DIR / "static"
    index_path = static_dir / "index.html"
    if await aiofiles.os.path.exists(index_path):
        async with aiofiles.open(index_path, "r", encoding="utf-8") as f:
            content = await f.read()
        return HTMLResponse(content)
    # Fallback if static file missing
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head><title>NexThread</title><style>body{background:#0a0a0a;color:#fff;font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;text-align:center;}h1{font-size:3rem;}</style></head>
    <body><h1>🚀 NexThread</h1><p>The Future of Discussion – API running</p><p><a href="/docs" style="color:#00bfff;">API Docs</a></p></body>
    </html>
    """)

# ---------- STATIC FILES ----------
@app.get("/static/{file_path:path}")
async def serve_static(file_path: str):
    path = BASE_DIR / "static" / file_path
    if not await aiofiles.os.path.exists(path):
        raise HTTPException(404, "File not found")
    return FileResponse(path)

# ---------- EXPORT ----------
# This is the app that Vercel will run.
