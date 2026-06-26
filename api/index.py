import os
import json
import asyncio
import hashlib
import bcrypt
import jwt
import uuid
import shutil
import mimetypes
import re
import html
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Request, UploadFile, File, Form, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, EmailStr, Field, validator
import aiofiles
import aiofiles.os

# ==================================================
# KONFIGURASI
# ==================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = BASE_DIR / "uploads"
LOG_DIR = BASE_DIR / "logs"
STATIC_DIR = BASE_DIR / "static"

SECRET_KEY = os.environ.get("SECRET_KEY", "supersecretkeychangeinproduction")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 hari
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10 MB
ALLOWED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".pdf", ".txt"}
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_PERIOD = 60

for d in [DATA_DIR, UPLOAD_DIR / "avatars", UPLOAD_DIR / "attachments", UPLOAD_DIR / "temp", LOG_DIR, STATIC_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ==================================================
# UTILITY FUNCTIONS
# ==================================================

def generate_id() -> str:
    return str(uuid.uuid4())

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.PyJWTError:
        return None

def sanitize_html(text: str) -> str:
    return html.escape(text)

# ==================================================
# FILE STORAGE ENGINE (Async JSON dengan locking)
# ==================================================

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
        except (json.JSONDecodeError, IOError):
            # Attempt recovery from backup
            backup_path = DATA_DIR / f"{filename}.backup"
            if await aiofiles.os.path.exists(backup_path):
                async with aiofiles.open(backup_path, "r", encoding="utf-8") as f:
                    content = await f.read()
                    return json.loads(content) if content else (default if default is not None else [])
            return default if default is not None else []

async def _write_json_file(filename: str, data: Any) -> None:
    path = DATA_DIR / filename
    backup_path = DATA_DIR / f"{filename}.backup"
    async with _get_lock(filename):
        # Tulis backup
        if await aiofiles.os.path.exists(path):
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                old_content = await f.read()
            async with aiofiles.open(backup_path, "w", encoding="utf-8") as f:
                await f.write(old_content)
        # Tulis atomic dengan temporary file
        temp_path = DATA_DIR / f"{filename}.tmp"
        async with aiofiles.open(temp_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, default=str))
        await aiofiles.os.replace(temp_path, path)

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
        for key, value in kwargs.items():
            if item.get(key) != value:
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
        for key, value in kwargs.items():
            if item.get(key) != value:
                match = False
                break
        if match:
            results.append(item)
    return results

# ==================================================
# MODELS (Pydantic)
# ==================================================

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=30)
    email: EmailStr
    password: str = Field(..., min_length=6)

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    avatar: Optional[str] = None
    bio: Optional[str] = None
    is_verified: bool = False
    is_developer: bool = False
    is_moderator: bool = False
    is_admin: bool = False
    karma: int = 0
    created_at: str

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
    target_type: str = Field(..., regex="^(thread|comment|user)$")
    target_id: str
    reason: str = Field(..., min_length=3, max_length=500)

# ==================================================
# SERVICE LAYER
# ==================================================

# --- User Service ---

async def create_user(username: str, email: str, password: str) -> dict:
    # Cek username/email sudah ada
    existing = await _find_in_json_file("users.json", email=email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    existing = await _find_in_json_file("users.json", username=username)
    if existing:
        raise HTTPException(status_code=400, detail="Username already taken")
    
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
    # Buat settings default
    settings = {
        "user_id": user["id"],
        "theme": "dark",
        "notifications_enabled": True,
        "email_notifications": True,
        "created_at": now_iso()
    }
    await _append_to_json_file("settings.json", settings)
    return user

async def authenticate_user(email: str, password: str) -> Optional[dict]:
    user = await _find_in_json_file("users.json", email=email)
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user

async def get_user_by_id(user_id: str) -> Optional[dict]:
    return await _find_in_json_file("users.json", id=user_id)

async def get_user_by_username(username: str) -> Optional[dict]:
    return await _find_in_json_file("users.json", username=username)

async def get_user_public(user_id: str) -> Optional[dict]:
    user = await get_user_by_id(user_id)
    if not user:
        return None
    return {
        "id": user["id"],
        "username": user["username"],
        "avatar": user.get("avatar"),
        "bio": user.get("bio"),
        "is_verified": user.get("is_verified", False),
        "is_developer": user.get("is_developer", False),
        "karma": user.get("karma", 0),
        "created_at": user.get("created_at")
    }

async def update_user(user_id: str, data: dict) -> bool:
    allowed = ["username", "bio", "avatar"]
    update_data = {k: v for k, v in data.items() if k in allowed}
    if not update_data:
        return False
    return await _update_in_json_file("users.json", user_id, update_data)

async def follow_user(follower_id: str, following_id: str) -> dict:
    if follower_id == following_id:
        raise HTTPException(status_code=400, detail="Cannot follow yourself")
    # Cek sudah follow
    existing = await _find_in_json_file("follows.json", follower_id=follower_id, following_id=following_id)
    if existing:
        raise HTTPException(status_code=400, detail="Already following")
    follow = {
        "id": generate_id(),
        "follower_id": follower_id,
        "following_id": following_id,
        "created_at": now_iso()
    }
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
        user = await get_user_public(f["follower_id"])
        if user:
            result.append(user)
    return result

async def get_following(user_id: str) -> List[dict]:
    follows = await _find_all_in_json_file("follows.json", follower_id=user_id)
    result = []
    for f in follows:
        user = await get_user_public(f["following_id"])
        if user:
            result.append(user)
    return result

async def is_following(follower_id: str, following_id: str) -> bool:
    return await _find_in_json_file("follows.json", follower_id=follower_id, following_id=following_id) is not None

# --- Thread Service ---

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
        # Increment view count
        await _update_in_json_file("threads.json", thread_id, {"view_count": thread.get("view_count", 0) + 1})
        thread["view_count"] = thread.get("view_count", 0) + 1
    return thread

async def get_threads(limit: int = 20, offset: int = 0, tag: str = None) -> List[dict]:
    threads = await _read_json_file("threads.json", [])
    if not isinstance(threads, list):
        return []
    if tag:
        threads = [t for t in threads if tag in t.get("tags", [])]
    # Sort by created_at descending, pinned first
    threads.sort(key=lambda x: (not x.get("is_pinned", False), x.get("created_at", "")), reverse=True)
    return threads[offset:offset + limit]

async def update_thread(thread_id: str, user_id: str, data: dict) -> bool:
    thread = await get_thread(thread_id)
    if not thread:
        return False
    if thread["user_id"] != user_id:
        # Cek moderator/admin
        user = await get_user_by_id(user_id)
        if not user or not (user.get("is_moderator") or user.get("is_admin")):
            raise HTTPException(status_code=403, detail="Not authorized to edit this thread")
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
            raise HTTPException(status_code=403, detail="Not authorized to delete this thread")
    # Delete all comments in thread
    comments = await _find_all_in_json_file("comments.json", thread_id=thread_id)
    for c in comments:
        await _delete_from_json_file("comments.json", c["id"])
    return await _delete_from_json_file("threads.json", thread_id)

# --- Comment Service ---

async def create_comment(user_id: str, thread_id: str, content: str, parent_id: str = None) -> dict:
    # Cek thread exists
    thread = await get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if thread.get("is_locked"):
        raise HTTPException(status_code=403, detail="Thread is locked")
    
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
    # Update comment count
    await _update_in_json_file("threads.json", thread_id, {"comment_count": thread.get("comment_count", 0) + 1})
    return comment

async def get_comments(thread_id: str, limit: int = 50, offset: int = 0) -> List[dict]:
    comments = await _find_all_in_json_file("comments.json", thread_id=thread_id)
    if not isinstance(comments, list):
        return []
    # Sort by created_at
    comments.sort(key=lambda x: x.get("created_at", ""))
    return comments[offset:offset + limit]

async def get_comment(comment_id: str) -> Optional[dict]:
    return await _find_in_json_file("comments.json", id=comment_id)

async def get_comment_tree(thread_id: str) -> List[dict]:
    comments = await _find_all_in_json_file("comments.json", thread_id=thread_id)
    if not isinstance(comments, list):
        return []
    # Build tree
    comment_map = {c["id"]: {**c, "replies": []} for c in comments}
    roots = []
    for c in comments:
        if c.get("parent_id"):
            parent = comment_map.get(c["parent_id"])
            if parent:
                parent["replies"].append(comment_map[c["id"]])
        else:
            roots.append(comment_map[c["id"]])
    return roots

async def update_comment(comment_id: str, user_id: str, content: str) -> bool:
    comment = await get_comment(comment_id)
    if not comment:
        return False
    if comment["user_id"] != user_id:
        user = await get_user_by_id(user_id)
        if not user or not (user.get("is_moderator") or user.get("is_admin")):
            raise HTTPException(status_code=403, detail="Not authorized")
    return await _update_in_json_file("comments.json", comment_id, {"content": content, "updated_at": now_iso()})

async def delete_comment(comment_id: str, user_id: str) -> bool:
    comment = await get_comment(comment_id)
    if not comment:
        return False
    if comment["user_id"] != user_id:
        user = await get_user_by_id(user_id)
        if not user or not (user.get("is_moderator") or user.get("is_admin")):
            raise HTTPException(status_code=403, detail="Not authorized")
    # Soft delete
    return await _update_in_json_file("comments.json", comment_id, {"is_deleted": True, "content": "[deleted]"})

# --- Vote Service ---

async def create_vote(user_id: str, target_type: str, target_id: str, value: int) -> dict:
    if target_type not in ["thread", "comment"]:
        raise HTTPException(status_code=400, detail="Invalid target type")
    
    # Cek sudah vote
    existing = await _find_in_json_file("votes.json", user_id=user_id, target_type=target_type, target_id=target_id)
    if existing:
        if existing.get("value") == value:
            raise HTTPException(status_code=400, detail="Already voted this way")
        # Update vote
        await _update_in_json_file("votes.json", existing["id"], {"value": value})
        # Update karma
        await _update_karma(target_type, target_id, value - existing["value"])
        return {"id": existing["id"], "value": value}
    
    vote = {
        "id": generate_id(),
        "user_id": user_id,
        "target_type": target_type,
        "target_id": target_id,
        "value": value,
        "created_at": now_iso()
    }
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
            # Update user karma
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

# --- Bookmark Service ---

async def create_bookmark(user_id: str, thread_id: str) -> dict:
    existing = await _find_in_json_file("bookmarks.json", user_id=user_id, thread_id=thread_id)
    if existing:
        raise HTTPException(status_code=400, detail="Already bookmarked")
    bookmark = {
        "id": generate_id(),
        "user_id": user_id,
        "thread_id": thread_id,
        "created_at": now_iso()
    }
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

# --- Notification Service ---

async def create_notification(user_id: str, notification_type: str, message: str, data: dict = None) -> dict:
    notif = {
        "id": generate_id(),
        "user_id": user_id,
        "type": notification_type,
        "message": message,
        "data": data or {},
        "is_read": False,
        "created_at": now_iso()
    }
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

# --- Message Service ---

async def create_message(sender_id: str, receiver_id: str, content: str) -> dict:
    if sender_id == receiver_id:
        raise HTTPException(status_code=400, detail="Cannot send message to yourself")
    receiver = await get_user_by_id(receiver_id)
    if not receiver:
        raise HTTPException(status_code=404, detail="Receiver not found")
    
    msg = {
        "id": generate_id(),
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "content": content,
        "is_read": False,
        "created_at": now_iso()
    }
    await _append_to_json_file("messages.json", msg)
    # Create notification
    sender = await get_user_by_id(sender_id)
    await create_notification(
        receiver_id,
        "message",
        f"New message from {sender['username'] if sender else 'someone'}",
        {"sender_id": sender_id, "message_id": msg["id"]}
    )
    return msg

async def get_messages(user_id: str, other_user_id: str, limit: int = 50) -> List[dict]:
    messages = await _read_json_file("messages.json", [])
    if not isinstance(messages, list):
        return []
    filtered = [
        m for m in messages
        if (m["sender_id"] == user_id and m["receiver_id"] == other_user_id) or
           (m["sender_id"] == other_user_id and m["receiver_id"] == user_id)
    ]
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
                if (m["sender_id"] == user_id and m["receiver_id"] == uid) or \
                   (m["sender_id"] == uid and m["receiver_id"] == user_id):
                    last_msg = m
                    break
            result.append({
                "user": user,
                "last_message": last_msg,
                "unread_count": len([m for m in messages if m["sender_id"] == uid and m["receiver_id"] == user_id and not m.get("is_read")])
            })
    result.sort(key=lambda x: x["last_message"].get("created_at", "") if x["last_message"] else "", reverse=True)
    return result

async def mark_message_read(message_id: str, user_id: str) -> bool:
    msg = await _find_in_json_file("messages.json", id=message_id)
    if not msg or msg["receiver_id"] != user_id:
        return False
    return await _update_in_json_file("messages.json", message_id, {"is_read": True})

# --- Search Service ---

async def search(query: str, limit: int = 20) -> dict:
    query_lower = query.lower()
    threads = await _read_json_file("threads.json", [])
    users = await _read_json_file("users.json", [])
    
    thread_results = []
    for t in threads:
        if query_lower in t.get("title", "").lower() or query_lower in t.get("content", "").lower():
            thread_results.append(t)
    
    user_results = []
    for u in users:
        if query_lower in u.get("username", "").lower():
            user_results.append({k: v for k, v in u.items() if k not in ["hashed_password"]})
    
    return {
        "threads": thread_results[:limit],
        "users": user_results[:limit]
    }

# --- Report Service ---

async def create_report(reporter_id: str, target_type: str, target_id: str, reason: str) -> dict:
    report = {
        "id": generate_id(),
        "reporter_id": reporter_id,
        "target_type": target_type,
        "target_id": target_id,
        "reason": reason,
        "status": "pending",
        "created_at": now_iso()
    }
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
        raise HTTPException(status_code=403, detail="Not authorized")
    if status not in ["pending", "reviewed", "dismissed"]:
        raise HTTPException(status_code=400, detail="Invalid status")
    return await _update_in_json_file("reports.json", report_id, {"status": status})

# --- Admin Service ---

async def promote_user(user_id: str, role: str, admin_id: str) -> bool:
    admin = await get_user_by_id(admin_id)
    if not admin or not admin.get("is_admin"):
        raise HTTPException(status_code=403, detail="Not authorized")
    if role not in ["moderator", "admin", "developer", "verified"]:
        raise HTTPException(status_code=400, detail="Invalid role")
    
    mapping = {
        "moderator": "is_moderator",
        "admin": "is_admin",
        "developer": "is_developer",
        "verified": "is_verified"
    }
    return await _update_in_json_file("users.json", user_id, {mapping[role]: True})

async def demote_user(user_id: str, role: str, admin_id: str) -> bool:
    admin = await get_user_by_id(admin_id)
    if not admin or not admin.get("is_admin"):
        raise HTTPException(status_code=403, detail="Not authorized")
    if role not in ["moderator", "admin", "developer", "verified"]:
        raise HTTPException(status_code=400, detail="Invalid role")
    
    # Prevent demoting self from admin
    if role == "admin" and user_id == admin_id:
        raise HTTPException(status_code=400, detail="Cannot demote yourself from admin")
    
    mapping = {
        "moderator": "is_moderator",
        "admin": "is_admin",
        "developer": "is_developer",
        "verified": "is_verified"
    }
    return await _update_in_json_file("users.json", user_id, {mapping[role]: False})

async def get_system_stats() -> dict:
    users = await _read_json_file("users.json", [])
    threads = await _read_json_file("threads.json", [])
    comments = await _read_json_file("comments.json", [])
    return {
        "total_users": len(users) if isinstance(users, list) else 0,
        "total_threads": len(threads) if isinstance(threads, list) else 0,
        "total_comments": len(comments) if isinstance(comments, list) else 0
    }

# ==================================================
# AUTHENTICATION DEPENDENCY
# ==================================================

async def get_current_user(request: Request) -> dict:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = auth_header.split(" ")[1]
    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    user = await get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user

async def get_current_user_optional(request: Request) -> Optional[dict]:
    try:
        return await get_current_user(request)
    except HTTPException:
        return None

# ==================================================
# WEBSOCKET MANAGER
# ==================================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.user_connections: Dict[str, str] = {}  # user_id -> connection_id
    
    async def connect(self, websocket: WebSocket, user_id: str) -> str:
        await websocket.accept()
        conn_id = generate_id()
        self.active_connections[conn_id] = websocket
        self.user_connections[user_id] = conn_id
        return conn_id
    
    def disconnect(self, conn_id: str, user_id: str = None):
        if conn_id in self.active_connections:
            del self.active_connections[conn_id]
        if user_id and user_id in self.user_connections:
            del self.user_connections[user_id]
    
    async def send_message(self, user_id: str, message: dict):
        conn_id = self.user_connections.get(user_id)
        if conn_id and conn_id in self.active_connections:
            try:
                await self.active_connections[conn_id].send_json(message)
            except:
                pass
    
    async def broadcast(self, message: dict):
        for conn in self.active_connections.values():
            try:
                await conn.send_json(message)
            except:
                pass

manager = ConnectionManager()

# ==================================================
# FASTAPI APP
# ==================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 NexThread starting up...")
    # Ensure data files exist
    for f in ["users.json", "threads.json", "comments.json", "votes.json", 
              "notifications.json", "messages.json", "sessions.json", "reports.json",
              "bookmarks.json", "settings.json", "follows.json", "logs.json"]:
        if not await aiofiles.os.path.exists(DATA_DIR / f):
            await _write_json_file(f, [])
    yield
    # Shutdown
    print("👋 NexThread shutting down...")

app = FastAPI(
    title="NexThread API",
    description="The Future of Discussion",
    version="1.0.0",
    lifespan=lifespan
)

# ==================================================
# MIDDLEWARES
# ==================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"]
)

# ==================================================
# RATE LIMITING (sederhana)
# ==================================================

_rate_limit_cache: Dict[str, List[float]] = {}

async def check_rate_limit(identifier: str) -> bool:
    now = datetime.utcnow().timestamp()
    if identifier not in _rate_limit_cache:
        _rate_limit_cache[identifier] = []
    
    # Clean old entries
    _rate_limit_cache[identifier] = [t for t in _rate_limit_cache[identifier] if now - t < RATE_LIMIT_PERIOD]
    
    if len(_rate_limit_cache[identifier]) >= RATE_LIMIT_REQUESTS:
        return False
    
    _rate_limit_cache[identifier].append(now)
    return True

# ==================================================
# API ROUTES
# ==================================================

# --- Health ---

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "service": "NexThread", "timestamp": now_iso()}

# --- Auth ---

@app.post("/api/auth/register")
async def register(user_data: UserCreate):
    if not await check_rate_limit(f"register_{user_data.email}"):
        raise HTTPException(status_code=429, detail="Too many requests")
    try:
        user = await create_user(user_data.username, user_data.email, user_data.password)
        token = create_access_token({"sub": user["id"]})
        return {"token": token, "user": {k: v for k, v in user.items() if k != "hashed_password"}}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/auth/login")
async def login(login_data: UserLogin):
    if not await check_rate_limit(f"login_{login_data.email}"):
        raise HTTPException(status_code=429, detail="Too many requests")
    user = await authenticate_user(login_data.email, login_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_access_token({"sub": user["id"]})
    return {"token": token, "user": {k: v for k, v in user.items() if k != "hashed_password"}}

@app.get("/api/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    return {k: v for k, v in current_user.items() if k != "hashed_password"}

# --- Users ---

@app.get("/api/users")
async def list_users(limit: int = 20, offset: int = 0):
    users = await _read_json_file("users.json", [])
    if not isinstance(users, list):
        return []
    users = users[offset:offset + limit]
    return [{k: v for k, v in u.items() if k != "hashed_password"} for u in users]

@app.get("/api/users/{user_id}")
async def get_user(user_id: str):
    user = await get_user_public(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.get("/api/users/username/{username}")
async def get_user_by_username_route(username: str):
    user = await get_user_by_username(username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {k: v for k, v in user.items() if k != "hashed_password"}

@app.put("/api/users/me")
async def update_me(
    username: Optional[str] = Form(None),
    bio: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user)
):
    data = {}
    if username is not None:
        # Check username not taken
        existing = await get_user_by_username(username)
        if existing and existing["id"] != current_user["id"]:
            raise HTTPException(status_code=400, detail="Username already taken")
        data["username"] = username
    if bio is not None:
        data["bio"] = sanitize_html(bio)[:500]
    if not data:
        raise HTTPException(status_code=400, detail="No fields to update")
    await update_user(current_user["id"], data)
    return {"message": "Profile updated"}

@app.post("/api/users/me/avatar")
async def upload_avatar(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    # Validate file
    if file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=400, detail="File too large")
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="File type not allowed")
    
    # Save file
    filename = f"{current_user['id']}_{generate_id()[:8]}{ext}"
    path = UPLOAD_DIR / "avatars" / filename
    async with aiofiles.open(path, "wb") as f:
        content = await file.read()
        await f.write(content)
    
    avatar_url = f"/uploads/avatars/{filename}"
    await update_user(current_user["id"], {"avatar": avatar_url})
    return {"avatar": avatar_url}

@app.post("/api/users/{user_id}/follow")
async def follow_user_route(
    user_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await follow_user(current_user["id"], user_id)
    # Create notification
    await create_notification(
        user_id,
        "follow",
        f"{current_user['username']} started following you",
        {"follower_id": current_user["id"]}
    )
    return result

@app.delete("/api/users/{user_id}/follow")
async def unfollow_user_route(
    user_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await unfollow_user(current_user["id"], user_id)
    if not result:
        raise HTTPException(status_code=404, detail="Not following")
    return {"message": "Unfollowed"}

@app.get("/api/users/{user_id}/followers")
async def get_user_followers(user_id: str):
    return await get_followers(user_id)

@app.get("/api/users/{user_id}/following")
async def get_user_following(user_id: str):
    return await get_following(user_id)

@app.get("/api/users/me/followers")
async def get_my_followers(current_user: dict = Depends(get_current_user)):
    return await get_followers(current_user["id"])

@app.get("/api/users/me/following")
async def get_my_following(current_user: dict = Depends(get_current_user)):
    return await get_following(current_user["id"])

# --- Threads ---

@app.post("/api/threads")
async def create_thread_route(
    title: str = Form(...),
    content: str = Form(...),
    tags: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user)
):
    if not await check_rate_limit(f"thread_{current_user['id']}"):
        raise HTTPException(status_code=429, detail="Too many threads")
    tag_list = [t.strip() for t in tags.split(",")] if tags else []
    thread = await create_thread(current_user["id"], title, content, tag_list)
    return thread

@app.get("/api/threads")
async def list_threads(limit: int = 20, offset: int = 0, tag: Optional[str] = None):
    return await get_threads(limit, offset, tag)

@app.get("/api/threads/{thread_id}")
async def get_thread_route(thread_id: str):
    thread = await get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    # Get author
    author = await get_user_public(thread["user_id"])
    return {**thread, "author": author}

@app.put("/api/threads/{thread_id}")
async def update_thread_route(
    thread_id: str,
    title: Optional[str] = Form(None),
    content: Optional[str] = Form(None),
    is_pinned: Optional[bool] = Form(None),
    is_locked: Optional[bool] = Form(None),
    current_user: dict = Depends(get_current_user)
):
    data = {}
    if title is not None:
        data["title"] = title
    if content is not None:
        data["content"] = content
    if is_pinned is not None:
        data["is_pinned"] = is_pinned
    if is_locked is not None:
        data["is_locked"] = is_locked
    if not data:
        raise HTTPException(status_code=400, detail="No fields to update")
    result = await update_thread(thread_id, current_user["id"], data)
    if not result:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {"message": "Thread updated"}

@app.delete("/api/threads/{thread_id}")
async def delete_thread_route(
    thread_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await delete_thread(thread_id, current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {"message": "Thread deleted"}

# --- Comments ---

@app.post("/api/threads/{thread_id}/comments")
async def create_comment_route(
    thread_id: str,
    content: str = Form(...),
    parent_id: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user)
):
    if not await check_rate_limit(f"comment_{current_user['id']}"):
        raise HTTPException(status_code=429, detail="Too many comments")
    comment = await create_comment(current_user["id"], thread_id, content, parent_id)
    # Create notification for thread owner
    thread = await get_thread(thread_id)
    if thread and thread["user_id"] != current_user["id"]:
        await create_notification(
            thread["user_id"],
            "comment",
            f"{current_user['username']} commented on your thread",
            {"thread_id": thread_id, "comment_id": comment["id"]}
        )
    return comment

@app.get("/api/threads/{thread_id}/comments")
async def list_comments(thread_id: int, limit: int = 50, offset: int = 0):
    return await get_comments(str(thread_id), limit, offset)

@app.get("/api/threads/{thread_id}/comments/tree")
async def get_comment_tree_route(thread_id: str):
    return await get_comment_tree(thread_id)

@app.put("/api/comments/{comment_id}")
async def update_comment_route(
    comment_id: str,
    content: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    result = await update_comment(comment_id, current_user["id"], content)
    if not result:
        raise HTTPException(status_code=404, detail="Comment not found")
    return {"message": "Comment updated"}

@app.delete("/api/comments/{comment_id}")
async def delete_comment_route(
    comment_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await delete_comment(comment_id, current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="Comment not found")
    return {"message": "Comment deleted"}

# --- Votes ---

@app.post("/api/votes")
async def create_vote_route(
    target_type: str = Form(...),
    target_id: str = Form(...),
    value: int = Form(...),
    current_user: dict = Depends(get_current_user)
):
    if value not in [-1, 1]:
        raise HTTPException(status_code=400, detail="Value must be -1 or 1")
    result = await create_vote(current_user["id"], target_type, target_id, value)
    return result

@app.get("/api/votes/{target_type}/{target_id}")
async def get_vote_route(
    target_type: str,
    target_id: str,
    current_user: dict = Depends(get_current_user)
):
    vote = await get_vote(current_user["id"], target_type, target_id)
    return {"value": vote["value"] if vote else 0}

# --- Bookmarks ---

@app.post("/api/bookmarks/{thread_id}")
async def create_bookmark_route(
    thread_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await create_bookmark(current_user["id"], thread_id)
    return result

@app.delete("/api/bookmarks/{thread_id}")
async def delete_bookmark_route(
    thread_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await delete_bookmark(current_user["id"], thread_id)
    if not result:
        raise HTTPException(status_code=404, detail="Bookmark not found")
    return {"message": "Bookmark removed"}

@app.get("/api/bookmarks")
async def get_my_bookmarks(current_user: dict = Depends(get_current_user)):
    return await get_bookmarks(current_user["id"])

# --- Notifications ---

@app.get("/api/notifications")
async def get_my_notifications(
    limit: int = 20,
    current_user: dict = Depends(get_current_user)
):
    return await get_notifications(current_user["id"], limit)

@app.put("/api/notifications/{notification_id}/read")
async def mark_notification_read_route(
    notification_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await mark_notification_read(notification_id, current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"message": "Marked as read"}

@app.put("/api/notifications/read-all")
async def mark_all_notifications_read_route(
    current_user: dict = Depends(get_current_user)
):
    count = await mark_all_notifications_read(current_user["id"])
    return {"message": f"Marked {count} notifications as read"}

# --- Messages ---

@app.post("/api/messages")
async def send_message_route(
    receiver_id: str = Form(...),
    content: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    if not await check_rate_limit(f"message_{current_user['id']}"):
        raise HTTPException(status_code=429, detail="Too many messages")
    msg = await create_message(current_user["id"], receiver_id, content)
    # Send realtime via websocket
    await manager.send_message(receiver_id, {
        "type": "message",
        "data": msg
    })
    return msg

@app.get("/api/messages/conversations")
async def get_conversations_route(current_user: dict = Depends(get_current_user)):
    return await get_conversations(current_user["id"])

@app.get("/api/messages/{user_id}")
async def get_messages_with_user(
    user_id: str,
    limit: int = 50,
    current_user: dict = Depends(get_current_user)
):
    return await get_messages(current_user["id"], user_id, limit)

@app.put("/api/messages/{message_id}/read")
async def mark_message_read_route(
    message_id: str,
    current_user: dict = Depends(get_current_user)
):
    result = await mark_message_read(message_id, current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="Message not found")
    return {"message": "Marked as read"}

# --- Search ---

@app.get("/api/search")
async def search_route(q: str, limit: int = 20):
    if not q or len(q) < 2:
        return {"threads": [], "users": []}
    return await search(q, limit)

# --- Reports ---

@app.post("/api/reports")
async def create_report_route(
    target_type: str = Form(...),
    target_id: str = Form(...),
    reason: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    if not await check_rate_limit(f"report_{current_user['id']}"):
        raise HTTPException(status_code=429, detail="Too many reports")
    report = await create_report(current_user["id"], target_type, target_id, reason)
    return report

@app.get("/api/reports")
async def list_reports(
    status: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    if not current_user.get("is_moderator") and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Not authorized")
    return await get_reports(status)

@app.put("/api/reports/{report_id}")
async def update_report_status_route(
    report_id: str,
    status: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    result = await update_report_status(report_id, status, current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="Report not found")
    return {"message": "Report updated"}

# --- Admin ---

@app.get("/api/admin/stats")
async def get_stats(current_user: dict = Depends(get_current_user)):
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Not authorized")
    return await get_system_stats()

@app.post("/api/admin/users/{user_id}/promote")
async def promote_user_route(
    user_id: str,
    role: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    result = await promote_user(user_id, role, current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": f"User promoted to {role}"}

@app.post("/api/admin/users/{user_id}/demote")
async def demote_user_route(
    user_id: str,
    role: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    result = await demote_user(user_id, role, current_user["id"])
    if not result:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": f"User demoted from {role}"}

# --- Uploads (static files) ---

@app.get("/uploads/{folder}/{filename}")
async def serve_upload(folder: str, filename: str):
    path = UPLOAD_DIR / folder / filename
    if not await aiofiles.os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path)

# --- WebSocket ---

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Get token from query params
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return
    
    payload = decode_access_token(token)
    if not payload:
        await websocket.close(code=1008)
        return
    
    user_id = payload.get("sub")
    user = await get_user_by_id(user_id)
    if not user:
        await websocket.close(code=1008)
        return
    
    conn_id = await manager.connect(websocket, user_id)
    
    try:
        # Send initial connection success
        await websocket.send_json({"type": "connected", "user_id": user_id})
        
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            
            if msg_type == "ping":
                await websocket.send_json({"type": "pong"})
            elif msg_type == "message":
                # Handle direct message via websocket
                receiver_id = data.get("receiver_id")
                content = data.get("content")
                if receiver_id and content:
                    msg = await create_message(user_id, receiver_id, content)
                    await manager.send_message(receiver_id, {
                        "type": "message",
                        "data": msg
                    })
                    await websocket.send_json({"type": "message_sent", "data": msg})
            elif msg_type == "typing":
                receiver_id = data.get("receiver_id")
                if receiver_id:
                    await manager.send_message(receiver_id, {
                        "type": "typing",
                        "sender_id": user_id
                    })
    except WebSocketDisconnect:
        manager.disconnect(conn_id, user_id)
    except Exception as e:
        manager.disconnect(conn_id, user_id)
        print(f"WebSocket error: {e}")

# --- Root ---

@app.get("/")
async def root():
    return {
        "service": "NexThread API",
        "version": "1.0.0",
        "tagline": "The Future of Discussion",
        "docs": "/docs",
        "health": "/api/health"
    }

# ==================================================
# EXPORT untuk Vercel
# ==================================================

# Vercel mencari variabel bernama 'app'
# File ini di api/index.py sudah sesuai
