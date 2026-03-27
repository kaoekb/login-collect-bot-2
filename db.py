import re
from datetime import datetime
from typing import Any

import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


def _utc_now() -> datetime:
    return datetime.utcnow()


def _current_month_key() -> str:
    return _utc_now().strftime("%Y-%m")


class MongoRepository:
    def __init__(
        self,
        mongo_uri: str,
        db_name: str,
        users_collection: str,
        stats_collection: str,
        groups_collection: str,
    ) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.users: Collection = self.db[users_collection]
        self.stats: Collection = self.db[stats_collection]
        self.groups: Collection = self.db[groups_collection]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.users.create_index("user_id", unique=True)
            self.users.create_index("login_school")
            self.users.create_index("login_tg")
            self.users.create_index("registered_at")
            self.stats.create_index("month", unique=True)
            self.groups.create_index("group_id", unique=True)
        except PyMongoError:
            # Индексы помогают производительности, но бот должен стартовать даже если индекс не создался.
            pass

    @staticmethod
    def registered_query() -> dict[str, Any]:
        return {
            "login_school": {"$exists": True, "$ne": ""},
            "$or": [{"verified": True}, {"verified": {"$exists": False}}],
        }

    def touch_user(self, user_id: int, login_tg: str | None) -> None:
        now = _utc_now()
        update: dict[str, Any] = {"last_seen_at": now, "updated_at": now}
        if login_tg:
            update["login_tg"] = login_tg
        self.users.update_one(
            {"user_id": user_id},
            {"$set": update, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )

    def migrate_legacy_users(self) -> int:
        result = self.users.update_many(
            {
                "login_school": {"$exists": True, "$ne": ""},
                "verified": {"$exists": False},
            },
            {
                "$set": {
                    "verified": True,
                    "registration_status": "legacy_verified",
                    "updated_at": _utc_now(),
                },
                "$unset": {
                    "pending_pin": "",
                    "pending_pin_expires_at": "",
                    "pending_login_school": "",
                },
            },
        )
        return int(result.modified_count)

    def get_user(self, user_id: int) -> dict[str, Any] | None:
        return self.users.find_one({"user_id": user_id})

    @staticmethod
    def is_registered_user(user: dict[str, Any] | None) -> bool:
        if not user:
            return False
        if not user.get("login_school"):
            return False
        return bool(user.get("verified")) or "verified" not in user

    def promote_legacy_user(self, user_id: int, login_tg: str | None = None) -> None:
        user = self.get_user(user_id)
        if not user or not user.get("login_school"):
            return
        if user and user.get("verified"):
            return

        update_set: dict[str, Any] = {
            "verified": True,
            "registration_status": user.get("registration_status") or "legacy_verified",
            "updated_at": _utc_now(),
        }
        if login_tg:
            update_set["login_tg"] = login_tg
        if not user.get("registered_at"):
            update_set["registered_at"] = _utc_now()

        self.users.update_one(
            {"user_id": user_id},
            {
                "$set": update_set,
                "$unset": {
                    "pending_pin": "",
                    "pending_pin_expires_at": "",
                    "pending_login_school": "",
                },
            },
            upsert=False,
        )

    def set_pending_registration(
        self,
        user_id: int,
        login_school: str,
        login_tg: str,
        pin: str,
        pin_ttl_seconds: int,
    ) -> None:
        now = _utc_now()
        expires_at = datetime.fromtimestamp(now.timestamp() + pin_ttl_seconds)
        self.users.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "pending_login_school": login_school,
                    "pending_pin": pin,
                    "pending_pin_expires_at": expires_at,
                    "registration_status": "pending",
                    "login_tg": login_tg,
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "verified": False,
                },
            },
            upsert=True,
        )

    def register_user(self, user_id: int, login_school: str, login_tg: str) -> None:
        user = self.get_user(user_id) or {}
        now = _utc_now()
        was_verified = bool(user.get("verified"))
        self.users.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "login_school": login_school,
                    "login_tg": login_tg,
                    "verified": True,
                    "registration_status": "verified",
                    "updated_at": now,
                    "registered_at": user.get("registered_at") or now,
                },
                "$unset": {
                    "pending_pin": "",
                    "pending_pin_expires_at": "",
                    "pending_login_school": "",
                },
                "$setOnInsert": {
                    "created_at": now,
                },
            },
            upsert=True,
        )

        if not was_verified:
            self.increment_new_users()

    def verify_pin(self, user_id: int, entered_pin: str, current_login_tg: str) -> str:
        user = self.get_user(user_id)
        now = _utc_now()

        if not user or not user.get("pending_pin"):
            return "no_pending"

        expires_at = user.get("pending_pin_expires_at")
        if expires_at and now > expires_at:
            return "expired"

        if user.get("pending_pin") != entered_pin:
            return "invalid"

        was_verified = bool(user.get("verified"))
        pending_login_school = user.get("pending_login_school")
        if not pending_login_school:
            return "no_pending"

        self.users.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "login_school": pending_login_school,
                    "login_tg": current_login_tg,
                    "verified": True,
                    "registration_status": "verified",
                    "updated_at": now,
                    "registered_at": user.get("registered_at") or now,
                },
                "$unset": {
                    "pending_pin": "",
                    "pending_pin_expires_at": "",
                    "pending_login_school": "",
                },
            },
            upsert=True,
        )

        if not was_verified:
            self.increment_new_users()
        return "ok"

    def clear_pending_registration(self, user_id: int) -> None:
        self.users.update_one(
            {"user_id": user_id},
            {
                "$unset": {
                    "pending_pin": "",
                    "pending_pin_expires_at": "",
                    "pending_login_school": "",
                },
                "$set": {"registration_status": "draft", "updated_at": _utc_now()},
            },
        )

    def delete_user(self, user_id: int) -> int:
        result = self.users.delete_one({"user_id": user_id})
        return result.deleted_count

    def find_by_login(self, login: str) -> dict[str, Any] | None:
        query = {
            "$and": [
                {
                    "$or": [
                        {"login_school": {"$eq": login}},
                        {"login_tg": {"$eq": login}},
                    ]
                },
                self.registered_query(),
            ]
        }
        user = self.users.find_one(query)
        if user:
            self.users.update_one(
                {"_id": user["_id"]},
                {"$set": {"last_access_in": _utc_now(), "updated_at": _utc_now()}},
            )
        return user

    def search_users_for_admin(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        query = query.strip()
        if not query:
            return []

        mongo_query: dict[str, Any]
        if query.isdigit():
            mongo_query = {"user_id": int(query)}
        else:
            normalized = query.lower().lstrip("@")
            escaped = re.escape(normalized)
            mongo_query = {
                "$or": [
                    {"login_school": {"$regex": f"^{escaped}"}},
                    {"login_tg": {"$regex": f"^{escaped}"}},
                ]
            }

        cursor = self.users.find(mongo_query).sort("registered_at", -1).limit(limit)
        return list(cursor)

    def increment_requests(self, source: str) -> None:
        month = _current_month_key()
        field = "bot_requests_this_month" if source == "bot" else "group_requests_this_month"
        self.stats.update_one(
            {"month": month},
            {
                "$inc": {
                    "total_requests": 1,
                    "requests_this_month": 1,
                    field: 1,
                },
                "$setOnInsert": {
                    "new_users_this_month": 0,
                    "created_at": _utc_now(),
                },
            },
            upsert=True,
        )

    def increment_new_users(self) -> None:
        month = _current_month_key()
        self.stats.update_one(
            {"month": month},
            {
                "$inc": {"new_users_this_month": 1},
                "$setOnInsert": {
                    "total_requests": 0,
                    "requests_this_month": 0,
                    "bot_requests_this_month": 0,
                    "group_requests_this_month": 0,
                    "created_at": _utc_now(),
                },
            },
            upsert=True,
        )

    def get_stats(self) -> dict[str, int]:
        month = _current_month_key()
        stat_doc = self.stats.find_one({"month": month}) or {}

        return {
            "month": month,
            "total_users": self.users.count_documents(self.registered_query()),
            "new_users_this_month": int(stat_doc.get("new_users_this_month", 0)),
            "total_requests": int(stat_doc.get("total_requests", 0)),
            "requests_this_month": int(stat_doc.get("requests_this_month", 0)),
            "bot_requests_this_month": int(stat_doc.get("bot_requests_this_month", 0)),
            "group_requests_this_month": int(stat_doc.get("group_requests_this_month", 0)),
        }

    def export_users_to_excel(self, file_path: str) -> None:
        cursor = self.users.find({}, {"_id": 0, "pending_pin": 0})
        rows = list(cursor)
        df = pd.DataFrame(rows)
        df.to_excel(file_path, index=False)

    def get_recent_registrations(self, limit: int = 10) -> list[dict[str, Any]]:
        cursor = (
            self.users.find(self.registered_query())
            .sort("registered_at", -1)
            .limit(limit)
        )
        return list(cursor)

    def set_group_state(self, group_id: int, active: bool) -> None:
        self.groups.update_one(
            {"group_id": group_id},
            {
                "$set": {
                    "active": active,
                    "updated_at": _utc_now(),
                },
                "$setOnInsert": {"created_at": _utc_now()},
            },
            upsert=True,
        )

    def is_group_active(self, group_id: int) -> bool:
        doc = self.groups.find_one({"group_id": group_id})
        return bool(doc and doc.get("active"))
