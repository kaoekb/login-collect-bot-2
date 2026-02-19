import os
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv


load_dotenv(find_dotenv())


def _parse_admin_ids(raw_admin_ids: str, legacy_single_admin: str) -> set[int]:
    admin_ids: set[int] = set()

    for item in (raw_admin_ids or "").split(","):
        item = item.strip()
        if item.isdigit():
            admin_ids.add(int(item))

    if legacy_single_admin and legacy_single_admin.strip().isdigit():
        admin_ids.add(int(legacy_single_admin.strip()))

    return admin_ids


def _parse_bool(raw_value: str | None, default: bool) -> bool:
    if raw_value is None:
        return default
    value = raw_value.strip().lower()
    return value in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    bot_token: str
    mongo_uri: str
    mongo_db: str
    users_collection: str
    stats_collection: str
    groups_collection: str
    admin_ids: set[int]
    admin_contact: str
    sender_email: str
    sender_password: str
    smtp_host: str
    smtp_port: int
    smtp_use_tls: bool
    enable_relay_test: bool
    relay_max_len: int
    pin_length: int
    pin_ttl_seconds: int
    log_file: str

    @staticmethod
    def from_env() -> "Settings":
        bot_token = os.getenv("BOT_TOKEN") or os.getenv("Token_tg")
        mongo_uri = os.getenv("MONGO_URI") or os.getenv("Token_MDB")

        if not bot_token:
            raise ValueError("BOT_TOKEN (или Token_tg) не задан в окружении")
        if not mongo_uri:
            raise ValueError("MONGO_URI (или Token_MDB) не задан в окружении")

        return Settings(
            bot_token=bot_token,
            mongo_uri=mongo_uri,
            mongo_db=os.getenv("MONGO_DB", "Users_school_21"),
            users_collection=os.getenv("MONGO_USERS_COLLECTION", "login"),
            stats_collection=os.getenv("MONGO_STATS_COLLECTION", "users"),
            groups_collection=os.getenv("MONGO_GROUPS_COLLECTION", "group_states"),
            admin_ids=_parse_admin_ids(
                os.getenv("ADMIN_IDS", ""),
                os.getenv("Your_user_ID", ""),
            ),
            admin_contact=os.getenv("ADMIN_CONTACT", "@kaoekb"),
            sender_email=os.getenv("sender", ""),
            sender_password=os.getenv("mail_password", ""),
            smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_use_tls=_parse_bool(os.getenv("SMTP_USE_TLS"), True),
            enable_relay_test=_parse_bool(os.getenv("ENABLE_RELAY_TEST"), False),
            relay_max_len=max(1, int(os.getenv("RELAY_MAX_LEN", "700"))),
            pin_length=int(os.getenv("PIN_LENGTH", "4")),
            pin_ttl_seconds=int(os.getenv("PIN_TTL_SECONDS", "300")),
            log_file=os.getenv("LOG_FILE", "bot.log"),
        )
