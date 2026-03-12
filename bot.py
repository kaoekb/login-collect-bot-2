import html
import logging
import os
import random
import re
import time
from collections.abc import Callable
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any

import telebot
from telebot.apihelper import ApiTelegramException
from telebot import types
from telebot.types import BotCommand, BotCommandScopeChat, BotCommandScopeDefault

from config import Settings
from db import MongoRepository
from mailer import Mailer


STATE_WAITING_LOGIN = "waiting_login"
STATE_WAITING_PIN = "waiting_pin"
STATE_ADMIN_FIND = "admin_find"
STATE_RELAY_MESSAGE = "relay_message"

PIN_HINT = "Код состоит из 4 цифр и действует 5 минут."
PEER_LOOKUP_PROMPT = "Введи школьный или телеграм ник интересующего тебя пира."
POLLING_CONFLICT_DELAY_SECONDS = 30


def build_logger(log_file: str) -> logging.Logger:
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("login_collect_bot_2")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    file_handler = RotatingFileHandler(
        log_file,
        mode="a",
        maxBytes=5 * 1024 * 1024,
        backupCount=2,
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


settings = Settings.from_env()
logger = build_logger(settings.log_file)
bot = telebot.TeleBot(settings.bot_token)
repo = MongoRepository(
    mongo_uri=settings.mongo_uri,
    db_name=settings.mongo_db,
    users_collection=settings.users_collection,
    stats_collection=settings.stats_collection,
    groups_collection=settings.groups_collection,
)
legacy_migrated = repo.migrate_legacy_users()
if legacy_migrated:
    logger.info("Legacy users migrated: %s", legacy_migrated)

mailer = Mailer(
    settings.sender_email,
    settings.sender_password,
    settings.smtp_host,
    settings.smtp_port,
    settings.smtp_use_tls,
)

user_states: dict[int, str] = {}
user_payloads: dict[int, dict[str, Any]] = {}


def normalize_login(raw: str) -> str:
    return raw.strip().lower().lstrip("@")


def is_valid_school_login(login: str) -> bool:
    return bool(re.fullmatch(r"[a-z0-9._-]{2,32}", login))


def is_admin(user_id: int) -> bool:
    return user_id in settings.admin_ids


def generate_pin() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(settings.pin_length))


def clear_state(user_id: int) -> None:
    user_states.pop(user_id, None)
    user_payloads.pop(user_id, None)


def set_state(user_id: int, state: str, payload: dict[str, Any] | None = None) -> None:
    user_states[user_id] = state
    user_payloads[user_id] = payload or {}


def format_lookup_message(user: dict[str, Any]) -> str:
    login_school = html.escape(user.get("login_school", "unknown"))
    login_tg = user.get("login_tg")
    tg_part = f"@{html.escape(login_tg)}" if login_tg else "не указан"
    profile_url = f"https://platform.21-school.ru/profile/{user.get('login_school', '')}"
    return (
        f"Login school: <a href='{profile_url}'>{login_school}</a>\n"
        f"Login tg: {tg_part}"
    )


def is_timeout_error(exc: BaseException) -> bool:
    return "timed out" in str(exc).lower()


def is_callback_query_expired(exc: ApiTelegramException) -> bool:
    text = str(exc).lower()
    return "query is too old" in text or "query id is invalid" in text


def is_polling_conflict(exc: ApiTelegramException) -> bool:
    return "terminated by other getupdates request" in str(exc).lower()


def safe_telegram_call(action: str, func: Callable[[], Any]) -> Any | None:
    try:
        return func()
    except ApiTelegramException as exc:
        if is_callback_query_expired(exc):
            logger.info("Skipping expired callback during %s: %s", action, exc)
        else:
            logger.warning("Telegram API error during %s: %s", action, exc)
    except Exception as exc:
        if is_timeout_error(exc):
            logger.warning("Telegram timeout during %s: %s", action, exc)
        else:
            logger.exception("Unexpected Telegram call failure during %s", action)
    return None


def send_message_safe(chat_id: int, text: str, **kwargs: Any) -> bool:
    return (
        safe_telegram_call(
            f"send_message chat_id={chat_id}",
            lambda: bot.send_message(chat_id, text, **kwargs),
        )
        is not None
    )


def send_document_safe(chat_id: int, document: Any, **kwargs: Any) -> bool:
    return (
        safe_telegram_call(
            f"send_document chat_id={chat_id}",
            lambda: bot.send_document(chat_id, document, **kwargs),
        )
        is not None
    )


def answer_callback_query_safe(callback_query_id: str, text: str | None = None, **kwargs: Any) -> bool:
    return (
        safe_telegram_call(
            "answer_callback_query",
            lambda: bot.answer_callback_query(callback_query_id, text, **kwargs),
        )
        is not None
    )


def send_peer_lookup_prompt(chat_id: int) -> None:
    send_message_safe(chat_id, PEER_LOOKUP_PROMPT)


def format_sender_signature(sender_user: dict[str, Any]) -> str:
    sender_school = html.escape(sender_user.get("login_school", "unknown"))
    sender_tg = sender_user.get("login_tg")
    sender_tg_text = f"@{html.escape(sender_tg)}" if sender_tg else "без username"
    return f"{sender_school} | {sender_tg_text}"


def build_relay_message(sender_user: dict[str, Any], sender_id: int, text: str) -> str:
    sender_signature = format_sender_signature(sender_user)
    safe_text = html.escape(text)
    return (
        "Тебе написал пользователь через @login_school21_bot.\n"
        f"Отправитель: {sender_signature}\n\n"
        f"{safe_text}"
    )


def send_relay_offer(chat_id: int, target_user_id: int) -> None:
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton(
            "Ник неактуален? Написать через бота",
            callback_data=f"relay:start:{target_user_id}",
        )
    )
    send_message_safe(
        chat_id,
        "Если Telegram-ник устарел, можно отправить сообщение пользователю через бота.",
        reply_markup=kb,
    )


def send_relay_preview(chat_id: int, preview_text: str) -> None:
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("Отправить", callback_data="relay:send"),
        types.InlineKeyboardButton("Изменить", callback_data="relay:edit"),
    )
    kb.row(types.InlineKeyboardButton("Отмена", callback_data="relay:cancel"))
    send_message_safe(chat_id, "Предпросмотр сообщения:")
    send_message_safe(chat_id, preview_text, parse_mode="HTML", reply_markup=kb)


def build_admin_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("Статистика", callback_data="admin:stats"),
        types.InlineKeyboardButton("Экспорт Excel", callback_data="admin:export"),
    )
    kb.row(
        types.InlineKeyboardButton("Последние регистрации", callback_data="admin:recent"),
        types.InlineKeyboardButton("Поиск пользователя", callback_data="admin:find"),
    )
    kb.row(
        types.InlineKeyboardButton("Обновить", callback_data="admin:refresh"),
        types.InlineKeyboardButton("Закрыть", callback_data="admin:close"),
    )
    return kb


def send_admin_panel(chat_id: int) -> None:
    send_message_safe(
        chat_id,
        "Админ-панель: выберите действие.",
        reply_markup=build_admin_keyboard(),
    )


def set_bot_commands() -> None:
    default_commands = [
        BotCommand("start", "Старт и регистрация"),
        BotCommand("register", "Регистрация заново"),
        BotCommand("help", "Помощь"),
        BotCommand("delete", "Удалить мой логин"),
        BotCommand("bot", "Поиск в группе: /bot <логин>"),
        BotCommand("stop", "Остановить бота в группе"),
    ]
    safe_telegram_call(
        "set_my_commands default",
        lambda: bot.set_my_commands(default_commands, scope=BotCommandScopeDefault()),
    )

    admin_commands = default_commands + [
        BotCommand("admin", "Открыть админку"),
        BotCommand("stat", "Краткая статистика"),
        BotCommand("user", "Экспорт пользователей"),
        BotCommand("log", "Логи бота"),
    ]
    for admin_id in settings.admin_ids:
        try:
            bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(admin_id))
        except ApiTelegramException as exc:
            logger.warning(
                "Cannot set admin commands for admin_id=%s (%s). "
                "Make sure this user started the bot and id is correct.",
                admin_id,
                exc,
            )
        except Exception as exc:
            if is_timeout_error(exc):
                logger.warning("Timeout while setting admin commands for admin_id=%s: %s", admin_id, exc)
            else:
                logger.exception("Failed to set admin commands for admin_id=%s", admin_id)


def start_registration(chat_id: int, user_id: int) -> None:
    set_state(user_id, STATE_WAITING_LOGIN)
    send_message_safe(
        chat_id,
        "Введите ваш школьный логин (без @student.21-school.ru).\n"
        "Пример: `thebestpeer`",
        parse_mode="Markdown",
    )


def handle_school_login_input(message: types.Message, login_raw: str) -> None:
    user_id = message.from_user.id
    chat_id = message.chat.id
    login_school = normalize_login(login_raw)

    if not is_valid_school_login(login_school):
        send_message_safe(
            chat_id,
            "Некорректный логин. Разрешены латиница, цифры, точка, дефис, подчеркивание.",
        )
        return

    login_tg = (message.from_user.username or "").strip().lower()
    if not login_tg:
        send_message_safe(
            chat_id,
            "Для регистрации нужен Telegram username. Создайте его в настройках Telegram и повторите /start.",
        )
        return

    pin = generate_pin()
    sent, status = mailer.send_pin(login_school, pin)
    if not sent:
        logger.error("PIN email send error: %s", status)
        send_message_safe(chat_id, "Не удалось отправить письмо. Попробуйте позже.")
        return

    repo.set_pending_registration(
        user_id=user_id,
        login_school=login_school,
        login_tg=login_tg,
        pin=pin,
        pin_ttl_seconds=settings.pin_ttl_seconds,
    )
    set_state(user_id, STATE_WAITING_PIN, {"pending_login_school": login_school})

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Отправить код повторно", callback_data="reg:resend"))
    send_message_safe(
        chat_id,
        f"Код подтверждения отправлен на {login_school}@student.21-school.ru.\n"
        f"Введите код в этот чат.\n{PIN_HINT}",
        reply_markup=kb,
    )


def handle_pin_input(message: types.Message, pin_raw: str) -> None:
    user_id = message.from_user.id
    chat_id = message.chat.id
    pin = pin_raw.strip()

    if not pin.isdigit() or len(pin) != settings.pin_length:
        send_message_safe(chat_id, f"Неверный формат кода. {PIN_HINT}")
        return

    login_tg = (message.from_user.username or "").strip().lower()
    if not login_tg:
        send_message_safe(
            chat_id,
            "У вас отсутствует username в Telegram. Добавьте его в настройках и начните регистрацию заново: /start",
        )
        return

    status = repo.verify_pin(user_id=user_id, entered_pin=pin, current_login_tg=login_tg)
    if status == "ok":
        clear_state(user_id)
        send_message_safe(chat_id, "Регистрация подтверждена. Теперь можете искать пользователей по нику.")
        send_peer_lookup_prompt(chat_id)
        return

    if status == "expired":
        repo.clear_pending_registration(user_id)
        clear_state(user_id)
        send_message_safe(chat_id, "Срок действия кода истек. Начните заново: /start")
        return

    if status == "invalid":
        send_message_safe(chat_id, "Неверный код. Попробуйте еще раз.")
        return

    clear_state(user_id)
    send_message_safe(chat_id, "Нет активной регистрации. Используйте /start")


def send_stats(chat_id: int) -> None:
    stats = repo.get_stats()
    text = (
        f"Статистика за {stats['month']}:\n"
        f"- Подтвержденных пользователей: {stats['total_users']}\n"
        f"- Новых пользователей в месяце: {stats['new_users_this_month']}\n"
        f"- Запросов всего: {stats['total_requests']}\n"
        f"- Запросов в этом месяце: {stats['requests_this_month']}\n"
        f"- Запросов в ЛС: {stats['bot_requests_this_month']}\n"
        f"- Запросов в группах: {stats['group_requests_this_month']}"
    )
    send_message_safe(chat_id, text)


def send_users_export(chat_id: int) -> None:
    file_name = f"users_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    repo.export_users_to_excel(file_name)
    try:
        with open(file_name, "rb") as file:
            send_document_safe(chat_id, file)
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)


def send_recent_registrations(chat_id: int) -> None:
    users = repo.get_recent_registrations(limit=10)
    if not users:
        send_message_safe(chat_id, "Подтвержденных пользователей пока нет.")
        return

    lines = ["Последние регистрации:"]
    for user in users:
        registered_at = user.get("registered_at")
        date_str = registered_at.strftime("%Y-%m-%d %H:%M") if registered_at else "n/a"
        lines.append(
            f"- ID {user.get('user_id')}: {user.get('login_school', 'n/a')} | "
            f"@{user.get('login_tg', 'n/a')} | {date_str}"
        )
    send_message_safe(chat_id, "\n".join(lines))


@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message) -> None:
    try:
        if message.chat.type in ["group", "supergroup"]:
            repo.set_group_state(message.chat.id, True)
            send_message_safe(message.chat.id, "Бот активирован. Используйте /bot <логин>.")
            return

        repo.increment_requests("bot")
        tg_username = (message.from_user.username or "").strip().lower()
        repo.touch_user(message.from_user.id, tg_username)

        user = repo.get_user(message.from_user.id)
        if repo.is_registered_user(user):
            repo.promote_legacy_user(message.from_user.id, tg_username)
            clear_state(message.from_user.id)
            send_message_safe(message.chat.id, "Вы уже зарегистрированы.")
            send_peer_lookup_prompt(message.chat.id)
        else:
            start_registration(message.chat.id, message.from_user.id)
    except Exception:
        logger.exception("cmd_start failed")
        send_message_safe(message.chat.id, "Ошибка обработки /start. Попробуйте позже.")


@bot.message_handler(commands=["register"])
def cmd_register(message: types.Message) -> None:
    if message.chat.type != "private":
        send_message_safe(message.chat.id, "Команда доступна только в личных сообщениях.")
        return
    repo.increment_requests("bot")
    start_registration(message.chat.id, message.from_user.id)


@bot.message_handler(commands=["help"])
def cmd_help(message: types.Message) -> None:
    source = "group" if message.chat.type in ["group", "supergroup"] else "bot"
    repo.increment_requests(source)

    if message.chat.type in ["group", "supergroup"]:
        text = (
            "Групповой режим:\n"
            "- /start активирует бота в группе\n"
            "- /stop отключает бота в группе\n"
            "- /bot <логин> ищет пользователя\n"
            "- Регистрация выполняется в личном чате с ботом"
        )
    else:
        text = (
            "Личный режим:\n"
            "- /start для регистрации\n"
            "- /register для перезапуска регистрации\n"
            "- /delete удаляет ваши данные\n"
            "- после регистрации отправьте логин пользователя для поиска\n"
            f"- если есть проблема, напишите администратору {settings.admin_contact}"
        )
    send_message_safe(message.chat.id, text)


@bot.message_handler(commands=["delete"])
def cmd_delete(message: types.Message) -> None:
    if message.chat.type != "private":
        send_message_safe(message.chat.id, "Команда доступна только в личных сообщениях.")
        return

    repo.increment_requests("bot")
    user = repo.get_user(message.from_user.id)
    if not user:
        send_message_safe(message.chat.id, "Ваших данных в базе нет.")
        return

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("Да, удалить", callback_data="delete:yes"),
        types.InlineKeyboardButton("Отмена", callback_data="delete:no"),
    )
    send_message_safe(message.chat.id, "Удалить ваш логин и регистрацию?", reply_markup=kb)


@bot.message_handler(commands=["bot"])
def cmd_bot_lookup(message: types.Message) -> None:
    if message.chat.type not in ["group", "supergroup"]:
        send_message_safe(message.chat.id, "Команда /bot доступна только в группах.")
        return

    if not repo.is_group_active(message.chat.id):
        return

    repo.increment_requests("group")
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        send_message_safe(message.chat.id, "Использование: /bot <логин>")
        return

    login = normalize_login(parts[1])
    user = repo.find_by_login(login)
    if not user:
        send_message_safe(message.chat.id, "Логин не найден.")
        return

    tg_username = user.get("login_tg")
    tg_part = f"@{tg_username}" if tg_username else "не указан"
    send_message_safe(
        message.chat.id,
        f"Login school: {user.get('login_school', 'n/a')}, login tg: {tg_part}",
    )


@bot.message_handler(commands=["stop"])
def cmd_stop(message: types.Message) -> None:
    if message.chat.type not in ["group", "supergroup"]:
        send_message_safe(message.chat.id, "Команда /stop доступна только в группах.")
        return

    repo.set_group_state(message.chat.id, False)
    send_message_safe(message.chat.id, "Бот деактивирован в этой группе.")


@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message) -> None:
    if message.chat.type != "private":
        send_message_safe(message.chat.id, "Админка доступна только в личных сообщениях.")
        return
    if not is_admin(message.from_user.id):
        send_message_safe(message.chat.id, "Недостаточно прав.")
        return

    repo.increment_requests("bot")
    send_admin_panel(message.chat.id)


@bot.message_handler(commands=["stat"])
def cmd_stat(message: types.Message) -> None:
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        send_message_safe(message.chat.id, "Недостаточно прав.")
        return

    repo.increment_requests("bot")
    send_stats(message.chat.id)


@bot.message_handler(commands=["user"])
def cmd_user_export(message: types.Message) -> None:
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        send_message_safe(message.chat.id, "Недостаточно прав.")
        return

    repo.increment_requests("bot")
    send_users_export(message.chat.id)


@bot.message_handler(commands=["log"])
def cmd_log(message: types.Message) -> None:
    if message.chat.type != "private" or not is_admin(message.from_user.id):
        send_message_safe(message.chat.id, "Недостаточно прав.")
        return

    repo.increment_requests("bot")
    if not os.path.exists(settings.log_file):
        send_message_safe(message.chat.id, "Файл логов не найден.")
        return
    with open(settings.log_file, "rb") as file:
        send_document_safe(message.chat.id, file)


@bot.callback_query_handler(func=lambda call: True)
def callback_router(call: types.CallbackQuery) -> None:
    data = call.data or ""
    user_id = call.from_user.id
    chat_id = call.message.chat.id

    if data.startswith("delete:"):
        if data == "delete:yes":
            deleted = repo.delete_user(user_id)
            clear_state(user_id)
            if deleted:
                send_message_safe(chat_id, "Ваши данные удалены.")
            else:
                send_message_safe(chat_id, "Данные не найдены.")
        else:
            send_message_safe(chat_id, "Удаление отменено.")
        answer_callback_query_safe(call.id)
        return

    if data == "reg:resend":
        user = repo.get_user(user_id) or {}
        pending_login_school = user.get("pending_login_school")
        if not pending_login_school:
            answer_callback_query_safe(call.id, "Нет активной регистрации.")
            return

        answer_callback_query_safe(call.id, "Отправляю код...")
        pin = generate_pin()
        sent, status = mailer.send_pin(pending_login_school, pin)
        if not sent:
            logger.error("PIN resend error: %s", status)
            send_message_safe(chat_id, "Не удалось отправить письмо. Попробуйте позже.")
            return

        login_tg = (call.from_user.username or "").strip().lower()
        repo.set_pending_registration(
            user_id=user_id,
            login_school=pending_login_school,
            login_tg=login_tg,
            pin=pin,
            pin_ttl_seconds=settings.pin_ttl_seconds,
        )
        set_state(user_id, STATE_WAITING_PIN, {"pending_login_school": pending_login_school})
        send_message_safe(chat_id, f"Новый код отправлен на {pending_login_school}@student.21-school.ru.")
        return

    if data.startswith("relay:"):
        if not settings.enable_relay_test:
            answer_callback_query_safe(call.id, "Функция временно отключена.")
            return

        action, _, arg = data.partition(":")
        relay_action = arg.split(":", maxsplit=1)[0]

        if relay_action == "start":
            parts = data.split(":")
            if len(parts) != 3 or not parts[2].isdigit():
                answer_callback_query_safe(call.id, "Некорректный запрос.")
                return

            target_user_id = int(parts[2])
            if target_user_id == user_id:
                answer_callback_query_safe(call.id, "Нельзя отправить сообщение самому себе.")
                return

            target_user = repo.get_user(target_user_id)
            if not repo.is_registered_user(target_user):
                answer_callback_query_safe(call.id, "Пользователь недоступен для отправки.")
                return

            set_state(
                user_id,
                STATE_RELAY_MESSAGE,
                {
                    "target_user_id": target_user_id,
                },
            )
            target_login = target_user.get("login_school", "unknown")
            send_message_safe(
                chat_id,
                f"Напиши сообщение для {target_login}. Сначала покажу предпросмотр, потом отправим.",
            )
            answer_callback_query_safe(call.id)
            return

        payload = user_payloads.get(user_id, {})
        state = user_states.get(user_id)
        if state != STATE_RELAY_MESSAGE:
            answer_callback_query_safe(call.id, "Нет активного сообщения для отправки.")
            return

        if relay_action == "edit":
            send_message_safe(chat_id, "Введите новый текст сообщения.")
            answer_callback_query_safe(call.id)
            return

        if relay_action == "cancel":
            clear_state(user_id)
            send_message_safe(chat_id, "Отправка отменена.")
            send_peer_lookup_prompt(chat_id)
            answer_callback_query_safe(call.id)
            return

        if relay_action == "send":
            target_user_id = payload.get("target_user_id")
            draft_message = payload.get("draft_message")
            if not target_user_id or not draft_message:
                clear_state(user_id)
                answer_callback_query_safe(call.id, "Черновик не найден.")
                return

            sender_user = repo.get_user(user_id) or {}
            if not repo.is_registered_user(sender_user):
                clear_state(user_id)
                answer_callback_query_safe(call.id, "Сначала завершите регистрацию.")
                return

            target_user = repo.get_user(int(target_user_id))
            if not repo.is_registered_user(target_user):
                clear_state(user_id)
                answer_callback_query_safe(call.id, "Получатель недоступен.")
                return

            answer_callback_query_safe(call.id, "Отправляю сообщение...")
            relay_text = build_relay_message(sender_user, user_id, draft_message)
            if not send_message_safe(int(target_user_id), relay_text, parse_mode="HTML"):
                logger.warning("Relay send failed from %s to %s", user_id, target_user_id)
                send_message_safe(
                    chat_id,
                    "Не удалось отправить сообщение: пользователь не открыл бота, деактивировал аккаунт или Telegram недоступен.",
                )
                return

            clear_state(user_id)
            send_message_safe(chat_id, "Сообщение отправлено.")
            send_peer_lookup_prompt(chat_id)
            return

        answer_callback_query_safe(call.id, "Неизвестное действие.")
        return

    if not data.startswith("admin:"):
        answer_callback_query_safe(call.id)
        return

    if not is_admin(user_id):
        answer_callback_query_safe(call.id, "Недостаточно прав.")
        return

    action = data.split(":", maxsplit=1)[1]
    if action == "stats":
        send_stats(chat_id)
    elif action == "export":
        send_users_export(chat_id)
    elif action == "recent":
        send_recent_registrations(chat_id)
    elif action == "find":
        set_state(user_id, STATE_ADMIN_FIND)
        send_message_safe(chat_id, "Введите user_id, школьный логин или @telegram username для поиска.")
    elif action == "refresh":
        send_admin_panel(chat_id)
    elif action == "close":
        send_message_safe(chat_id, "Админ-панель закрыта.")
    answer_callback_query_safe(call.id)


@bot.message_handler(content_types=["text"])
def private_text_router(message: types.Message) -> None:
    if message.chat.type != "private":
        return

    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    repo.increment_requests("bot")
    tg_username = (message.from_user.username or "").strip().lower()
    repo.touch_user(message.from_user.id, tg_username)

    user_id = message.from_user.id
    state = user_states.get(user_id)
    if state == STATE_WAITING_LOGIN:
        handle_school_login_input(message, text)
        return
    if state == STATE_WAITING_PIN:
        handle_pin_input(message, text)
        return
    if state == STATE_ADMIN_FIND:
        if not is_admin(user_id):
            clear_state(user_id)
            send_message_safe(message.chat.id, "Недостаточно прав.")
            return
        clear_state(user_id)
        matches = repo.search_users_for_admin(text, limit=10)
        if not matches:
            send_message_safe(message.chat.id, "Пользователи не найдены.")
            return
        lines = ["Результаты поиска:"]
        for user in matches:
            registered = user.get("registered_at")
            reg_str = registered.strftime("%Y-%m-%d %H:%M") if registered else "n/a"
            lines.append(
                f"- ID {user.get('user_id')} | school={user.get('login_school', 'n/a')} | "
                f"tg=@{user.get('login_tg', 'n/a')} | verified={bool(user.get('verified'))} | {reg_str}"
            )
        send_message_safe(message.chat.id, "\n".join(lines))
        return
    if state == STATE_RELAY_MESSAGE:
        payload = user_payloads.get(user_id, {})
        target_user_id = payload.get("target_user_id")
        if not target_user_id:
            clear_state(user_id)
            send_message_safe(message.chat.id, "Сессия отправки устарела. Повтори поиск пользователя.")
            send_peer_lookup_prompt(message.chat.id)
            return

        if len(text) > settings.relay_max_len:
            send_message_safe(
                message.chat.id,
                f"Сообщение слишком длинное. Лимит: {settings.relay_max_len} символов.",
            )
            return

        sender_user = repo.get_user(user_id) or {}
        if not repo.is_registered_user(sender_user):
            clear_state(user_id)
            send_message_safe(message.chat.id, "Сначала завершите регистрацию: /start")
            return

        payload["draft_message"] = text
        set_state(user_id, STATE_RELAY_MESSAGE, payload)

        preview = build_relay_message(sender_user, user_id, text)
        send_relay_preview(message.chat.id, preview)
        return

    user = repo.get_user(user_id)
    if not repo.is_registered_user(user):
        send_message_safe(message.chat.id, "Сначала завершите регистрацию: /start")
        return
    repo.promote_legacy_user(user_id, tg_username)

    login = normalize_login(text)
    found = repo.find_by_login(login)
    if not found:
        send_message_safe(message.chat.id, "Логин не найден.")
        send_peer_lookup_prompt(message.chat.id)
        return

    send_message_safe(message.chat.id, format_lookup_message(found), parse_mode="HTML")
    if settings.enable_relay_test and found.get("user_id") and found.get("user_id") != user_id:
        send_relay_offer(message.chat.id, int(found["user_id"]))
    send_peer_lookup_prompt(message.chat.id)


def polling_with_retries(delay_seconds: int = 5) -> None:
    while True:
        try:
            logger.info("Bot polling started")
            bot.polling(none_stop=True, interval=0, timeout=30)
        except ApiTelegramException as exc:
            if is_polling_conflict(exc):
                logger.error(
                    "Polling conflict: another bot instance is already calling getUpdates. Retrying in %s sec",
                    POLLING_CONFLICT_DELAY_SECONDS,
                )
                time.sleep(POLLING_CONFLICT_DELAY_SECONDS)
                continue
            if "502" in str(exc):
                logger.warning("Telegram 502, retrying in %s sec", delay_seconds)
                time.sleep(delay_seconds)
                continue
            if is_timeout_error(exc):
                logger.warning("Telegram polling timeout, retrying in %s sec: %s", delay_seconds, exc)
            else:
                logger.exception("Telegram API exception")
            time.sleep(delay_seconds)
        except Exception as exc:
            if is_timeout_error(exc):
                logger.warning("Telegram polling timeout, retrying in %s sec: %s", delay_seconds, exc)
            else:
                logger.exception("Unexpected polling failure")
            time.sleep(delay_seconds)


if __name__ == "__main__":
    logger.info("Starting login_collect_bot_2.0")
    set_bot_commands()
    polling_with_retries()
