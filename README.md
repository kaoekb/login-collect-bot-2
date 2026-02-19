# login_collect_bot_2.0

Улучшенная версия Telegram-бота для поиска пиров по школьному и Telegram логину.

## Что улучшено

- Разделение на модули: `bot.py`, `config.py`, `db.py`, `mailer.py`.
- Улучшенная регистрация:
  - ввод школьного логина;
  - отправка PIN на `@student.21-school.ru`;
  - подтверждение PIN с TTL (по умолчанию 5 минут);
  - повторная отправка PIN через кнопку.
- Админка в Telegram через `/admin`:
  - статистика;
  - экспорт пользователей в Excel;
  - последние регистрации;
  - поиск пользователя по `user_id`, school login, `@telegram`.
- Сохранение состояния активности бота по группам (`/start` и `/stop` в группе).

## Команды

- `/start` - старт и регистрация.
- `/register` - перезапуск регистрации.
- `/help` - справка.
- `/delete` - удаление своей записи.
- `/bot <логин>` - поиск пользователя в группе.
- `/stop` - отключение бота в группе.

Админ-команды:

- `/admin` - открыть админ-панель.
- `/stat` - статистика текстом.
- `/user` - экспорт пользователей.
- `/log` - отправка лог-файла.

## Быстрый запуск

1. Скопируйте окружение:

   ```bash
   cp .env.example .env
   ```

2. Заполните обязательные переменные:
   - `BOT_TOKEN`
   - `MONGO_URI`
   - `ADMIN_IDS`
   - `sender` и `mail_password` (для отправки PIN)
   - при необходимости SMTP:
     `SMTP_HOST`, `SMTP_PORT`, `SMTP_USE_TLS`
   - тестовая пересылка сообщений через бота: `ENABLE_RELAY_TEST=true`
   - лимит длины relay-сообщения: `RELAY_MAX_LEN=700`

3. Локальный запуск:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python bot.py
   ```

4. Docker:

   ```bash
   docker build -t login-collect-bot-2 .
   docker run --env-file .env login-collect-bot-2
   ```

5. Docker Compose:

   ```bash
   docker compose up -d --build
   docker compose logs -f bot
   docker compose down
   ```

## Следующий шаг

Подготовлена база для добавления проверки уже существующих пользователей при регистрации (дополнительные бизнес-правила и уведомления).
