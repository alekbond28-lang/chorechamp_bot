import os
import asyncio
from datetime import timedelta

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

from aiohttp import web

from db import (
    SessionLocal,
    init_db,
    User,
    TaskTemplate,
    TaskInstance,
    Completion,
    get_today,
)

# Загружаем токен
load_dotenv()
BOT_TOKEN = os.environ.get("BOT_TOKEN")


# ---------- Вспомогательные функции работы с БД ----------


def get_or_create_user(session, tg_user) -> User:
    user = session.query(User).filter_by(telegram_id=tg_user.id).first()
    if not user:
        user = User(
            telegram_id=tg_user.id,
            username=tg_user.username,
            full_name=tg_user.full_name,
        )
        session.add(user)
        session.commit()
    return user


def ensure_default_tasks(session):
    """Создаём несколько дефолтных задач, если их ещё нет."""
    if session.query(TaskTemplate).count() > 0:
        return

    defaults = [
        ("Мыть посуду", "Посуда после еды", "daily", 5),
        ("Пылесосить", "Пропылесосить квартиру", "weekly", 10),
        ("Выносить мусор", "Выбросить мусор", "daily", 3),
    ]
    for title, desc, period, pts in defaults:
        t = TaskTemplate(
            title=title,
            description=desc,
            periodicity=period,
            points=pts,
        )
        session.add(t)
    session.commit()

    today = get_today()
    templates = session.query(TaskTemplate).all()
    for tmpl in templates:
        inst = TaskInstance(
            template_id=tmpl.id,
            date=today,
            status="free",
            priority="normal",
        )
        session.add(inst)
    session.commit()


# ---------- Хендлеры бота ----------


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as session:
        user = get_or_create_user(session, update.effective_user)
        ensure_default_tasks(session)

    await update.message.reply_text(
        "Привет! Это бот для домашних дел.\n\n"
        "Основные команды:\n"
        "/add название | баллы — добавить задачу\n"
        "/today — дела на сегодня\n"
        "/done id — отметить выполненным\n"
        "/score — рейтинг по баллам (упрощённая версия)\n\n"
        "Пока бот работает в одной группе/чате как один 'дом'."
    )


async def add_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /add Название | баллы")
        return

    text = " ".join(context.args)
    if "|" not in text:
        await update.message.reply_text("Нужно: Название | баллы")
        return

    title_part, points_part = [p.strip() for p in text.split("|", 1)]
    if not title_part:
        await update.message.reply_text("Пустое название")
        return

    try:
        points = int(points_part)
    except ValueError:
        await update.message.reply_text("Баллы должны быть числом")
        return

    with SessionLocal() as session:
        tmpl = TaskTemplate(
            title=title_part,
            description=None,
            periodicity="daily",  # пока всегда ежедневно
            points=points,
        )
        session.add(tmpl)
        session.commit()

        inst = TaskInstance(
            template_id=tmpl.id,
            date=get_today(),
            status="free",
            priority="normal",
        )
        session.add(inst)
        session.commit()
        tid = inst.id

    await update.message.reply_text(
        f"Добавлена задача #{tid}: {title_part} ({points} баллов)"
    )


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today_date = get_today()
    with SessionLocal() as session:
        instances = (
            session.query(TaskInstance)
            .join(TaskTemplate)
            .filter(TaskInstance.date == today_date)
            .all()
        )

        if not instances:
            await update.message.reply_text("На сегодня дел нет! 🎉")
            return

        lines = []
        for inst in instances:
            tmpl = inst.template
            prefix = "[HIGH] " if inst.priority == "high" else ""
            status_text = {
                "free": "свободна",
                "in_progress": "в работе",
                "done": "выполнена",
            }.get(inst.status, inst.status)

            performer = ""
            if inst.status in ("in_progress", "done") and inst.assigned_user:
                performer = f" у {inst.assigned_user.full_name or inst.assigned_user.username}"

            line = f"{inst.id}. {prefix}{tmpl.title} — {tmpl.points} баллов — {status_text}{performer}"
            lines.append(line)

    await update.message.reply_text("Дела на сегодня:\n" + "\n".join(lines))


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /done id_задачи")
        return

    try:
        instance_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом")
        return

    with SessionLocal() as session:
        inst = session.query(TaskInstance).filter_by(id=instance_id).first()
        if not inst:
            await update.message.reply_text("Такой задачи нет")
            return

        tmpl = inst.template
        user = get_or_create_user(session, update.effective_user)

        inst.status = "done"
        inst.done_by_user_id = user.id
        inst.done_at = get_today()
        if inst.assigned_user_id is None:
            inst.assigned_user_id = user.id

        comp = Completion(
            user_id=user.id,
            task_instance_id=inst.id,
            points=tmpl.points,
        )
        session.add(comp)
        session.commit()

        await update.message.reply_text(
            f"Задача #{inst.id} выполнена! +{tmpl.points} баллов"
        )


async def score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as session:
        rows = (
            session.query(User, Completion)
            .join(Completion, Completion.user_id == User.id)
            .all()
        )
        if not rows:
            await update.message.reply_text("Пока никто не заработал баллы")
            return

        totals = {}
        for user, comp in rows:
            totals[user.id] = totals.get(user.id, 0) + comp.points

        lines = []
        for user_id, pts in sorted(totals.items(), key=lambda x: x[1], reverse=True):
            user = session.query(User).get(user_id)
            name = user.full_name or user.username or str(user.telegram_id)
            lines.append(f"{name}: {pts} баллов")

    await update.message.reply_text("Рейтинг (всё время):\n" + "\n".join(lines))


# -------- Минимальный HTTP-сервер для Render --------


async def health(request):
    return web.Response(text="OK")


async def run_http_server():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


# -------- Запуск бота + HTTP-сервер --------


def main():
    init_db()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_task))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("done", done))
    application.add_handler(CommandHandler("score", score))

    loop = asyncio.get_event_loop()
    loop.create_task(run_http_server())
    application.run_polling()


if __name__ == "__main__":
    main()
