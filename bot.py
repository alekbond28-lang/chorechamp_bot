import os
import asyncio
from datetime import datetime, timedelta

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

from aiohttp import web

# Загружаем токен из окружения (.env локально, Environment на Render)
load_dotenv()
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# Простое хранилище в памяти
tasks = {}         # id -> {title, points, next_due}
completions = {}   # user_id -> total_points
task_counter = 1


def get_today():
    return datetime.now().date()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    completions.setdefault(user.id, 0)
    await update.message.reply_text(
        "Привет! Это бот для домашних дел.\n\n"
        "Команды:\n"
        "/add название | баллы — добавить ежедневную задачу\n"
        "/today — дела на сегодня\n"
        "/done id — отметить выполненным\n"
        "/score — рейтинг по баллам\n\n"
        "Пример:\n"
        "/add Мыть посуду | 5"
    )


async def add_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global task_counter
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

    tasks[task_counter] = {
        "title": title_part,
        "points": points,
        "next_due": get_today(),
    }

    await update.message.reply_text(
        f"Добавлена задача #{task_counter}: {title_part} ({points} баллов)"
    )
    task_counter += 1


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today_date = get_today()
    lines = []
    for tid, t in tasks.items():
        if t["next_due"] <= today_date:
            lines.append(f"{tid}. {t['title']} ({t['points']} баллов)")
    if not lines:
        await update.message.reply_text("На сегодня дел нет! 🎉")
    else:
        await update.message.reply_text("Дела на сегодня:\n" + "\n".join(lines))


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.args:
        await update.message.reply_text("Формат: /done id_задачи")
        return

    try:
        tid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом")
        return

    if tid not in tasks:
        await update.message.reply_text("Такой задачи нет")
        return

    task = tasks[tid]
    completions[user.id] = completions.get(user.id, 0) + task["points"]
    task["next_due"] = get_today() + timedelta(days=1)

    await update.message.reply_text(
        f"Задача #{tid} выполнена! +{task['points']} баллов\n"
        f"Твой счёт: {completions[user.id]}"
    )


async def score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not completions:
        await update.message.reply_text("Пока никто не заработал баллы")
        return

    items = sorted(completions.items(), key=lambda x: x[1], reverse=True)
    lines = []
    for user_id, pts in items:
        try:
            member = await update.effective_chat.get_member(user_id)
            name = member.user.full_name
        except Exception:
            name = str(user_id)
        lines.append(f"{name}: {pts} баллов")

    await update.message.reply_text("Рейтинг:\n" + "\n".join(lines))


# -------- Минимальный HTTP-сервер для Render --------

async def health(request):
    return web.Response(text="OK")


async def run_http_server():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 10000))  # Render может сам задавать PORT
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


# -------- Запуск бота + HTTP-сервер --------

def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_task))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("done", done))
    application.add_handler(CommandHandler("score", score))

    loop = asyncio.get_event_loop()
    # поднимаем HTTP-сервер, чтобы Render видел открытый порт
    loop.create_task(run_http_server())
    # запускаем бота (polling)
    application.run_polling()


if __name__ == "__main__":
    main()
