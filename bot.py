import os
import asyncio
from datetime import timedelta, time, date
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from aiohttp import web
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from db import (
    SessionLocal,
    init_db,
    User,
    TaskTemplate,
    TaskInstance,
    Completion,
    get_today,
)

load_dotenv()
BOT_TOKEN = os.environ.get("BOT_TOKEN")
LOCAL_TZ = ZoneInfo("Europe/Moscow")
MAIN_CHAT_ID = None

OWNER_ID = 680630275
ALLOWED_USER_IDS = {OWNER_ID}

ACCESS_TEXT = (
    "Этот бот доступен только по приглашению.\n"
    "Твой id: {uid}\n"
    "Передай его владельцу, чтобы он добавил тебя."
)

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("/today"), KeyboardButton("/mytasks")],
        [KeyboardButton("/add"), KeyboardButton("/again")],
    ],
    resize_keyboard=True,
)

# ---------- Вспомогательные функции ----------

def is_owner(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id == OWNER_ID)

def is_allowed_user_id(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return user_id == OWNER_ID or user_id in ALLOWED_USER_IDS

def ensure_access(update: Update):
    user = update.effective_user
    uid = user.id if user else "unknown"
    if not is_allowed_user_id(user.id if user else None):
        return ACCESS_TEXT.format(uid=uid)
    return None

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
    if session.query(TaskTemplate).count() > 0:
        return

    defaults = [
        ("Протереть столы", "Протереть обеденный стол, раковину, плиту", "daily", 1),
        ("Вынести мусор", "Вынести бытовой мусор из квартиры", "daily", 1),
        ("Посудомойка", "Загрузить и выгрузить", "daily", 2),
        ("Включить робот пылесос", "Почистить бак до и после, помыть тряпку", "daily", 2),
        ("Загрузить/разложить стирку", "Поставить стирку и разложить белье", "daily", 2),
        ("Покормить кошку и поменять воду", None, "daily", 1),

        ("Разобрать вещи в комнате", "Разложить одежду и вещи по местам", "weekly", 3),
        ("Уборка туалета", "Почистить унитаз", "weekly", 4),
        ("Уборка ванной", "Вымыть ванну/душ, раковину", "weekly", 4),
        ("Смена постельного белья", "Поменять бельё на кроватях", "weekly", 5),
        ("Протереть пыль", "Протереть пыль на основных поверхностях", "weekly", 5),
        ("Влажная уборка полов", "Вымыть полы во всех комнатах", "weekly", 6),
        ("Вычесать кошку", None, "weekly", 2),
        ("Поменять лоток", None, "weekly", 2),
        ("Покормить черепаху", None, "twice_weekly", 1),

        ("Мытьё окон и зеркал", "Вымыть стёкла и подоконники", "monthly", 5),
        ("Разобрать холодильник", "Выкинуть просроченное, протереть полки", "monthly", 6),
        ("Мытьё дверей и ручек", "Протереть двери, ручки, выключатели", "monthly", 7),
        ("Мытьё духовки/микроволновки", "Отмыть внутри и снаружи", "monthly", 8),
        ("Уборка в ящиках кухни", "Разбор и протирка ящиков/органайзеров", "monthly", 10),
        ("Помыть поилку, насыпать корм", None, "twice_monthly", 2),
        ("Заменить воду в аквариуме", None, "twice_monthly", 4),

        ("Чистка вытяжки и фильтров", "Помыть фильтр и корпус вытяжки", "quarterly", 10),
        ("Мытьё холодильника полностью", "Разморозка (если нужно), мойка", "quarterly", 10),
        ("Протереть батареи", "Протереть батареи", "quarterly", 5),
    ]

    for title, desc, period, pts in defaults:
        session.add(TaskTemplate(
            title=title,
            description=desc,
            periodicity=period,
            points=pts,
        ))
    session.commit()

    today = get_today()
    for tmpl in session.query(TaskTemplate).all():
        session.add(TaskInstance(
            template_id=tmpl.id,
            date=today,
            status="free",
            priority="normal",
        ))
    session.commit()

async def carry_over_tasks(context: ContextTypes.DEFAULT_TYPE):
    today_date = get_today()
    tomorrow = today_date + timedelta(days=1)

    with SessionLocal() as session:
        instances = (
            session.query(TaskInstance)
            .filter(TaskInstance.date == today_date, TaskInstance.status != "done")
            .all()
        )
        for inst in instances:
            session.add(TaskInstance(
                template_id=inst.template_id,
                date=tomorrow,
                status="free",
                priority="high",
            ))
        session.commit()

async def generate_recurring_tasks(context: ContextTypes.DEFAULT_TYPE):
    today = get_today()
    weekday = today.weekday()
    day = today.day

    with SessionLocal() as session:
        templates = session.query(TaskTemplate).filter_by(active=True).all()

        for tmpl in templates:
            p = tmpl.periodicity
            if p == "daily":
                should_create = True
            elif p == "weekly":
                should_create = (weekday == 0)
            elif p == "monthly":
                should_create = (day == 1)
            elif p == "quarterly":
                should_create = (day == 1 and today.month in (1, 4, 7, 10))
            elif p == "twice_weekly":
                should_create = (weekday in (0, 3))
            elif p == "twice_monthly":
                should_create = (day in (1, 15))
            else:
                should_create = False

            if should_create:
                session.add(TaskInstance(
                    template_id=tmpl.id,
                    date=today,
                    status="free",
                    priority="normal",
                ))

        session.commit()

def get_period_bounds_for_today():
    today = date.today()

    weekday = today.weekday()
    week_start = today - timedelta(days=weekday)
    week_end = week_start + timedelta(days=6)

    month_start = today.replace(day=1)
    if today.month == 12:
        next_month_start = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month_start = today.replace(month=today.month + 1, day=1)
    month_end = next_month_start - timedelta(days=1)

    year_start = today.replace(month=1, day=1)
    year_end = today.replace(month=12, day=31)

    return (week_start, week_end), (month_start, month_end), (year_start, year_end)

# ---------- Формирование текста и клавиатур ----------

def format_task_button_text(inst: TaskInstance) -> str:
    tmpl = inst.template
    prefix = "[HIGH] " if inst.priority == "high" else ""
    status_text = {
        "free": "⚪ свободна",
        "in_progress": "🕒 в работе",
        "done": "✅ выполнена",
    }.get(inst.status, inst.status)
    performer = ""
    if inst.status in ("in_progress", "done") and inst.assigned_user:
        performer_name = inst.assigned_user.full_name or inst.assigned_user.username
        performer = f" у {performer_name}"
    return f"{inst.id}. {prefix}{tmpl.title} — {tmpl.points} баллов — {status_text}{performer}"

def build_today_keyboard(instances, current_tg_id: int):
    keyboard_rows = []
    for inst in instances:
        info_text = format_task_button_text(inst)
        info_btn = InlineKeyboardButton(info_text, callback_data="noop")

        if inst.status == "free":
            action_btn = InlineKeyboardButton("❓ Взять", callback_data=f"take:{inst.id}")
        elif inst.status == "in_progress":
            if inst.assigned_user and inst.assigned_user.telegram_id == current_tg_id:
                action_btn = InlineKeyboardButton("🕒 Выполнить", callback_data=f"done:{inst.id}")
            else:
                action_btn = InlineKeyboardButton("🚫 Занято", callback_data="noop")
        else:
            action_btn = InlineKeyboardButton("✅ Выполнено", callback_data="noop")

        keyboard_rows.append([info_btn, action_btn])

    return InlineKeyboardMarkup(keyboard_rows)

# ---------- Хендлеры бота ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    global MAIN_CHAT_ID
    MAIN_CHAT_ID = update.effective_chat.id

    with SessionLocal() as session:
        get_or_create_user(session, update.effective_user)
        ensure_default_tasks(session)

    await update.message.reply_text(
        "Привет! Это бот для домашних дел.\n\n"
        "Основные команды:\n"
        "• /today — дела на сегодня\n"
        "• /mytasks — мои задачи на сегодня\n"
        "• /add — добавить новую задачу\n"
        "• /again — отметить, что задача сделана ещё раз\n"
        "• /my_stats — моя статистика\n"
        "• /leaderboard — лидеры по баллам\n"
        "• /list_templates — управление шаблонами",
        reply_markup=MAIN_KEYBOARD,
    )

async def add_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    context.user_data["add_state"] = "waiting_title"
    context.user_data.pop("add_title", None)
    context.user_data.pop("add_points", None)

    await update.message.reply_text("Пришли название задачи.")

async def add_task_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    state = context.user_data.get("add_state")
    if not state:
        return

    text = (update.message.text or "").strip()

    if state == "waiting_title":
        if not text:
            await update.message.reply_text("Название не может быть пустым. Пришли название задачи.")
            return

        context.user_data["add_title"] = text
        context.user_data["add_state"] = "waiting_points"
        await update.message.reply_text("Сколько баллов за эту задачу? Пришли число.")
        return

    if state == "waiting_points":
        try:
            points = int(text)
        except ValueError:
            await update.message.reply_text("Баллы должны быть числом. Пришли число, например: 5")
            return

        context.user_data["add_points"] = points
        context.user_data["add_state"] = "waiting_period"

        keyboard = [
            [
                InlineKeyboardButton("Единоразово", callback_data="period:once"),
                InlineKeyboardButton("Ежедневно", callback_data="period:daily"),
            ],
            [
                InlineKeyboardButton("Еженедельно", callback_data="period:weekly"),
                InlineKeyboardButton("2 раза в неделю", callback_data="period:twice_weekly"),
            ],
            [
                InlineKeyboardButton("Ежемесячно", callback_data="period:monthly"),
                InlineKeyboardButton("2 раза в месяц", callback_data="period:twice_monthly"),
            ],
            [
                InlineKeyboardButton("Ежеквартально", callback_data="period:quarterly"),
            ],
        ]
        await update.message.reply_text(
            "Выбери периодичность задачи:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    chat_id = update.effective_chat.id
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

        markup = build_today_keyboard(instances, update.effective_user.id)

    await context.bot.send_message(
        chat_id=chat_id,
        text="Задачи на сегодня:",
        reply_markup=markup,
    )

async def mytasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    chat_id = update.effective_chat.id
    today_date = get_today()
    tg_user = update.effective_user

    with SessionLocal() as session:
        user = get_or_create_user(session, tg_user)

        instances = (
            session.query(TaskInstance)
            .join(TaskTemplate)
            .filter(TaskInstance.date == today_date)
            .filter(
                (TaskInstance.assigned_user_id == user.id)
                | (TaskInstance.done_by_user_id == user.id)
            )
            .all()
        )

        if not instances:
            await update.message.reply_text("На сегодня у тебя нет задач 🙂")
            return

        keyboard_rows = []
        for inst in instances:
            info_text = format_task_button_text(inst)
            info_btn = InlineKeyboardButton(info_text, callback_data="noop")

            if inst.status == "free":
                action_btns = [InlineKeyboardButton("❓ Взять", callback_data=f"take:{inst.id}")]
            elif inst.status == "in_progress" and inst.assigned_user_id == user.id:
                action_btns = [
                    InlineKeyboardButton("🕒 Выполнить", callback_data=f"done:{inst.id}"),
                    InlineKeyboardButton("↩️ Вернуть", callback_data=f"return:{inst.id}"),
                ]
            elif inst.status == "done":
                action_btns = [InlineKeyboardButton("✅ Выполнено", callback_data="noop")]
            else:
                action_btns = [InlineKeyboardButton("🚫 Занято", callback_data="noop")]

            keyboard_rows.append([info_btn, *action_btns])

        markup = InlineKeyboardMarkup(keyboard_rows)

    await context.bot.send_message(
        chat_id=chat_id,
        text="Твои задачи на сегодня:",
        reply_markup=markup,
    )

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

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

        session.add(Completion(
            user_id=user.id,
            task_instance_id=inst.id,
            points=tmpl.points,
        ))
        session.commit()

    await update.message.reply_text(
        f"Задача #{inst.id} выполнена! +{tmpl.points} баллов"
    )

async def again(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать задачи, которые сегодня в работе или уже выполнены (любым пользователем),
    и дать возможность добавить такую задачу ещё раз как новую свободную."""
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    chat_id = update.effective_chat.id
    today_date = get_today()

    with SessionLocal() as session:
        instances = (
            session.query(TaskInstance)
            .join(TaskTemplate)
            .filter(TaskInstance.date == today_date)
            .filter(TaskInstance.status.in_(("in_progress", "done")))
            .all()
        )

        if not instances:
            await update.message.reply_text("Сегодня нет задач в работе или выполненных 🙂")
            return

        keyboard_rows = []
        for inst in instances:
            info_text = format_task_button_text(inst)
            info_btn = InlineKeyboardButton(info_text, callback_data="noop")
            # новая логика: создаём новую свободную задачу
            action_btn = InlineKeyboardButton("🔁 Ещё раз", callback_data=f"again:{inst.id}")
            keyboard_rows.append([info_btn, action_btn])

        markup = InlineKeyboardMarkup(keyboard_rows)

    await context.bot.send_message(
        chat_id=chat_id,
        text="Задачи для повторного выполнения:",
        reply_markup=markup,
    )

async def list_templates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    with SessionLocal() as session:
        templates = session.query(TaskTemplate).order_by(TaskTemplate.id).all()

        if not templates:
            await update.message.reply_text("Шаблонов задач пока нет.")
            return

        keyboard_rows = []
        for tmpl in templates:
            status = "активен" if tmpl.active else "деактивирован"
            info_text = f"{tmpl.id}. {tmpl.title} — {tmpl.periodicity}, {tmpl.points} баллов, {status}"
            info_btn = InlineKeyboardButton(info_text, callback_data="noop")

            # одна кнопка, которая будет переключать состояние
            if tmpl.active:
                btn_text = "🚫 Деактивировать"
                cb_data = f"toggle_template:{tmpl.id}"
            else:
                btn_text = "✅ Активировать"
                cb_data = f"toggle_template:{tmpl.id}"

            action_btn = InlineKeyboardButton(btn_text, callback_data=cb_data)
            keyboard_rows.append([info_btn, action_btn])

        markup = InlineKeyboardMarkup(keyboard_rows)

    await update.message.reply_text(
        "Управление шаблонами:",
        reply_markup=markup,
    )

# ---------- CallbackQuery хендлер ----------

async def task_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not is_allowed_user_id(user.id if user else None):
        uid = user.id if user else "unknown"
        await query.answer(
            ACCESS_TEXT.format(uid=uid),
            show_alert=True,
        )
        return

    await query.answer()
    data = query.data or ""
    user_tg = query.from_user

    # выбор периодичности при добавлении задачи
    if data.startswith("period:"):
        period_code = data.split(":", 1)[1]

        title = context.user_data.get("add_title")
        points = context.user_data.get("add_points")
        state = context.user_data.get("add_state")

        if state != "waiting_period" or not title or points is None:
            await query.edit_message_text("Не удалось сохранить задачу. Попробуй ещё раз через /add.")
            context.user_data.clear()
            return

        with SessionLocal() as session:
            tmpl = TaskTemplate(
                title=title,
                description=None,
                periodicity=period_code,
                points=points,
            )
            session.add(tmpl)
            session.commit()

            session.add(TaskInstance(
                template_id=tmpl.id,
                date=get_today(),
                status="free",
                priority="normal",
            ))
            session.commit()

        context.user_data.clear()

        period_human = {
            "once": "единоразово",
            "daily": "ежедневно",
            "weekly": "еженедельно",
            "twice_weekly": "2 раза в неделю",
            "monthly": "ежемесячно",
            "twice_monthly": "2 раза в месяц",
            "quarterly": "ежеквартально",
        }.get(period_code, period_code)

        await query.edit_message_text(
            f"Задача добавлена:\n"
            f"{title}\n"
            f"Баллы: {points}\n"
            f"Периодичность: {period_human}"
        )
        return

    # управление шаблонами: одна кнопка toggle
    if data.startswith("toggle_template:"):
        if not is_owner(update):
            await query.answer("Только владелец может менять шаблоны.", show_alert=True)
            return

        _, _, raw_id = data.partition(":")
        try:
            tmpl_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id шаблона.")
            return

        with SessionLocal() as session:
            tmpl = session.query(TaskTemplate).filter_by(id=tmpl_id).first()
            if not tmpl:
                await query.edit_message_text("Шаблон не найден.")
                return

            tmpl.active = not tmpl.active
            session.commit()

            status = "активен" if tmpl.active else "деактивирован"
            info_text = f"{tmpl.id}. {tmpl.title} — {tmpl.periodicity}, {tmpl.points} баллов, {status}"
            # меняем текст кнопок в строке
            if tmpl.active:
                btn_text = "🚫 Деактивировать"
            else:
                btn_text = "✅ Активировать"

            # перестраиваем клавиатуру целиком по текущему сообщению
            templates = session.query(TaskTemplate).order_by(TaskTemplate.id).all()
            keyboard_rows = []
            for t in templates:
                t_status = "активен" if t.active else "деактивирован"
                row_info_text = f"{t.id}. {t.title} — {t.periodicity}, {t.points} баллов, {t_status}"
                info_btn = InlineKeyboardButton(row_info_text, callback_data="noop")
                row_btn_text = "🚫 Деактивировать" if t.active else "✅ Активировать"
                action_btn = InlineKeyboardButton(row_btn_text, callback_data=f"toggle_template:{t.id}")
                keyboard_rows.append([info_btn, action_btn])

            markup = InlineKeyboardMarkup(keyboard_rows)

        await query.edit_message_text(
            text="Управление шаблонами:",
            reply_markup=markup,
        )
        return

    if data == "noop":
        return

    # вернуть задачу в очередь
    if data.startswith("return:"):
        _, _, raw_id = data.partition(":")
        try:
            instance_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id задачи.")
            return

        with SessionLocal() as session:
            inst = session.query(TaskInstance).filter_by(id=instance_id).first()
            if not inst:
                await query.edit_message_text("Задача не найдена.")
                return

            user_db = get_or_create_user(session, user_tg)

            if inst.status != "in_progress" or inst.assigned_user_id != user_db.id:
                await query.edit_message_text("Ты не выполняешь эту задачу.")
                return

            inst.status = "free"
            inst.assigned_user_id = None
            session.commit()

            tmpl = inst.template

        await query.edit_message_text(
            f"{tmpl.title}\n"
            f"Баллы: {tmpl.points}\n"
            "Статус: ⚪ свободна (возвращена в очередь)"
        )
        return

    # остальные действия: take/drop/done/again
    action, _, raw_id = data.partition(":")
    try:
        instance_id = int(raw_id)
    except ValueError:
        await query.edit_message_text("Некорректный id задачи.")
        return

    is_today_message = query.message and query.message.text.startswith("Задачи на сегодня")
    is_mytasks_message = query.message and query.message.text.startswith("Твои задачи на сегодня:")

    with SessionLocal() as session:
        inst = (
            session.query(TaskInstance)
            .filter_by(id=instance_id)
            .join(TaskTemplate)
            .first()
        )
        if not inst:
            await query.edit_message_text("Задача не найдена.")
            return

        tmpl = inst.template
        user_db = get_or_create_user(session, user_tg)

        if action == "take":
            if inst.status != "free":
                await query.edit_message_text("Задача уже занята или выполнена.")
                return
            inst.status = "in_progress"
            inst.assigned_user_id = user_db.id
            session.commit()

        elif action == "drop":
            if inst.status != "in_progress" or inst.assigned_user_id != user_db.id:
                await query.edit_message_text("Вы не выполняете эту задачу.")
                return
            inst.status = "free"
            inst.assigned_user_id = None
            session.commit()

        elif action == "done":
            if inst.status == "done":
                await query.edit_message_text("Задача уже выполнена.")
                return
            if inst.status == "in_progress" and inst.assigned_user_id != user_db.id:
                await query.edit_message_text("Вы не выполняете эту задачу.")
                return

            inst.status = "done"
            if inst.assigned_user_id is None:
                inst.assigned_user_id = user_db.id
            inst.done_by_user_id = user_db.id
            inst.done_at = get_today()

            session.add(Completion(
                user_id=user_db.id,
                task_instance_id=inst.id,
                points=tmpl.points,
            ))
            session.commit()

        elif action == "again":
            # НОВАЯ ЛОГИКА: создаём новый инстанс задачи на сегодня, свободный, без исполнителя
            today_date = get_today()
            new_inst = TaskInstance(
                template_id=tmpl.id,
                date=today_date,
                status="free",
                priority="normal",
                assigned_user_id=None,
                done_by_user_id=None,
                done_at=None,
            )
            session.add(new_inst)
            session.commit()

            await query.edit_message_text(
                f"{tmpl.title}\n"
                f"Баллы: {tmpl.points}\n"
                f"Создана новая свободная задача на сегодня."
            )
            return

        else:
            await query.edit_message_text("Неизвестное действие.")
            return

        today_date = get_today()

        if is_today_message:
            instances = (
                session.query(TaskInstance)
                .join(TaskTemplate)
                .filter(TaskInstance.date == today_date)
                .all()
            )
            markup = build_today_keyboard(instances, user_tg.id)
            await query.edit_message_text(
                text="Задачи на сегодня:",
                reply_markup=markup,
            )
        elif is_mytasks_message:
            instances = (
                session.query(TaskInstance)
                .join(TaskTemplate)
                .filter(TaskInstance.date == today_date)
                .filter(
                    (TaskInstance.assigned_user_id == user_db.id)
                    | (TaskInstance.done_by_user_id == user_db.id)
                )
                .all()
            )

            keyboard_rows = []
            for inst2 in instances:
                info_text = format_task_button_text(inst2)
                info_btn = InlineKeyboardButton(info_text, callback_data="noop")

                if inst2.status == "free":
                    action_btns = [InlineKeyboardButton("❓ Взять", callback_data=f"take:{inst2.id}")]
                elif inst2.status == "in_progress" and inst2.assigned_user_id == user_db.id:
                    action_btns = [
                        InlineKeyboardButton("🕒 Выполнить", callback_data=f"done:{inst2.id}"),
                        InlineKeyboardButton("↩️ Вернуть", callback_data=f"return:{inst2.id}"),
                    ]
                elif inst2.status == "done":
                    action_btns = [InlineKeyboardButton("✅ Выполнено", callback_data="noop")]
                else:
                    action_btns = [InlineKeyboardButton("🚫 Занято", callback_data="noop")]

                keyboard_rows.append([info_btn, *action_btns])

            markup = InlineKeyboardMarkup(keyboard_rows)

            await query.edit_message_text(
                text="Твои задачи на сегодня:",
                reply_markup=markup,
            )
        else:
            if inst.status == "free":
                status_line = "⚪ свободна"
            elif inst.status == "in_progress":
                status_line = f"🕒 в работе у {user_db.full_name or user_db.username}"
            else:
                status_line = f"✅ выполнена {user_db.full_name or user_db.username}"

            await query.edit_message_text(
                f"{tmpl.title}\n"
                f"Баллы: {tmpl.points}\n"
                f"Статус: {status_line}"
            )

# ---------- Статистика и сервисные вещи ----------

async def allow_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("Эта команда доступна только владельцу.")
        return

    if not context.args:
        await update.message.reply_text("Формат: /allow <telegram_id>")
        return

    try:
        new_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом")
        return

    if new_id in ALLOWED_USER_IDS:
        await update.message.reply_text(f"Пользователь {new_id} уже в доме.")
        return

    ALLOWED_USER_IDS.add(new_id)
    await update.message.reply_text(f"Пользователь {new_id} добавлен в дом ✅")

async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    (week_start, week_end), (month_start, month_end), (year_start, year_end) = get_period_bounds_for_today()
    tg_user = update.effective_user

    with SessionLocal() as session:
        user = get_or_create_user(session, tg_user)

        comps = session.query(Completion).filter(Completion.user_id == user.id).all()

        if not comps:
            await update.message.reply_text("У тебя пока нет баллов 🙂")
            return

        total_all = total_week = total_month = total_year = 0

        for comp in comps:
            day = comp.created_at.date()
            pts = comp.points

            total_all += pts

            if week_start <= day <= week_end:
                total_week += pts
            if month_start <= day <= month_end:
                total_month += pts
            if year_start <= day <= year_end:
                total_year += pts

    await update.message.reply_text(
        "Твоя статистика:\n"
        f"Неделя: {total_week}\n"
        f"Месяц: {total_month}\n"
        f"Год: {total_year}\n"
        f"Всё время: {total_all}"
    )

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error)
        return

    (week_start, week_end), (month_start, month_end), (year_start, year_end) = get_period_bounds_for_today()

    with SessionLocal() as session:
        rows = (
            session.query(User, Completion)
            .join(Completion, Completion.user_id == User.id)
            .all()
        )

        if not rows:
            await update.message.reply_text("Лидеров пока нет — никто не заработал баллы")
            return

        totals_week = {}
        totals_month = {}
        totals_year = {}
        totals_all = {}

        for user, comp in rows:
            day = comp.created_at.date()
            uid = user.id
            pts = comp.points

            totals_all[uid] = totals_all.get(uid, 0) + pts

            if week_start <= day <= week_end:
                totals_week[uid] = totals_week.get(uid, 0) + pts
            if month_start <= day <= month_end:
                totals_month[uid] = totals_month.get(uid, 0) + pts
            if year_start <= day <= year_end:
                totals_year[uid] = totals_year.get(uid, 0) + pts

        def top_3_block(title, data_dict):
            if not data_dict:
                return f"{title}: пока никто не в лидерах"
            lines = []
            for uid, pts in sorted(data_dict.items(), key=lambda x: x[1], reverse=True)[:3]:
                u = session.get(User, uid)
                name = u.full_name or u.username or str(u.telegram_id)
                lines.append(f"{name}: {pts}")
            return f"{title}:\n" + "\n".join(lines)

        text = "\n\n".join(
            [
                top_3_block("Лидеры недели", totals_week),
                top_3_block("Лидеры месяца", totals_month),
                top_3_block("Лидеры года", totals_year),
                top_3_block("Лидеры (всё время)", totals_all),
            ]
        )

    await update.message.reply_text(text)

async def send_daily_digest(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data.get("chat_id") if context.job and context.job.data else None
    if chat_id is None:
        return

    today_date = get_today()

    with SessionLocal() as session:
        instances = (
            session.query(TaskInstance)
            .join(TaskTemplate)
            .filter(TaskInstance.date == today_date)
            .all()
        )

        if not instances:
            await context.bot.send_message(chat_id=chat_id, text="На сегодня дел нет! 🎉")
            return

        lines = [format_task_button_text(inst) for inst in instances]

    await context.bot.send_message(
        chat_id=chat_id,
        text="Ежедневный дайджест задач:\n" + "\n".join(lines),
    )

async def send_daily_summary(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data.get("chat_id") if context.job and context.job.data else None
    if chat_id is None:
        return

    today = date.today()
    (week_start, week_end), (month_start, month_end), (year_start, year_end) = get_period_bounds_for_today()

    with SessionLocal() as session:
        rows = (
            session.query(User, Completion)
            .join(Completion, Completion.user_id == User.id)
            .all()
        )

    if not rows:
        await context.bot.send_message(chat_id=chat_id, text="Сегодня никто не заработал баллы.")
        return

    totals_today = {}
    for user, comp in rows:
        if comp.created_at.date() == today:
            totals_today[user.id] = totals_today.get(user.id, 0) + comp.points

    def block_for_period(title, start, end):
        totals = {}
        for user, comp in rows:
            d = comp.created_at.date()
            if start <= d <= end:
                totals[user.id] = totals.get(user.id, 0) + comp.points
        if not totals:
            return f"{title}: пока нет баллов"
        lines = []
        for uid, pts in sorted(totals.items(), key=lambda x: x[1], reverse=True)[:3]:
            u = next(u for u, c in rows if u.id == uid)
            name = u.full_name or u.username or str(u.telegram_id)
            lines.append(f"{name}: {pts}")
        return f"{title}:\n" + "\n".join(lines)

    parts = []

    if totals_today:
        day_lines = []
        for uid, pts in sorted(totals_today.items(), key=lambda x: x[1], reverse=True)[:3]:
            u = next(u for u, c in rows if u.id == uid)
            name = u.full_name or u.username or str(u.telegram_id)
            day_lines.append(f"{name}: {pts}")
        parts.append("Итоги дня:\n" + "\n".join(day_lines))
    else:
        parts.append("Итоги дня: никто не заработал баллы.")

    if today == week_end:
        parts.append(block_for_period("Итоги недели", week_start, week_end))

    if today == month_end:
        parts.append(block_for_period("Итоги месяца", month_start, month_end))

    if today == year_end:
        parts.append(block_for_period("Итоги года", year_start, year_end))

    await context.bot.send_message(chat_id=chat_id, text="\n\n".join(parts))

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

# -------- setup команд --------

async def setup_commands(application):
    commands = [
        BotCommand("start", "Описание бота и главное меню"),
        BotCommand("today", "Показать задачи на сегодня"),
        BotCommand("mytasks", "Мои задачи на сегодня"),
        BotCommand("add", "Добавить новую задачу"),
        BotCommand("again", "Отметить, что задача сделана ещё раз"),
        BotCommand("my_stats", "Моя статистика"),
        BotCommand("leaderboard", "Лидеры по баллам"),
        BotCommand("list_templates", "Показать и управлять шаблонами задач"),
        BotCommand("allow", "Добавить участника (только владелец)"),
    ]
    await application.bot.set_my_commands(commands)

# -------- Запуск бота + HTTP-сервер --------

def main():
    init_db()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_task))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("again", again))
    application.add_handler(CommandHandler("done", done))
    application.add_handler(CommandHandler("my_stats", my_stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("allow", allow_user))
    application.add_handler(CommandHandler("mytasks", mytasks))
    application.add_handler(CommandHandler("list_templates", list_templates))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, add_task_flow))
    application.add_handler(CallbackQueryHandler(task_button_handler))

    job_queue = application.job_queue

    job_queue.run_daily(
        carry_over_tasks,
        time=time(hour=23, minute=50, tzinfo=LOCAL_TZ),
        name="carry_over_tasks",
    )

    job_queue.run_daily(
        send_daily_digest,
        time=time(hour=9, minute=0, tzinfo=LOCAL_TZ),
        data={"chat_id": MAIN_CHAT_ID},
        name="daily_digest",
    )

    job_queue.run_daily(
        send_daily_summary,
        time=time(hour=23, minute=59, tzinfo=LOCAL_TZ),
        data={"chat_id": MAIN_CHAT_ID},
        name="daily_summary",
    )

    job_queue.run_daily(
        generate_recurring_tasks,
        time=time(hour=6, minute=0, tzinfo=LOCAL_TZ),
        name="generate_recurring_tasks",
    )

    loop = asyncio.get_event_loop()
    loop.create_task(run_http_server())
    loop.run_until_complete(setup_commands(application))

    application.run_polling()

if __name__ == "__main__":
    main()
