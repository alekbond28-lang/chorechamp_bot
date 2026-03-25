import os
import asyncio
from datetime import timedelta, time, date
from zoneinfo import ZoneInfo
from sqlalchemy import case, func
from telegram.ext import MessageHandler, filters
from dotenv import load_dotenv
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

load_dotenv()
BOT_TOKEN = os.environ.get("BOT_TOKEN")
LOCAL_TZ = ZoneInfo("Europe/Moscow")
MAIN_CHAT_ID = None
OWNER_ID = 680630275
ALLOWED_USER_IDS = {OWNER_ID}

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("today"), KeyboardButton("mytasks")],
        [KeyboardButton("add")],
    ],
    resize_keyboard=True,
)


def is_owner(update: Update) -> bool:
    user = update.effective_user
    return user and user.id == OWNER_ID


def is_allowed(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    if user.id == OWNER_ID:
        return True
    return user.id in ALLOWED_USER_IDS


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
        # title, description, periodicity, points
        ("Уборка кухни", "", "daily", 1),
        ("Уборка гостиной", "", "daily", 1),
        ("Уборка санузла", "", "daily", 2),
        ("Мытьё полов", "", "daily", 2),
        ("Протереть пыль", "", "daily", 2),
        ("Вынести мусор", "", "daily", 1),
        ("Покупка продуктов", "", "weekly", 3),
        ("Стирка", "", "weekly", 4),
        ("Глажка", "", "weekly", 4),
        ("Полив цветов", "", "weekly", 5),
        ("Разбор почты", "", "weekly", 5),
        ("Разбор документов", "", "weekly", 6),
        ("Проверка аптечки", "", "monthly", 5),
        ("Общий разбор вещей", "", "monthly", 6),
        ("Генеральная уборка", "", "monthly", 7),
        ("Мытьё окон", "", "monthly", 8),
        ("Разбор кладовки", "", "monthly", 10),
        ("Проверка финансов", "", "twicemonthly", 2),
        ("Планирование дел", "", "twicemonthly", 4),
        ("Сезонная уборка", "", "quarterly", 10),
        ("Проверка техники", "", "quarterly", 10),
        ("Проверка гардероба", "", "quarterly", 5),
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


async def carry_over_tasks(context: ContextTypes.DEFAULT_TYPE):
    today_date = get_today()
    tomorrow = today_date + timedelta(days=1)
    with SessionLocal() as session:
        instances = (
            session.query(TaskInstance)
            .filter(TaskInstance.date == today_date)
            .filter(TaskInstance.status != "done")
            .all()
        )
        for inst in instances:
            new_inst = TaskInstance(
                template_id=inst.template_id,
                date=tomorrow,
                status="free",
                priority="high",
            )
            session.add(new_inst)
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
                should_create = weekday == 0
            elif p == "monthly":
                should_create = day == 1
            elif p == "quarterly":
                should_create = day == 1 and today.month in (1, 4, 7, 10)
            elif p == "twiceweekly":
                should_create = weekday in (0, 3)
            elif p == "twicemonthly":
                should_create = day in (1, 15)
            else:
                should_create = False

            if not should_create:
                continue

            inst = TaskInstance(
                template_id=tmpl.id,
                date=today,
                status="free",
                priority="normal",
            )
            session.add(inst)
        session.commit()


def get_period_bounds_for_today(today: date):
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

    return week_start, week_end, month_start, month_end, year_start, year_end


def format_task_button_text(inst: TaskInstance, tmpl: TaskTemplate) -> str:
    prefix = "HIGH" if inst.priority == "high" else ""
    status_text = {"free": "🟢", "inprogress": "🟡", "done": "🔵"}.get(
        inst.status, inst.status
    )
    performer = ""
    if inst.status in ("inprogress", "done") and inst.assigned_user:
        performer_name = inst.assigned_user.full_name or inst.assigned_user.username
        performer = f" ({performer_name})"
    return f"{inst.id}. {prefix} {tmpl.title} [{tmpl.points}] {status_text}{performer}"


def build_today_keyboard(instances, current_tg_id: int) -> InlineKeyboardMarkup:
    keyboard_rows = []
    for inst in instances:
        tmpl = inst.template
        info_text = format_task_button_text(inst, tmpl)
        info_btn = InlineKeyboardButton(info_text, callback_data="noop")

        if inst.status == "free":
            action_btn = InlineKeyboardButton("Взять", callback_data=f"take:{inst.id}")
        elif inst.status == "inprogress":
            if inst.assigned_user and inst.assigned_user.telegram_id == current_tg_id:
                action_btn = InlineKeyboardButton("Готово", callback_data=f"done:{inst.id}")
            else:
                action_btn = InlineKeyboardButton("Занято", callback_data="noop")
        else:  # done
            action_btn = InlineKeyboardButton("✔", callback_data="noop")

        keyboard_rows.append([info_btn, action_btn])

    return InlineKeyboardMarkup(keyboard_rows)


def get_today_instances_filtered(session, today_date, filter_type: str, user: User | None):
    q = (
        session.query(TaskInstance)
        .join(TaskTemplate)
        .filter(TaskInstance.date == today_date)
    )

    if filter_type == "my" and user is not None:
        q = q.filter(
            (TaskInstance.assigned_user_id == user.id)
            & (TaskInstance.done_by_user_id == None)
        )
    elif filter_type == "done" and user is not None:
        q = q.filter(TaskInstance.done_by_user_id == user.id)

    q = q.order_by(
        case(
            (
                (TaskInstance.status == "free", 0),
                (TaskInstance.status == "inprogress", 1),
                (TaskInstance.status == "done", 2),
            ),
            else_=3,
        ),
        TaskInstance.id,
    )

    return q.all()


def build_today_header_keyboard(current_filter: str) -> list[list[InlineKeyboardButton]]:
    def label(code, text):
        return f"[{text}]" if code == current_filter else text

    return [[
        InlineKeyboardButton(label("all", "All"), callback_data="filter:all"),
        InlineKeyboardButton(label("my", "My tasks"), callback_data="filter:my"),
        InlineKeyboardButton(label("done", "Done"), callback_data="filter:done"),
    ]]


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    global MAIN_CHAT_ID
    MAIN_CHAT_ID = update.effective_chat.id

    with SessionLocal() as session:
        get_or_create_user(session, update.effective_user)
        ensure_default_tasks(session)

    await update.message.reply_text(
        "Привет! Команды: today, mytasks, add, again, mystats, leaderboard.",
        reply_markup=MAIN_KEYBOARD,
    )


async def add_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    context.user_data["add_state"] = "waiting_title"
    context.user_data.pop("add_title", None)
    context.user_data.pop("add_points", None)
    await update.message.reply_text("Введите название задачи:")


async def add_task_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    state = context.user_data.get("add_state")
    if not state:
        await update.message.reply_text("Сначала используйте команду /add.")
        return

    text = update.message.text.strip()

    if state == "waiting_title":
        if not text:
            await update.message.reply_text("Название не может быть пустым.")
            return
        context.user_data["add_title"] = text
        context.user_data["add_state"] = "waiting_points"
        await update.message.reply_text("Сколько баллов даёт задача? (число)")
        return

    if state == "waiting_points":
        try:
            points = int(text)
        except ValueError:
            await update.message.reply_text("Нужно ввести целое число, например 5.")
            return
        context.user_data["add_points"] = points
        context.user_data["add_state"] = "waiting_period"

        keyboard = [
            [
                InlineKeyboardButton("Разово", callback_data="period:once"),
                InlineKeyboardButton("Каждый день", callback_data="period:daily"),
            ],
            [
                InlineKeyboardButton("Раз в неделю", callback_data="period:weekly"),
                InlineKeyboardButton("2 раза в неделю", callback_data="period:twiceweekly"),
            ],
            [
                InlineKeyboardButton("Раз в месяц", callback_data="period:monthly"),
                InlineKeyboardButton("2 раза в месяц", callback_data="period:twicemonthly"),
            ],
            [
                InlineKeyboardButton("Раз в квартал", callback_data="period:quarterly"),
            ],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "Как часто повторять задачу?", reply_markup=reply_markup
        )
        return


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    chat_id = update.effective_chat.id
    tg_user = update.effective_user
    today_date = get_today()

    with SessionLocal() as session:
        user = get_or_create_user(session, tg_user)
        instances = get_today_instances_filtered(session, today_date, "all", user)

        if not instances:
            await update.message.reply_text("На сегодня задач нет!")
            return

        header_row = build_today_header_keyboard("all")
        list_markup = build_today_keyboard(instances, tg_user.id)
        keyboard = header_row + list_markup.inline_keyboard

    await context.bot.send_message(
        chat_id=chat_id,
        text="Задачи на сегодня:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def mytasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
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
                TaskInstance.assigned_user_id == user.id,
                TaskInstance.done_by_user_id == None,
            )
            .all()
        )

        if not instances:
            await update.message.reply_text("У вас нет задач в работе.")
            return

        keyboard_rows = []
        for inst in instances:
            tmpl = inst.template
            info_text = format_task_button_text(inst, tmpl)
            info_btn = InlineKeyboardButton(info_text, callback_data="noop")

            if inst.status == "free":
                action_btns = [InlineKeyboardButton("Взять", callback_data=f"take:{inst.id}")]
            elif inst.status == "inprogress" and inst.assigned_user_id == user.id:
                action_btns = [
                    InlineKeyboardButton("Готово", callback_data=f"done:{inst.id}"),
                    InlineKeyboardButton("Вернуть", callback_data=f"return:{inst.id}"),
                ]
            elif inst.status == "done":
                action_btns = [InlineKeyboardButton("✔", callback_data="noop")]
            else:
                action_btns = [InlineKeyboardButton("Занято", callback_data="noop")]

            keyboard_rows.append([info_btn] + action_btns)

        markup = InlineKeyboardMarkup(keyboard_rows)

    await context.bot.send_message(
        chat_id=chat_id,
        text="Ваши задачи:",
        reply_markup=markup,
    )


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    if not context.args:
        await update.message.reply_text("Нужно указать id задачи: /done <id>")
        return

    try:
        instance_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом.")
        return

    with SessionLocal() as session:
        inst = session.query(TaskInstance).filter_by(id=instance_id).first()
        if not inst:
            await update.message.reply_text("Задача не найдена.")
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

    await update.message.reply_text(f"Задача {inst.id} выполнена! +{tmpl.points} баллов.")


async def again(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    chat_id = update.effective_chat.id
    today_date = get_today()

    with SessionLocal() as session:
        instances = (
            session.query(TaskInstance)
            .join(TaskTemplate)
            .filter(TaskInstance.date == today_date)
            .filter(TaskInstance.status == "done")
            .all()
        )

        if not instances:
            await update.message.reply_text("Нет выполненных задач для повтора.")
            return

        keyboard_rows = []
        for inst in instances:
            tmpl = inst.template
            info_text = format_task_button_text(inst, tmpl)
            info_btn = InlineKeyboardButton(info_text, callback_data="noop")
            action_btn = InlineKeyboardButton("Сделать снова", callback_data=f"again:{inst.id}")
            keyboard_rows.append([info_btn, action_btn])

        markup = InlineKeyboardMarkup(keyboard_rows)

    await context.bot.send_message(
        chat_id=chat_id,
        text="Выберите задачу для повторного выполнения:",
        reply_markup=markup,
    )


async def list_templates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    with SessionLocal() as session:
        templates = session.query(TaskTemplate).order_by(TaskTemplate.id).all()

        if not templates:
            await update.message.reply_text("Шаблонов задач пока нет.")
            return

        keyboard_rows = []
        for tmpl in templates:
            status = "✅" if tmpl.active else "❌"
            info_text = f"{tmpl.id}. {tmpl.title} [{tmpl.periodicity}, {tmpl.points}] {status}"
            info_btn = InlineKeyboardButton(info_text, callback_data="noop")

            if tmpl.active:
                action_btn = InlineKeyboardButton("Деактивировать", callback_data=f"deactivate:{tmpl.id}")
            else:
                action_btn = InlineKeyboardButton("Активировать", callback_data=f"activate:{tmpl.id}")

            keyboard_rows.append([info_btn, action_btn])

        markup = InlineKeyboardMarkup(keyboard_rows)

    await update.message.reply_text(
        "Шаблоны задач:",
        reply_markup=markup,
    )


async def deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await list_templates(update, context)


async def task_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        user = update.effective_user
        uid = user.id if user else "unknown"
        await update.callback_query.answer(
            f"У вас нет доступа. Ваш id: {uid}", show_alert=True
        )
        return

    query = update.callback_query
    await query.answer()
    data = query.data or ""
    user_tg = query.from_user

    if data.startswith("period:"):
        period_code = data.split(":", 1)[1]
        title = context.user_data.get("add_title")
        points = context.user_data.get("add_points")
        state = context.user_data.get("add_state")

        if state != "waiting_period" or not title or points is None:
            await query.edit_message_text("Некорректное состояние добавления задачи. Начните с /add.")
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

            inst = TaskInstance(
                template_id=tmpl.id,
                date=get_today(),
                status="free",
                priority="normal",
            )
            session.add(inst)
            session.commit()

        context.user_data.pop("add_state", None)
        context.user_data.pop("add_title", None)
        context.user_data.pop("add_points", None)

        period_human = {
            "once": "разово",
            "daily": "каждый день",
            "weekly": "раз в неделю",
            "twiceweekly": "два раза в неделю",
            "monthly": "раз в месяц",
            "twicemonthly": "два раза в месяц",
            "quarterly": "раз в квартал",
        }.get(period_code, period_code)

        await query.edit_message_text(
            f"Создана задача: {title}. Баллы: {points}. Периодичность: {period_human}."
        )
        return

    if data.startswith(("activate:", "deactivate:")):
        if not is_owner(update):
            await query.answer("Только владелец может управлять шаблонами.", show_alert=True)
            return

        action, _, raw_id = data.partition(":")
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

            if action == "deactivate":
                tmpl.active = False
                status = "деактивирован"
            else:
                tmpl.active = True
                status = "активирован"

            session.commit()

        await query.answer(f"Шаблон {status}.", show_alert=False)
        return

    if data == "noop":
        return

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

            user = get_or_create_user(session, user_tg)
            if inst.status != "inprogress" or inst.assigned_user_id != user.id:
                await query.edit_message_text("Вы не можете вернуть эту задачу.")
                return

            inst.status = "free"
            inst.assigned_user_id = None
            session.commit()

            tmpl = inst.template

        await query.edit_message_text(f"Задача {tmpl.title} возвращена в свободные.")
        return

    action, _, raw_id = data.partition(":")
    try:
        instance_id = int(raw_id)
    except ValueError:
        await query.edit_message_text("Некорректный id задачи.")
        return

    is_today_message = query.message and query.message.text.startswith("Задачи на сегодня")
    is_mytasks_message = query.message and query.message.text.startswith("Ваши задачи")

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
        user = get_or_create_user(session, user_tg)

        if action == "take":
            if inst.status != "free":
                await query.edit_message_text("Эта задача уже занята или выполнена.")
                return
            inst.status = "inprogress"
            inst.assigned_user_id = user.id
            session.commit()
        elif action == "drop":
            if inst.status != "inprogress" or inst.assigned_user_id != user.id:
                await query.edit_message_text("Вы не можете вернуть эту задачу.")
                return
            inst.status = "free"
            inst.assigned_user_id = None
            session.commit()
        elif action == "done":
            if inst.status == "done":
                await query.edit_message_text("Задача уже выполнена.")
                return
            if inst.status == "inprogress" and inst.assigned_user_id != user.id:
                await query.edit_message_text("Эта задача в работе у другого пользователя.")
                return

            inst.status = "done"
            if inst.assigned_user_id is None:
                inst.assigned_user_id = user.id
            inst.done_by_user_id = user.id
            inst.done_at = get_today()

            comp = Completion(
                user_id=user.id,
                task_instance_id=inst.id,
                points=tmpl.points,
            )
            session.add(comp)
            session.commit()
        elif action == "again":
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
        else:
            await query.edit_message_text("Неизвестное действие.")
            return

        if is_today_message:
            today_date = get_today()
            instances = (
                session.query(TaskInstance)
                .join(TaskTemplate)
                .filter(TaskInstance.date == today_date)
                .all()
            )
            markup = build_today_keyboard(instances, user_tg.id)
            await query.edit_message_text(
                "Задачи на сегодня:",
                reply_markup=markup,
            )
        elif is_mytasks_message:
            today_date = get_today()
            user = get_or_create_user(session, user_tg)
            instances = (
                session.query(TaskInstance)
                .join(TaskTemplate)
                .filter(TaskInstance.date == today_date)
                .filter(
                    TaskInstance.assigned_user_id == user.id,
                    TaskInstance.done_by_user_id == None,
                )
                .all()
            )

            keyboard_rows = []
            for inst in instances:
                tmpl = inst.template
                info_text = format_task_button_text(inst, tmpl)
                info_btn = InlineKeyboardButton(info_text, callback_data="noop")

                if inst.status == "free":
                    action_btns = [InlineKeyboardButton("Взять", callback_data=f"take:{inst.id}")]
                elif inst.status == "inprogress" and inst.assigned_user_id == user.id:
                    action_btns = [
                        InlineKeyboardButton("Готово", callback_data=f"done:{inst.id}"),
                        InlineKeyboardButton("Вернуть", callback_data=f"return:{inst.id}"),
                    ]
                elif inst.status == "done":
                    action_btns = [InlineKeyboardButton("✔", callback_data="noop")]
                else:
                    action_btns = [InlineKeyboardButton("Занято", callback_data="noop")]

                keyboard_rows.append([info_btn] + action_btns)

            markup = InlineKeyboardMarkup(keyboard_rows)
            await query.edit_message_text(
                "Ваши задачи:",
                reply_markup=markup,
            )
        else:
            if inst.status == "free":
                status_line = "свободна"
            elif inst.status == "inprogress":
                status_line = f"в работе у {user.full_name or user.username}"
            else:
                status_line = f"выполнена {user.full_name or user.username}"

            await query.edit_message_text(
                f"{tmpl.title} [{tmpl.points}] — {status_line}"
            )


async def score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    with SessionLocal() as session:
        rows = (
            session.query(User, Completion)
            .join(Completion, Completion.user_id == User.id)
            .all()
        )

        if not rows:
            await update.message.reply_text("Пока нет начисленных баллов.")
            return

        totals = {}
        for user, comp in rows:
            totals[user.id] = totals.get(user.id, 0) + comp.points

        lines = []
        for user_id, pts in sorted(totals.items(), key=lambda x: x[1], reverse=True):
            user = session.query(User).get(user_id)
            name = user.full_name or user.username or str(user.telegram_id)
            lines.append(f"{name}: {pts}")

    await update.message.reply_text("\n".join(lines))


async def allow_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("Только владелец может добавлять пользователей.")
        return

    if not context.args:
        await update.message.reply_text("Нужно указать telegram id: /allow <telegram_id>")
        return

    try:
        new_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("id должен быть числом.")
        return

    if new_id in ALLOWED_USER_IDS:
        await update.message.reply_text(f"Пользователь {new_id} уже имеет доступ.")
        return

    ALLOWED_USER_IDS.add(new_id)
    await update.message.reply_text(f"Пользователь {new_id} добавлен в список доступа.")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    week_start, week_end, month_start, month_end, year_start, year_end = get_period_bounds_for_today(
        get_today()
    )

    with SessionLocal() as session:
        rows = (
            session.query(User, Completion)
            .join(Completion, Completion.user_id == User.id)
            .all()
        )

        if not rows:
            await update.message.reply_text("Нет статистики.")
            return

        totals_all = {}
        totals_week = {}
        totals_month = {}
        totals_year = {}

        for user, comp in rows:
            day = comp.created_at.date()
            uid = user.id
            totals_all[uid] = totals_all.get(uid, 0) + comp.points
            if week_start <= day <= week_end:
                totals_week[uid] = totals_week.get(uid, 0) + comp.points
            if month_start <= day <= month_end:
                totals_month[uid] = totals_month.get(uid, 0) + comp.points
            if year_start <= day <= year_end:
                totals_year[uid] = totals_year.get(uid, 0) + comp.points

        def format_block(title, data_dict):
            if not data_dict:
                return f"{title}: нет данных."
            lines = []
            for uid, pts in sorted(data_dict.items(), key=lambda x: x[1], reverse=True):
                u = session.query(User).get(uid)
                name = u.full_name or u.username or str(u.telegram_id)
                lines.append(f"{name}: {pts}")
            return f"{title}:\n" + "\n".join(lines)

        text = "\n\n".join(
            [
                format_block("Неделя", totals_week),
                format_block("Месяц", totals_month),
                format_block("Год", totals_year),
                format_block("За всё время", totals_all),
            ]
        )

    await update.message.reply_text(text)


async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    week_start, week_end, month_start, month_end, year_start, year_end = get_period_bounds_for_today(
        get_today()
    )
    tg_user = update.effective_user

    with SessionLocal() as session:
        user = get_or_create_user(session, tg_user)
        comps = session.query(Completion).filter(Completion.user_id == user.id).all()

        if not comps:
            await update.message.reply_text("У вас пока нет выполненных задач.")
            return

        total_all = 0
        total_week = 0
        total_month = 0
        total_year = 0

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
        f"Неделя: {total_week}\nМесяц: {total_month}\nГод: {total_year}\nЗа всё время: {total_all}"
    )


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(
            f"У вас нет доступа. Ваш id: {update.effective_user.id}."
        )
        return

    week_start, week_end, month_start, month_end, year_start, year_end = get_period_bounds_for_today(
        get_today()
    )

    with SessionLocal() as session:
        rows = (
            session.query(User, Completion)
            .join(Completion, Completion.user_id == User.id)
            .all()
        )

        if not rows:
            await update.message.reply_text("Нет статистики.")
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

        def top3_block(title, data_dict):
            if not data_dict:
                return f"{title}: нет данных."
            lines = []
            for uid, pts in sorted(
                data_dict.items(), key=lambda x: x[1], reverse=True
            )[:3]:
                u = session.query(User).get(uid)
                name = u.full_name or u.username or str(u.telegram_id)
                lines.append(f"{name}: {pts}")
            return f"{title}:\n" + "\n".join(lines)

        text = "\n\n".join(
            [
                top3_block("Топ недели", totals_week),
                top3_block("Топ месяца", totals_month),
                top3_block("Топ года", totals_year),
                top3_block("Топ за всё время", totals_all),
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
            await context.bot.send_message(chat_id=chat_id, text="На сегодня задач нет!")
            return

        lines = []
        for inst in instances:
            tmpl = inst.template
            text = format_task_button_text(inst, tmpl)
            lines.append(text)

    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))


async def send_daily_summary(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data.get("chat_id") if context.job and context.job.data else None
    if chat_id is None:
        return

    today = date.today()
    week_start, week_end, month_start, month_end, year_start, year_end = get_period_bounds_for_today(
        today
    )

    with SessionLocal() as session:
        rows = (
            session.query(User, Completion)
            .join(Completion, Completion.user_id == User.id)
            .all()
        )

        if not rows:
            await context.bot.send_message(chat_id=chat_id, text="Нет статистики.")
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
                return f"{title}: нет данных."
            lines = []
            for uid, pts in sorted(totals.items(), key=lambda x: x[1], reverse=True)[:3]:
                u = session.query(User).get(uid)
                name = u.full_name or u.username or str(u.telegram_id)
                lines.append(f"{name}: {pts}")
            return f"{title}:\n" + "\n".join(lines)

        parts = []
        if totals_today:
            day_lines = []
            for uid, pts in sorted(
                totals_today.items(), key=lambda x: x[1], reverse=True
            )[:3]:
                u = session.query(User).get(uid)
                name = u.full_name or u.username or str(u.telegram_id)
                day_lines.append(f"{name}: {pts}")
            parts.append("Сегодня:\n" + "\n".join(day_lines))
        else:
            parts.append("Сегодня: нет выполненных задач.")

        if today == week_end:
            parts.append(block_for_period("Неделя", week_start, week_end))
        if today == month_end:
            parts.append(block_for_period("Месяц", month_start, month_end))
        if today == year_end:
            parts.append(block_for_period("Год", year_start, year_end))

    await context.bot.send_message(chat_id=chat_id, text="\n\n".join(parts))


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


async def setup_commands(application):
    commands = [
        BotCommand("start", "Начать"),
        BotCommand("today", "Задачи на сегодня"),
        BotCommand("mytasks", "Мои задачи"),
        BotCommand("add", "Добавить задачу"),
        BotCommand("again", "Повторить задачу"),
        BotCommand("mystats", "Моя статистика"),
        BotCommand("stats", "Статистика"),
        BotCommand("leaderboard", "Топ"),
        BotCommand("score", "Баллы"),
        BotCommand("allow", "Добавить пользователя"),
        BotCommand("list_templates", "Шаблоны задач"),
        BotCommand("deactivate", "Активировать/деактивировать шаблон"),
    ]
    await application.bot.set_my_commands(commands)


def main():
    init_db()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_task))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("again", again))
    application.add_handler(CommandHandler("done", done))
    application.add_handler(CommandHandler("score", score))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("my_stats", my_stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("allow", allow_user))
    application.add_handler(CommandHandler("mytasks", mytasks))
    application.add_handler(CommandHandler("list_templates", list_templates))
    application.add_handler(CommandHandler("deactivate", deactivate))

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
