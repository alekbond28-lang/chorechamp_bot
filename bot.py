import os
import asyncio
import random
import string
from datetime import timedelta, time, date, datetime
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
    Application,
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
    House,
    HouseOnboarding,
    TaskTemplate,
    TaskInstance,
    Completion,
    get_today,
)

load_dotenv()
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

LOCAL_TZ = ZoneInfo("Europe/Moscow")
MAIN_CHAT_ID = None

OWNER_ID = 680630275

ACCESS_TEXT = (
    "Этот бот доступен только по приглашению.\n"
    "Твой id: {uid}\n"
    "Передай его владельцу дома, чтобы он добавил тебя или прислал код дома."
)

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("/today")],
        [KeyboardButton("/add"), KeyboardButton("/again")],
        [KeyboardButton("/leaderboard")],
        [KeyboardButton("/list_templates")],
    ],
    resize_keyboard=True,
)

HOUSE_NAMES = [
    "Гнездо героев",
    "Домовой штаб HomeHero",
    "Вселенная порядка",
    "Лига чистоты",
    "Команда супер-уборки",
]

def random_house_name() -> str:
    return random.choice(HOUSE_NAMES)


# ---------- Вспомогательные ----------

def is_global_owner(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id == OWNER_ID)

def get_user(session, tg_user) -> User | None:
    if not tg_user:
        return None
    return session.query(User).filter_by(telegram_id=tg_user.id).first()

def get_or_create_user(session, tg_user) -> User:
    user = get_user(session, tg_user)
    if not user:
        user = User(
            telegram_id=tg_user.id,
            username=tg_user.username,
            full_name=tg_user.full_name,
        )
        session.add(user)
        session.commit()
    return user

def ensure_access(update: Update):
    tg_user = update.effective_user
    if not tg_user:
        return "Не удалось распознать пользователя."
    with SessionLocal() as session:
        user = get_user(session, tg_user)
        if not user or not user.house_id:
            uid = tg_user.id
            return (
                "Для использования бота нужно быть в доме.\n\n"
                "1. Создайте новый дом.\n"
                "2. Или сообщите владельцу дома ваш код: {uid} и дождитесь приглашения.\n\n"
                "Нажмите кнопку ниже, чтобы создать дом."
            ).format(uid=uid)
    return None

def generate_join_code(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

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


# ---------- Дом, онбординг ----------

DEFAULT_ONBOARDING_TEXT = (
    "Добро пожаловать в домовой бот HomeHero!\n\n"
    "Как пользоваться:\n\n"
    "• /today — главный экран.\n"
    "  Показывает задачи на сегодня с вкладками:\n"
    "  - Free — свободные задачи, которые можно взять.\n"
    "  - My — задачи, которые сейчас в работе у вас.\n"
    "  - Done — все выполненные сегодня задачи всех участников.\n\n"
    "• Чтобы начать:\n"
    "  1) Зайдите в /today.\n"
    "  2) Во вкладке Free нажмите «Взять» — задача появится во вкладке My.\n"
    "  3) После выполнения нажмите «Выполнить» — задача перейдёт в Done, а вы получите баллы.\n\n"
    "• /add — добавить новый тип задачи (название, баллы, периодичность).\n"
    "• /again — отметить, что уже существующая задача выполнена ещё раз.\n"
    "• /leaderboard — рейтинг участников по баллам за неделю, месяц, год и всё время.\n"
    "• /list_templates — список всех типов задач с настройками.\n\n"
    "Бот автоматически:\n"
    "• переносит невыполненные задачи на следующий день;\n"
    "• создаёт повторяющиеся задачи по расписанию;\n"
    "• отправляет утренний список задач и вечерние итоги дня."
)

async def send_house_onboarding_message(bot, chat_id: int, house: House, session):
    onboarding = house.onboarding_text
    if not onboarding:
        onboarding = HouseOnboarding(house_id=house.id, text=DEFAULT_ONBOARDING_TEXT)
        session.add(onboarding)
        session.commit()
    await bot.send_message(chat_id=chat_id, text=onboarding.text)

def user_in_house(session, tg_user) -> tuple[User | None, House | None]:
    user = get_or_create_user(session, tg_user)
    if not user.house_id:
        return user, None
    house = session.query(House).filter_by(id=user.house_id).first()
    return user, house


# ---------- Формирование текста/клавиатур ----------

def format_task_button_text(inst: TaskInstance) -> str:
    tmpl = inst.template
    prefix = ""
    if getattr(inst, "priority", None) == "high":
        prefix = "🔥 "

    status_text = {
        "free": "⚪ свободна",
        "in_progress": "🕒 в работе",
        "done": "✅ выполнена",
    }.get(inst.status, inst.status)

    performer = ""
    if inst.status in ("in_progress", "done") and inst.assigned_user:
        performer_name = inst.assigned_user.full_name or inst.assigned_user.username
        performer = f" у {performer_name}"

    return f"{prefix}{tmpl.title} — {tmpl.points} баллов — {status_text}{performer}"

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

def build_today_view(session, tab: str, tg_user) -> tuple[str, InlineKeyboardMarkup]:
    user, house = user_in_house(session, tg_user)
    if not house:
        filter_row = [
            InlineKeyboardButton("Free", callback_data="filter:free"),
            InlineKeyboardButton("My", callback_data="filter:my"),
            InlineKeyboardButton("Done", callback_data="filter:done"),
        ]
        markup = InlineKeyboardMarkup([filter_row])
        return "Сначала нужно создать дом или быть добавленным в дом.", markup

    today_date = get_today()
    base_q = (
        session.query(TaskInstance)
        .join(TaskTemplate)
        .filter(TaskInstance.date == today_date)
        .filter(TaskTemplate.house_id == house.id)
        .filter(TaskTemplate.deleted.is_(False))
    )

    if tab == "free":
        q = base_q.filter(TaskInstance.status == "free")
        instances = q.all()
        title = "Задачи на сегодня (вкладка: Free)"
        empty_text = "Свободных задач на сегодня нет."
    elif tab == "my":
        q = base_q.filter(
            TaskInstance.status == "in_progress",
            TaskInstance.assigned_user_id == user.id,
        )
        instances = q.all()
        title = "Задачи на сегодня (вкладка: My)"
        empty_text = "У тебя сейчас нет задач в работе."
    elif tab == "done":
        q = base_q.filter(TaskInstance.status == "done")
        instances = q.all()
        title = "Задачи на сегодня (вкладка: Done)"
        empty_text = "Сегодня ещё никто не завершал задачи."
    else:
        instances = []
        title = "Задачи на сегодня"
        empty_text = "На сегодня задач нет."

    if tab == "free" and not instances:
        my_exists = base_q.filter(
            TaskInstance.status == "in_progress",
            TaskInstance.assigned_user_id == user.id,
        ).first()
        done_exists = base_q.filter(TaskInstance.status == "done").first()

        if not my_exists and not done_exists:
            title = "Ты хорошо поработал, задач больше нет, до завтра!"
            filter_row = [
                InlineKeyboardButton("Free", callback_data="filter:free"),
                InlineKeyboardButton("My", callback_data="filter:my"),
                InlineKeyboardButton("Done", callback_data="filter:done"),
            ]
            markup = InlineKeyboardMarkup([filter_row])
            return title, markup

    tasks_markup = build_today_keyboard(instances, tg_user.id)
    filter_row = [
        InlineKeyboardButton("Free", callback_data="filter:free"),
        InlineKeyboardButton("My", callback_data="filter:my"),
        InlineKeyboardButton("Done", callback_data="filter:done"),
    ]
    full_keyboard = InlineKeyboardMarkup(
        [filter_row] + list(tasks_markup.inline_keyboard)
    )
    if not instances:
        title = empty_text
    return title, full_keyboard


# ---------- Планировщик ----------

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
        templates = (
            session.query(TaskTemplate)
            .filter(TaskTemplate.deleted.is_(False))
            .all()
        )

        for tmpl in templates:
            if tmpl.start_date and today < tmpl.start_date:
                continue

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


# ---------- Команды ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    chat_id = update.effective_chat.id

    with SessionLocal() as session:
        user = get_or_create_user(session, tg_user)

        if not user.house_id:
            uid = tg_user.id
            keyboard = [
                [
                    InlineKeyboardButton("🏠 Создать дом", callback_data="house:create"),
                ]
            ]
            await update.message.reply_text(
                "Привет! Я HomeHero — бот, который помогает честно распределять домашние дела.\n\n"
                "Для использования бота нужно быть в доме.\n\n"
                "1. Создайте новый дом.\n"
                "2. Или сообщите владельцу дома ваш код: {uid} и дождитесь приглашения.\n\n"
                "Нажмите «Создать дом», чтобы начать.".format(uid=uid),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        house = session.query(House).filter_by(id=user.house_id).first()

    await update.message.reply_text(
        f"Привет! Ты уже в доме «{house.name or 'без названия'}».\n"
        "Используй /today, чтобы посмотреть задачи на сегодня.",
        reply_markup=MAIN_KEYBOARD,
    )

    with SessionLocal() as session:
        _, house = user_in_house(session, tg_user)
        if house:
            await send_house_onboarding_message(context.bot, chat_id, house, session)


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        tg_user = update.effective_user
        uid = tg_user.id if tg_user else "unknown"
        keyboard = [
            [InlineKeyboardButton("🏠 Создать дом", callback_data="house:create")],
        ]
        await update.message.reply_text(
            access_error.format(uid=uid),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    chat_id = update.effective_chat.id
    tg_user = update.effective_user

    with SessionLocal() as session:
        title, markup = build_today_view(session, "free", tg_user)

    await context.bot.send_message(
        chat_id=chat_id,
        text=title,
        reply_markup=markup,
    )


async def add_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        tg_user = update.effective_user
        uid = tg_user.id if tg_user else "unknown"
        keyboard = [
            [InlineKeyboardButton("🏠 Создать дом", callback_data="house:create")],
        ]
        await update.message.reply_text(
            access_error.format(uid=uid),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    context.user_data["add_state"] = "waiting_title"
    context.user_data.pop("add_title", None)
    context.user_data.pop("add_points", None)

    await update.message.reply_text("Пришли название задачи.")


async def add_task_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        tg_user = update.effective_user
        uid = tg_user.id if tg_user else "unknown"
        keyboard = [
            [InlineKeyboardButton("🏠 Создать дом", callback_data="house:create")],
        ]
        await update.message.reply_text(
            access_error.format(uid=uid),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
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


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error.format(uid=update.effective_user.id))
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
        user, house = user_in_house(session, update.effective_user)
        if not house or tmpl.house_id != house.id:
            await update.message.reply_text("Эта задача не из твоего дома.")
            return

        inst.status = "done"
        inst.done_by_user_id = user.id
        inst.done_at = datetime.utcnow()
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
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error.format(uid=update.effective_user.id))
        return

    chat_id = update.effective_chat.id
    today_date = get_today()

    with SessionLocal() as session:
        user, house = user_in_house(session, update.effective_user)
        if not house:
            await update.message.reply_text("Сначала нужно быть в доме.")
            return

        instances = (
            session.query(TaskInstance)
            .join(TaskTemplate)
            .filter(TaskInstance.date == today_date)
            .filter(TaskTemplate.house_id == house.id)
            .filter(TaskTemplate.deleted.is_(False))
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
        await update.message.reply_text(access_error.format(uid=update.effective_user.id))
        return

    with SessionLocal() as session:
        user, house = user_in_house(session, update.effective_user)
        if not house:
            await update.message.reply_text("Сначала нужно создать дом.")
            return

        templates = (
            session.query(TaskTemplate)
            .filter(TaskTemplate.house_id == house.id)
            .filter(TaskTemplate.deleted.is_(False))
            .order_by(TaskTemplate.id)
            .all()
        )

        if not templates:
            await update.message.reply_text("Шаблонов задач пока нет.")
            return

        keyboard_rows = []
        for tmpl in templates:
            start_str = tmpl.start_date.isoformat() if tmpl.start_date else "cегодня"
            info_text = (
                f"{tmpl.id}. {tmpl.title} — {tmpl.periodicity}, "
                f"{tmpl.points} баллов, начиная с: {start_str}"
            )
            info_btn = InlineKeyboardButton(info_text, callback_data="noop")

            settings_btn = InlineKeyboardButton("⚙️ Настройки", callback_data=f"template_settings:{tmpl.id}")
            keyboard_rows.append([info_btn])
            keyboard_rows.append([settings_btn])

        markup = InlineKeyboardMarkup(keyboard_rows)

    await update.message.reply_text(
        "Шаблоны задач:",
        reply_markup=markup,
    )


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    access_error = ensure_access(update)
    if access_error:
        await update.message.reply_text(access_error.format(uid=update.effective_user.id))
        return

    (week_start, week_end), (month_start, month_end), (year_start, year_end) = get_period_bounds_for_today()

    with SessionLocal() as session:
        user, house = user_in_house(session, update.effective_user)
        if not house:
            await update.message.reply_text("Сначала нужно быть в доме.")
            return

        rows = (
            session.query(User, Completion, TaskInstance, TaskTemplate)
            .join(Completion, Completion.user_id == User.id)
            .join(TaskInstance, Completion.task_instance_id == TaskInstance.id)
            .join(TaskTemplate, TaskInstance.template_id == TaskTemplate.id)
            .filter(TaskTemplate.house_id == house.id)
            .all()
        )

        if not rows:
            await update.message.reply_text("Лидеров пока нет — никто не заработал баллы")
            return

        totals_week = {}
        totals_month = {}
        totals_year = {}
        totals_all = {}

        for user_row, comp, inst, tmpl in rows:
            day = comp.created_at.date()
            uid = user_row.id
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
                u = next(u for u, *_ in rows if u.id == uid)
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


# ---------- Allow / дома / онбординг ----------

async def allow_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with SessionLocal() as session:
        inviter = get_or_create_user(session, update.effective_user)
        if not inviter.house_id or not inviter.is_house_owner:
            await update.message.reply_text("Только владелец дома может приглашать участников.")
            return

        house = session.query(House).filter_by(id=inviter.house_id).first()

        if not context.args:
            await update.message.reply_text("Формат: /allow <telegram_id>")
            return

        try:
            new_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("id должен быть числом")
            return

        user = session.query(User).filter_by(telegram_id=new_id).first()
        if not user:
            await update.message.reply_text("Пользователь ещё ни разу не писал боту.")
            return

        if user.house_id == house.id:
            await update.message.reply_text(f"Пользователь {new_id} уже в доме.")
            return

        user.house_id = house.id
        session.commit()

        await update.message.reply_text(f"Пользователь {new_id} добавлен в дом ✅")

        await send_house_onboarding_message(update.get_bot(), update.effective_chat.id, house, session)


# ---------- Дайджесты ----------

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
            .filter(TaskTemplate.deleted.is_(False))
            .all()
        )

        if not instances:
            await context.bot.send_message(chat_id=chat_id, text="На сегодня дел нет! 🎉")
            return

        lines = [format_task_button_text(inst) for inst in instances]

    await context.bot.send_message(
        chat_id=chat_id,
        text="Ежедневный дайджест задач от HomeHero:\n" + "\n".join(lines),
    )


async def send_daily_summary(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data.get("chat_id") if context.job and context.job.data else None
    if chat_id is None:
        return

    today = date.today()
    (week_start, week_end), (month_start, month_end), (year_start, year_end) = get_period_bounds_for_today()

    with SessionLocal() as session:
        rows = (
            session.query(User, Completion, TaskInstance, TaskTemplate)
            .join(Completion, Completion.user_id == User.id)
            .join(TaskInstance, Completion.task_instance_id == TaskInstance.id)
            .join(TaskTemplate, TaskInstance.template_id == TaskTemplate.id)
            .filter(TaskTemplate.deleted.is_(False))
            .all()
        )

    if not rows:
        await context.bot.send_message(chat_id=chat_id, text="Сегодня никто не заработал баллы.")
        return

    totals_today = {}
    for user, comp, inst, tmpl in rows:
        if comp.created_at.date() == today:
            totals_today[user.id] = totals_today.get(user.id, 0) + comp.points

    def block_for_period(title, start, end):
        totals = {}
        for user, comp, inst, tmpl in rows:
            d = comp.created_at.date()
            if start <= d <= end:
                totals[user.id] = totals.get(user.id, 0) + comp.points
        if not totals:
            return f"{title}: пока нет баллов"
        lines = []
        for uid, pts in sorted(totals.items(), key=lambda x: x[1], reverse=True)[:3]:
            u = next(u for u, *_ in rows if u.id == uid)
            name = u.full_name or u.username or str(u.telegram_id)
            lines.append(f"{name}: {pts}")
        return f"{title}:\n" + "\n".join(lines)

    parts = []

    if totals_today:
        day_lines = []
        for uid, pts in sorted(totals_today.items(), key=lambda x: x[1], reverse=True)[:3]:
            u = next(u for u, *_ in rows if u.id == uid)
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


# ---------- HTTP + webhook ----------

async def health(request):
    return web.Response(text="OK")


async def setup_commands(application: Application):
    commands = [
        BotCommand("start", "Описание бота и главное меню"),
        BotCommand("today", "Показать задачи на сегодня"),
        BotCommand("add", "Добавить новую задачу"),
        BotCommand("again", "Отметить, что задача сделана ещё раз"),
        BotCommand("leaderboard", "Лидеры по баллам"),
        BotCommand("list_templates", "Показать и управлять шаблонами задач"),
        BotCommand("allow", "Добавить участника (только владелец дома)"),
    ]
    await application.bot.set_my_commands(commands)


application: Application  # глобально


async def webhook_handler(request):
    data = await request.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return web.Response(text="OK")


# ---------- Текстовый роутер для /add и редактирования шаблонов ----------

async def handle_template_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    tmpl_id = context.user_data.get("edit_template_id")
    field = context.user_data.get("edit_template_field")

    if not tmpl_id or not field:
        await update.message.reply_text(
            "Не удалось определить, что редактировать. Попробуй через /list_templates ещё раз."
        )
        return

    with SessionLocal() as session:
        tg_user = update.effective_user
        user, house = user_in_house(session, tg_user)
        if not house:
            await update.message.reply_text("Сначала нужно быть в доме.")
            return

        tmpl = (
            session.query(TaskTemplate)
            .filter_by(id=tmpl_id, house_id=house.id, deleted=False)
            .first()
        )
        if not tmpl:
            await update.message.reply_text("Шаблон не найден.")
            return

        if field == "title":
            old = tmpl.title
            if not text:
                await update.message.reply_text("Название не может быть пустым. Пришли новое название.")
                return
            tmpl.title = text
            session.commit()
            await update.message.reply_text(
                f"Название шаблона обновлено ✅\n\nБыло: «{old}»\nСтало: «{tmpl.title}»"
            )

        elif field == "points":
            try:
                new_points = int(text)
            except ValueError:
                await update.message.reply_text("Баллы должны быть числом. Пришли число, например: 5")
                return
            old = tmpl.points
            tmpl.points = new_points
            session.commit()
            await update.message.reply_text(
                f"Баллы обновлены ✅\n\nБыло: {old}\nСтало: {tmpl.points}"
            )

        elif field == "start_date":
            try:
                new_date = date.fromisoformat(text)
            except ValueError:
                await update.message.reply_text(
                    "Не удалось распознать дату. Пришли в формате ГГГГ-ММ-ДД, например: 2026-04-01."
                )
                return
            old = tmpl.start_date
            tmpl.start_date = new_date
            session.commit()
            old_str = old.isoformat() if old else "не задана"
            await update.message.reply_text(
                f"Дата начала обновлена ✅\n\nБыло: {old_str}\nСтало: {tmpl.start_date.isoformat()}"
            )

        else:
            await update.message.reply_text("Это поле пока нельзя редактировать таким образом.")
            return

    context.user_data.pop("edit_template_id", None)
    context.user_data.pop("edit_template_field", None)


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("edit_template_id") and context.user_data.get("edit_template_field"):
        await handle_template_edit_text(update, context)
        return

    await add_task_flow(update, context)


# ---------- CallbackQuery (house + задачи + шаблоны) ----------

async def task_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    tg_user = update.effective_user
    await query.answer()
    data = query.data or ""
    user_tg = query.from_user

    if data == "house:create":
        with SessionLocal() as session:
            user = get_or_create_user(session, tg_user)
            if user.house_id:
                house = session.query(House).filter_by(id=user.house_id).first()
                await query.edit_message_text(
                    f"Ты уже в доме «{house.name or 'без названия'}».\n"
                    f"Код дома: {house.join_code}"
                )
                return

            code = generate_join_code()
            house = House(name=random_house_name(), join_code=code)
            session.add(house)
            session.commit()

            user.house_id = house.id
            user.is_house_owner = True
            session.commit()

            await query.edit_message_text(
                f"Дом «{house.name}» создан! 🏠\n\n"
                f"Код дома: {house.join_code}\n"
                "Передай его участникам, чтобы владелец добавил их через /allow."
            )

            await send_house_onboarding_message(query.get_bot(), query.message.chat_id, house, session)
        return

    if data.startswith("filter:"):
        _, _, filter_code = data.partition(":")
        tab = {
            "free": "free",
            "my": "my",
            "done": "done",
        }.get(filter_code, "free")

        with SessionLocal() as session:
            title, markup = build_today_view(session, tab, user_tg)

        await query.edit_message_text(
            text=title,
            reply_markup=markup,
        )
        return

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
            user, house = user_in_house(session, user_tg)
            if not house:
                await query.edit_message_text("Сначала нужно быть в доме.")
                context.user_data.clear()
                return

            tmpl = TaskTemplate(
                house_id=house.id,
                title=title,
                description=None,
                periodicity=period_code,
                points=points,
                start_date=get_today(),
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

    if data.startswith("template_settings:"):
        _, _, raw_id = data.partition(":")
        try:
            tmpl_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id шаблона.")
            return

        with SessionLocal() as session:
            user, house = user_in_house(session, user_tg)
            if not house:
                await query.edit_message_text("Сначала нужно быть в доме.")
                return

            tmpl = (
                session.query(TaskTemplate)
                .filter_by(id=tmpl_id, house_id=house.id, deleted=False)
                .first()
            )
            if not tmpl:
                await query.edit_message_text("Шаблон не найден.")
                return

            start_str = tmpl.start_date.isoformat() if tmpl.start_date else "cегодня"
            text = (
                f"Настройки шаблона:\n\n"
                f"{tmpl.title}\n"
                f"Баллы: {tmpl.points}\n"
                f"Периодичность: {tmpl.periodicity}\n"
                f"Начиная с: {start_str}"
            )
            keyboard = [
                [
                    InlineKeyboardButton("✏️ Изменить", callback_data=f"template_edit:{tmpl.id}"),
                    InlineKeyboardButton("🗑 Удалить", callback_data=f"template_delete:{tmpl.id}"),
                ],
            ]
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        return

    if data.startswith("template_delete:"):
        _, _, raw_id = data.partition(":")
        try:
            tmpl_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id шаблона.")
            return

        with SessionLocal() as session:
            user, house = user_in_house(session, user_tg)
            if not house:
                await query.edit_message_text("Сначала нужно быть в доме.")
                return

            tmpl = (
                session.query(TaskTemplate)
                .filter_by(id=tmpl_id, house_id=house.id, deleted=False)
                .first()
            )
            if not tmpl:
                await query.edit_message_text("Шаблон не найден.")
                return

            text = (
                f"Удалить шаблон «{tmpl.title}»?\n"
                "Задача исчезнет из списка, новые экземпляры создаваться не будут."
            )
            keyboard = [
                [
                    InlineKeyboardButton("✅ Да, удалить", callback_data=f"template_delete_confirm:{tmpl.id}"),
                    InlineKeyboardButton("↩ Отмена", callback_data="templates_back"),
                ]
            ]
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        return

    if data.startswith("template_delete_confirm:"):
        _, _, raw_id = data.partition(":")
        try:
            tmpl_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id шаблона.")
            return

        with SessionLocal() as session:
            user, house = user_in_house(session, user_tg)
            if not house:
                await query.edit_message_text("Сначала нужно быть в доме.")
                return

            tmpl = (
                session.query(TaskTemplate)
                .filter_by(id=tmpl_id, house_id=house.id, deleted=False)
                .first()
            )
            if not tmpl:
                await query.edit_message_text("Шаблон не найден.")
                return

            tmpl.deleted = True
            session.commit()

        await query.edit_message_text("Шаблон удалён.")
        return

    if data == "templates_back":
        fake_update = Update(
            update_id=update.update_id,
            message=update.effective_message,
        )
        await list_templates(fake_update, context)
        return

    if data.startswith("template_edit:"):
        _, _, raw_id = data.partition(":")
        try:
            tmpl_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id шаблона.")
            return

        with SessionLocal() as session:
            user, house = user_in_house(session, user_tg)
            if not house:
                await query.edit_message_text("Сначала нужно быть в доме.")
                return

            tmpl = (
                session.query(TaskTemplate)
                .filter_by(id=tmpl_id, house_id=house.id, deleted=False)
                .first()
            )
            if not tmpl:
                await query.edit_message_text("Шаблон не найден.")
                return

        keyboard = [
            [InlineKeyboardButton("Название", callback_data=f"template_edit_field:title:{tmpl_id}")],
            [InlineKeyboardButton("Баллы", callback_data=f"template_edit_field:points:{tmpl_id}")],
            [InlineKeyboardButton("Периодичность", callback_data=f"template_edit_field:period:{tmpl_id}")],
            [InlineKeyboardButton("Дата начала", callback_data=f"template_edit_field:start_date:{tmpl_id}")],
        ]
        await query.edit_message_text(
            "Что изменить?",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("template_edit_field:"):
        _, field, raw_id = data.split(":", 2)
        try:
            tmpl_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id шаблона.")
            return

        context.user_data["edit_template_id"] = tmpl_id
        context.user_data["edit_template_field"] = field

        if field == "title":
            await query.edit_message_text("Пришли новое название задачи.")
        elif field == "points":
            await query.edit_message_text("Пришли новое количество баллов (число).")
        elif field == "period":
            keyboard = [
                [
                    InlineKeyboardButton("Единоразово", callback_data=f"template_edit_period:once:{tmpl_id}"),
                    InlineKeyboardButton("Ежедневно", callback_data=f"template_edit_period:daily:{tmpl_id}"),
                ],
                [
                    InlineKeyboardButton("Еженедельно", callback_data=f"template_edit_period:weekly:{tmpl_id}"),
                    InlineKeyboardButton("2 раза в неделю", callback_data=f"template_edit_period:twice_weekly:{tmpl_id}"),
                ],
                [
                    InlineKeyboardButton("Ежемесячно", callback_data=f"template_edit_period:monthly:{tmpl_id}"),
                    InlineKeyboardButton("2 раза в месяц", callback_data=f"template_edit_period:twice_monthly:{tmpl_id}"),
                ],
                [
                    InlineKeyboardButton("Ежеквартально", callback_data=f"template_edit_period:quarterly:{tmpl_id}"),
                ],
            ]
            await query.edit_message_text(
                "Выбери новую периодичность:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        elif field == "start_date":
            await query.edit_message_text(
                "Пришли дату начала в формате ГГГГ-ММ-ДД (например, 2026-04-01)."
            )
        return

    if data.startswith("template_edit_period:"):
        _, period_code, raw_id = data.split(":", 2)
        try:
            tmpl_id = int(raw_id)
        except ValueError:
            await query.edit_message_text("Некорректный id шаблона.")
            return

        with SessionLocal() as session:
            user, house = user_in_house(session, user_tg)
            if not house:
                await query.edit_message_text("Сначала нужно быть в доме.")
                return

            tmpl = (
                session.query(TaskTemplate)
                .filter_by(id=tmpl_id, house_id=house.id, deleted=False)
                .first()
            )
            if not tmpl:
                await query.edit_message_text("Шаблон не найден.")
                return

            tmpl.periodicity = period_code
            session.commit()

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
            f"Периодичность обновлена ✅\n\nТеперь: {period_human}"
        )
        return

    if data == "noop":
        return

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
        user, house = user_in_house(session, user_tg)
        if not house or tmpl.house_id != house.id:
            await query.edit_message_text("Эта задача не из твоего дома.")
            return

        if action == "take":
            if inst.status != "free":
                await query.edit_message_text("Задача уже занята или выполнена.")
                return
            inst.status = "in_progress"
            inst.assigned_user_id = user.id
            session.commit()

        elif action == "drop":
            if inst.status != "in_progress" or inst.assigned_user_id != user.id:
                await query.edit_message_text("Вы не выполняете эту задачу.")
                return
            inst.status = "free"
            inst.assigned_user_id = None
            session.commit()

        elif action == "done":
            if inst.status == "done":
                await query.edit_message_text("Задача уже выполнена.")
                return
            if inst.status == "in_progress" and inst.assigned_user_id != user.id:
                await query.edit_message_text("Вы не выполняете эту задачу.")
                return

            inst.status = "done"
            if inst.assigned_user_id is None:
                inst.assigned_user_id = user.id
            inst.done_by_user_id = user.id
            inst.done_at = datetime.utcnow()

            session.add(Completion(
                user_id=user.id,
                task_instance_id=inst.id,
                points=tmpl.points,
            ))
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

            await query.edit_message_text(
                f"{tmpl.title}\n"
                f"Баллы: {tmpl.points}\n"
                f"Создана новая свободная задача на сегодня."
            )
            return

        else:
            await query.edit_message_text("Неизвестное действие.")
            return

        if is_today_message:
            msg_text = query.message.text or ""
            if "вкладка: My" in msg_text:
                current_tab = "my"
            elif "вкладка: Done" in msg_text:
                current_tab = "done"
            else:
                current_tab = "free"

            if action == "take" and current_tab == "free":
                free_title, free_markup = build_today_view(session, "free", user_tg)
                if "Свободных задач на сегодня нет" in free_title:
                    title, markup = build_today_view(session, "my", user_tg)
                else:
                    title, markup = free_title, free_markup
            elif action == "done" and current_tab == "my":
                my_title, my_markup = build_today_view(session, "my", user_tg)
                if "нет задач в работе" in my_title:
                    title, markup = build_today_view(session, "free", user_tg)
                else:
                    title, markup = my_title, my_markup
            else:
                title, markup = build_today_view(session, current_tab, user_tg)

            await query.edit_message_text(
                text=title,
                reply_markup=markup,
            )

        elif is_mytasks_message:
            await query.edit_message_text("Экран /mytasks скоро будет удалён.")
        else:
            if inst.status == "free":
                status_line = "⚪ свободна"
            elif inst.status == "in_progress":
                status_line = f"🕒 в работе у {user.full_name or user.username}"
            else:
                status_line = f"✅ выполнена {user.full_name or user.username}"

            await query.edit_message_text(
                f"{tmpl.title}\n"
                f"Баллы: {tmpl.points}\n"
                f"Статус: {status_line}"
            )


# ---------- main ----------

async def main():
    global application, MAIN_CHAT_ID

    init_db()

    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add", add_task))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("again", again))
    application.add_handler(CommandHandler("done", done))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("allow", allow_user))
    application.add_handler(CommandHandler("list_templates", list_templates))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
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

    await application.initialize()
    await application.start()
    await setup_commands(application)

    app_host = os.getenv("RENDER_EXTERNAL_HOSTNAME", "chorechamp-bot.onrender.com")
    base_url = f"https://{app_host}"
    webhook_path = "/webhook"
    webhook_url = f"{base_url}{webhook_path}"

    await application.bot.set_webhook(url=webhook_url)

    app = web.Application()
    app.router.add_post(webhook_path, webhook_handler)
    app.router.add_get("/", health)

    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    print(f"Webhook set to: {webhook_url}")
    print(f"Server started on port {port}")

    try:
        await asyncio.Event().wait()
    finally:
        await application.stop()
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
