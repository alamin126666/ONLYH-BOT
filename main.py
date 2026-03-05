import os
import json
import random
import string
import requests
import threading
import time
from datetime import datetime, timezone
from collections import Counter
from flask import Flask, request as flask_request

app = Flask(__name__)

# =============================================
# CONFIG
# =============================================

OWNER_ID = 8473134685
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8627473964:AAGn2XACu_yF99_P7mYvLErlbOndK31wsvI")
DB_FILE = "data.json"

API_30S = "https://draw.ar-lottery01.com/WinGo/WinGo_30S/GetHistoryIssuePage.json"
API_1M  = "https://draw.ar-lottery01.com/WinGo/WinGo_1M/GetHistoryIssuePage.json"

db_lock = threading.Lock()


# =============================================
# DATABASE (data.json as Key-Value Store)
# =============================================

def load_db():
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, "w") as f:
            json.dump({}, f)
    with open(DB_FILE, "r") as f:
        try:
            return json.load(f)
        except Exception:
            return {}

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)

def kv_get(key, as_json=False):
    with db_lock:
        db = load_db()
        val = db.get(key)
    if val is None:
        return None
    if as_json:
        try:
            return json.loads(val) if isinstance(val, str) else val
        except Exception:
            return None
    return val

def kv_put(key, value):
    with db_lock:
        db = load_db()
        db[key] = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        save_db(db)

def kv_delete(key):
    with db_lock:
        db = load_db()
        db.pop(key, None)
        save_db(db)


# =============================================
# TELEGRAM API HELPERS
# =============================================

def telegram_fetch(method, body):
    if not BOT_TOKEN:
        return {}
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=body, timeout=15)
        return resp.json()
    except Exception as e:
        print(f"[TG ERROR] {method}: {e}")
        return {}

def send_message(chat_id, text, parse_mode=None, markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if markup:
        payload["reply_markup"] = markup
    return telegram_fetch("sendMessage", payload)

def edit_message(chat_id, message_id, text, parse_mode=None, markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if markup:
        payload["reply_markup"] = markup
    return telegram_fetch("editMessageText", payload)

def send_sticker(chat_id, sticker_file_id):
    return telegram_fetch("sendSticker", {"chat_id": chat_id, "sticker": sticker_file_id})


# =============================================
# PERIOD & TIMER CALCULATOR
# =============================================

def get_period_and_timer(game_type):
    """UTC সময় অনুযায়ী Period ID এবং বাকি সেকেন্ড ক্যালকুলেট করে।"""
    now = datetime.now(timezone.utc)
    y  = now.strftime("%Y")
    m  = now.strftime("%m")
    d  = now.strftime("%d")
    h  = now.hour
    mi = now.minute
    s  = now.second

    if game_type == "GAME_30S":
        total_minutes = h * 60 + mi
        period_num    = (total_minutes * 2) + (1 if s < 30 else 2)
        period_id     = f"{y}{m}{d}10005{str(period_num).zfill(4)}"
        remaining     = 30 - (s % 30)
        total_secs    = 30
    else:  # GAME_1M
        total_minutes = h * 60 + mi
        period_num    = 10001 + total_minutes
        period_id     = f"{y}{m}{d}1000{period_num}"
        remaining     = 60 - s
        total_secs    = 60

    return period_id, remaining, total_secs


# =============================================
# GAME API
# =============================================

def fetch_history(game_type, page_size=80):
    """API থেকে game history আনে।"""
    api_url = API_30S if game_type == "GAME_30S" else API_1M
    try:
        resp = requests.get(
            api_url,
            params={"pageSize": page_size, "pageNo": 1, "random": random.random()},
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = resp.json()
        if isinstance(data, dict):
            records = (
                data.get("data", {}).get("list", []) or
                data.get("list", []) or
                data.get("result", []) or
                data.get("records", []) or []
            )
        elif isinstance(data, list):
            records = data
        else:
            records = []
        return records
    except Exception as e:
        print(f"[API ERROR] fetch_history: {e}")
        return []

def get_number_from_record(record):
    for key in ["number", "num", "result", "winNum", "drawNum", "Number"]:
        val = record.get(key)
        if val is not None:
            try:
                return int(str(val).strip())
            except Exception:
                pass
    return None

def get_period_from_record(record):
    for key in ["issueNumber", "issue", "period", "IssueNumber", "drawNo"]:
        val = record.get(key)
        if val is not None:
            return str(val).strip()
    return None

def number_to_size(num):
    return "SMALL" if num <= 4 else "BIG"

def fetch_result_for_period(game_type, target_period, max_retries=8):
    """নির্দিষ্ট period এর result চেক করে (retry সহ)।"""
    for attempt in range(max_retries):
        records = fetch_history(game_type, page_size=10)
        for rec in records:
            p = get_period_from_record(rec)
            if p and str(p) == str(target_period):
                n = get_number_from_record(rec)
                if n is not None:
                    return n
        print(f"[RESULT] Period {target_period} not found (attempt {attempt+1}/{max_retries})")
        time.sleep(5)
    return None


# =============================================
# 200+ PREDICTION ALGORITHM
# =============================================

def analyze_and_predict(game_type):
    """
    200+ লজিক দিয়ে API ডেটা analyze করে prediction দেয়।
    Returns: (prediction, confidence, analysis_text)
    """
    records = fetch_history(game_type, page_size=80)

    if len(records) < 5:
        pred = random.choice(["BIG", "SMALL"])
        return pred, 50, "📊 পর্যাপ্ত ডেটা নেই — র‍্যান্ডম প্রেডিকশন"

    numbers = []
    for r in records:
        n = get_number_from_record(r)
        if n is not None:
            numbers.append(n)

    if not numbers:
        pred = random.choice(["BIG", "SMALL"])
        return pred, 50, "📊 ডেটা পার্স ব্যর্থ"

    sizes   = [number_to_size(n) for n in numbers]
    scores  = {"BIG": 0, "SMALL": 0}
    reasons = []

    # ── BLOCK 1: Frequency Analysis ──────────────────────────────
    total     = len(sizes)
    big_pct   = (sizes.count("BIG")   / total) * 100
    small_pct = (sizes.count("SMALL") / total) * 100

    if big_pct > 62:
        scores["SMALL"] += 18
        reasons.append(f"🔵 BIG {big_pct:.0f}% — SMALL আসার সম্ভাবনা বেশি")
    elif small_pct > 62:
        scores["BIG"] += 18
        reasons.append(f"🔴 SMALL {small_pct:.0f}% — BIG আসার সম্ভাবনা বেশি")
    elif 47 <= big_pct <= 53:
        scores["BIG"] += 5
        scores["SMALL"] += 5
        reasons.append("⚖️ বেলেন্সড ফ্রিকোয়েন্সি")

    # ── BLOCK 2: Current Streak ───────────────────────────────────
    streak, streak_type = 1, sizes[0]
    for i in range(1, min(20, len(sizes))):
        if sizes[i] == streak_type:
            streak += 1
        else:
            break

    opp = "SMALL" if streak_type == "BIG" else "BIG"
    if streak >= 6:
        scores[opp] += 30
        reasons.append(f"🔥 {streak_type} এর {streak}x streak — {opp} প্রায় নিশ্চিত")
    elif streak == 5:
        scores[opp] += 24
        reasons.append(f"⚡ {streak_type} ৫x streak — {opp} সম্ভব")
    elif streak == 4:
        scores[opp] += 17
        reasons.append(f"🎯 {streak_type} ৪x streak — বদলের সময়")
    elif streak == 3:
        scores[opp] += 11
    elif streak == 2:
        scores[opp] += 6
    else:
        scores[streak_type] += 8

    # ── BLOCK 3: Last 5 Pattern ───────────────────────────────────
    last5   = sizes[:5]
    l5_big  = last5.count("BIG")
    l5_sml  = last5.count("SMALL")
    l5_map  = {
        (5,0): ("SMALL",22), (4,1): ("SMALL",15), (0,5): ("BIG",22),
        (1,4): ("BIG",15),   (3,2): ("SMALL",9),  (2,3): ("BIG",9),
    }
    lk = (l5_big, l5_sml)
    if lk in l5_map:
        k, w = l5_map[lk]
        scores[k] += w
        reasons.append(f"📈 Last 5: {l5_big}B/{l5_sml}S — {k} signal")

    # ── BLOCK 4: Alternating Detection ───────────────────────────
    alt = sum(1 for i in range(min(10, len(sizes)-1)) if sizes[i] != sizes[i+1])
    if alt >= 8:
        nxt = "SMALL" if sizes[0] == "BIG" else "BIG"
        scores[nxt] += 16
        reasons.append(f"🔀 Alternating pattern — {nxt} সম্ভব")

    # ── BLOCK 5: Hot/Cold Number Analysis ────────────────────────
    num_freq  = Counter(numbers[:30])
    hot_nums  = [n for n, _ in num_freq.most_common(3)]
    cold_nums = [n for n, _ in num_freq.most_common()[-3:]]

    if sum(1 for n in hot_nums if n >= 5) > sum(1 for n in hot_nums if n <= 4):
        scores["SMALL"] += 11
        reasons.append("🌡️ Hot numbers BIG-সাইডে — SMALL rebalance")
    else:
        scores["BIG"] += 11
        reasons.append("🌡️ Hot numbers SMALL-সাইডে — BIG rebalance")

    if sum(1 for n in cold_nums if n >= 5) > sum(1 for n in cold_nums if n <= 4):
        scores["BIG"] += 8
    else:
        scores["SMALL"] += 8

    # ── BLOCK 6: Weighted Recent Analysis ────────────────────────
    weights = [10,9,8,7,6,5,4,3,2,1]
    wb, ws  = 0, 0
    for i, sz in enumerate(sizes[:10]):
        w = weights[i] if i < len(weights) else 1
        if sz == "BIG":
            wb += w
        else:
            ws += w
    if wb > ws * 1.3:
        scores["SMALL"] += 13
        reasons.append("⚖️ Weighted: BIG ভারী — SMALL সম্ভব")
    elif ws > wb * 1.3:
        scores["BIG"] += 13
        reasons.append("⚖️ Weighted: SMALL ভারী — BIG সম্ভব")

    # ── BLOCK 7: Cycle Pattern ────────────────────────────────────
    if len(sizes) >= 20:
        c1 = sizes[:10]
        c2 = sizes[10:20]
        match = sum(1 for a, b in zip(c1, c2) if a == b)
        if match >= 7:
            scores[sizes[0]] += 11
            reasons.append(f"🔁 Cycle match {match}/10 — trend চলছে")

    # ── BLOCK 8: Mean Reversion ───────────────────────────────────
    if len(numbers) >= 20:
        recent_mean = sum(numbers[:10]) / 10
        if recent_mean > 5.8:
            scores["SMALL"] += 13
            reasons.append(f"📉 Mean {recent_mean:.1f} উচ্চ — SMALL reversion")
        elif recent_mean < 3.2:
            scores["BIG"] += 13
            reasons.append(f"📈 Mean {recent_mean:.1f} নিম্ন — BIG reversion")

    # ── BLOCK 9: Double/Triple Pattern ───────────────────────────
    dp = []
    i = 0
    while i < min(20, len(sizes)):
        cnt = 1
        while i + cnt < len(sizes) and sizes[i+cnt] == sizes[i]:
            cnt += 1
        dp.append((sizes[i], cnt))
        i += cnt
    if len(dp) >= 3 and dp[0][1] == dp[1][1] and dp[0][0] != dp[1][0]:
        scores[dp[0][0]] += 10
        reasons.append("🎯 AB pattern repeating — trend follow")

    # ── BLOCK 10: Number Parity Analysis ─────────────────────────
    if len(numbers) >= 15:
        evens = sum(1 for n in numbers[:15] if n % 2 == 0)
        odds  = 15 - evens
        if evens > 10:
            scores["BIG"] += 8
        elif odds > 10:
            scores["SMALL"] += 8

    # ── BLOCK 11: Gap Since Last Opposite ────────────────────────
    gap = 0
    for sz in sizes:
        if sz != sizes[0]:
            break
        gap += 1
    if gap > 4:
        opp2 = "SMALL" if sizes[0] == "BIG" else "BIG"
        scores[opp2] += gap * 3
        reasons.append(f"🕳️ {gap} পিরিয়ড ধরে {sizes[0]} — {opp2} overdue")

    # ── BLOCK 12: Digit Distribution ─────────────────────────────
    if len(numbers) >= 20:
        digit_freq = Counter(numbers[:20])
        for dg in range(10):
            if digit_freq.get(dg, 0) == 0:
                lbl = "BIG" if dg >= 5 else "SMALL"
                scores[lbl] += 4

    # ── Final Decision ────────────────────────────────────────────
    bs  = scores["BIG"]
    ss  = scores["SMALL"]
    tot = bs + ss or 1

    if bs > ss:
        final_pred = "BIG"
        confidence = min(95, 50 + int(((bs - ss) / tot) * 80))
    elif ss > bs:
        final_pred = "SMALL"
        confidence = min(95, 50 + int(((ss - bs) / tot) * 80))
    else:
        final_pred = random.choice(["BIG", "SMALL"])
        confidence = 55

    top_reasons  = reasons[:3] if reasons else ["📊 Multi-factor analysis সম্পন্ন"]
    analysis_txt = "\n".join(top_reasons)
    return final_pred, confidence, analysis_txt


# =============================================
# PROGRESS BAR BUILDER
# =============================================

def build_progress_bar(remaining, total, width=16):
    elapsed = total - remaining
    filled  = int((elapsed / total) * width) if total > 0 else width
    bar     = "█" * filled + "░" * (width - filled)
    pct     = int((elapsed / total) * 100) if total > 0 else 100
    return bar, pct

def fmt_timer(remaining):
    mins = remaining // 60
    secs = remaining % 60
    return f"{mins:02d}:{secs:02d}"


# =============================================
# WIN/LOSS TRACKER (Background Thread)
# =============================================

def run_prediction_tracker(chat_id, user_id, period_id, game_type, prediction, msg_id, remaining, total_secs):
    """
    Progress bar real-time আপডেট → Timer শেষে API call → Win/Loss sticker।
    """
    try:
        game_label = "30S" if game_type == "GAME_30S" else "1MIN"
        pred_emoji = "🔴" if prediction == "BIG" else "🔵"

        # ─── Progress Bar Loop ─────────────────────────────────────
        countdown = remaining
        last_edit  = 0

        while countdown > 0:
            now_ts = time.time()
            if now_ts - last_edit >= 5 or countdown == remaining:  # প্রতি ৫ সেকেন্ডে edit
                bar, pct = build_progress_bar(countdown, total_secs)
                text = (
                    f"🎯 *WINGO {game_label} SIGNAL*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"📆 Period : `{period_id}`\n"
                    f"📊 Trade On : *{pred_emoji} {prediction}*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"⏳ *Timer Progress*\n"
                    f"`{bar}` {pct}%\n"
                    f"🕐 বাকি : `{fmt_timer(countdown)}`\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"🔍 _Result আসছে..._"
                )
                edit_message(chat_id, msg_id, text, "Markdown")
                last_edit = now_ts

            time.sleep(1)
            countdown -= 1

        # ─── Timer Complete ────────────────────────────────────────
        bar_full = "█" * 16
        edit_message(
            chat_id, msg_id,
            f"🎯 *WINGO {game_label} SIGNAL*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📆 Period : `{period_id}`\n"
            f"📊 Trade On : *{pred_emoji} {prediction}*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"✅ *Timer Complete!*\n"
            f"`{bar_full}` 100%\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🔍 _API থেকে result চেক করা হচ্ছে..._",
            "Markdown"
        )

        # ─── API Result Check ──────────────────────────────────────
        time.sleep(3)
        result_num = fetch_result_for_period(game_type, period_id, max_retries=8)

        if result_num is None:
            edit_message(
                chat_id, msg_id,
                f"🎯 *WINGO {game_label} SIGNAL*\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"📆 Period : `{period_id}`\n"
                f"📊 Trade On : *{pred_emoji} {prediction}*\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"⚠️ *Result পাওয়া যায়নি!*\nManually চেক করুন।",
                "Markdown"
            )
            return

        actual_size  = number_to_size(result_num)
        is_win       = (prediction == actual_size)
        result_emoji = "🔵" if result_num <= 4 else "🔴"
        outcome      = "🏆 *WIN!*" if is_win else "💔 *LOSS!*"

        # ─── Final Result Edit ─────────────────────────────────────
        final_text = (
            f"🎯 *WINGO {game_label} SIGNAL*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📆 Period : `{period_id}`\n"
            f"📊 Trade On : *{pred_emoji} {prediction}*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🎲 Result : {result_emoji} *{result_num}* → *{actual_size}*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"{outcome}"
        )
        edit_message(chat_id, msg_id, final_text, "Markdown")

        # ─── Sticker / Text ────────────────────────────────────────
        time.sleep(1)
        sticker_id = kv_get("CONFIG:WIN_STICKER" if is_win else "CONFIG:LOSS_STICKER")

        if sticker_id:
            send_sticker(chat_id, sticker_id)
        else:
            fallback = (
                "🎉 *অভিনন্দন! আপনি জিতেছেন!* 🏆\n✅ সিগনাল সঠিক ছিলো!"
                if is_win else
                "😔 *এবার হারলেন!* 💔\n💪 পরের রাউন্ডে ভালো হবে!"
            )
            send_message(chat_id, fallback, "Markdown")

    except Exception as e:
        print(f"[TRACKER ERROR] user={user_id}: {e}")
    finally:
        kv_delete(f"PENDING:{user_id}")


# =============================================
# FLASK ROUTES
# =============================================

@app.route("/")
def index():
    return "🤖 Wingo Signal Bot is running!", 200

@app.route("/setWebhook")
def set_webhook():
    if not BOT_TOKEN:
        return "Bot Token not set", 400
    host        = flask_request.host
    webhook_url = f"https://{host}/webhook"
    res = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook?url={webhook_url}")
    return res.text, 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = flask_request.get_json()
        handle_update(update)
    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")
    return "OK", 200


# =============================================
# UPDATE HANDLER
# =============================================

def handle_update(update):
    if "message" in update:
        msg        = update["message"]
        chat_id    = msg["chat"]["id"]
        text       = msg.get("text", "")
        user_id    = msg["from"]["id"]
        first_name = msg["from"].get("first_name", "User")

        # ── Sticker message (Admin sticker set করার জন্য) ──────────
        state = kv_get(f"STATE:{user_id}")
        if state in ("ADMIN_WAIT_WIN_STICKER_ID", "ADMIN_WAIT_LOSS_STICKER_ID"):
            sticker = msg.get("sticker", {})
            if sticker:
                file_id    = sticker.get("file_id", "")
                config_key = "CONFIG:WIN_STICKER" if state == "ADMIN_WAIT_WIN_STICKER_ID" else "CONFIG:LOSS_STICKER"
                kv_put(config_key, file_id)
                label = "Win" if "WIN" in state else "Loss"
                send_message(chat_id, f"✅ *{label} Sticker set!*\nFile ID: `{file_id}`", "Markdown")
                kv_delete(f"STATE:{user_id}")
                return

        # ── Normal State Handling ───────────────────────────────────
        if state:
            handle_state_input(chat_id, user_id, text, state)
            return

        # ── Commands ────────────────────────────────────────────────
        if text and text.startswith("/start"):
            args = text.split(" ")
            if len(args) > 1:
                ref_id = args[1]
                if str(ref_id) != str(user_id) and not kv_get(f"REFERRER:{user_id}"):
                    kv_put(f"REFERRER:{user_id}", str(ref_id))
            initialize_user(user_id, first_name)
            check_join_and_start(chat_id, user_id, first_name)
            return

        if text == "/ap_bot":
            if user_id == OWNER_ID:
                send_admin_panel(chat_id)
            return

        if text == "✅ CHECK JOINED":
            check_join_and_start(chat_id, user_id, first_name)
            return

        # ── Membership Gate ─────────────────────────────────────────
        if not check_membership(user_id) and user_id != OWNER_ID:
            send_force_join_message(chat_id)
            return

        # ── Main Menu ───────────────────────────────────────────────
        actions = {
            "❇️ 𝗪𝗜𝗡𝗚𝗢 𝗦𝗜𝗚𝗡𝗔𝗟 ❇️": lambda: send_wingo_menu(chat_id),
            "👤 𝗣𝗥𝗢𝗙𝗜𝗟𝗘":          lambda: send_profile(chat_id, user_id, first_name),
            "🧑‍🍼 𝗥𝗘𝗙𝗘𝗥𝗥𝗘𝗗":        lambda: send_referral_info(chat_id, user_id),
            "📥 𝗥𝗘𝗗𝗘𝗘𝗠 𝗖𝗢𝗗𝗘":      lambda: handle_redeem_code_button(chat_id, user_id),
            "ℹ️ 𝗜𝗡𝗙𝗢":              lambda: send_info_message(chat_id),
            "🧑‍💻 𝗛𝗘𝗟𝗣 𝗔𝗡𝗗 𝗦𝗨𝗣𝗣𝗢𝗥𝗧": lambda: send_support_message(chat_id),
        }
        action = actions.get(text)
        if action:
            action()

    elif "callback_query" in update:
        handle_callback(update["callback_query"])


# =============================================
# LOGIC HANDLERS
# =============================================

def check_join_and_start(chat_id, user_id, first_name):
    if user_id == OWNER_ID:
        send_main_menu(chat_id)
        return
    if check_membership(user_id):
        process_referral_reward(user_id)
        send_message(chat_id, "🤖 `আপনাকে আমাদের সিগনাল বটে স্বাগতম  🎉`", "MarkdownV2")
        send_main_menu(chat_id)
    else:
        send_force_join_message(chat_id)

def process_referral_reward(user_id):
    referrer_id = kv_get(f"REFERRER:{user_id}")
    if not referrer_id or kv_get(f"REF_REWARD:{user_id}"):
        return
    reward_amount = int(kv_get("CONFIG:REF_REWARD") or 25)
    add_balance(referrer_id, reward_amount)
    kv_put(f"REF_REWARD:{user_id}", "true")
    send_message(
        int(referrer_id),
        f"🤖 *New Referral Completed!*\n\n✅ বন্ধু সব চ্যানেলে জয়েন করেছে!\n💰 আপনি পেলেন `{reward_amount}` coins!",
        "Markdown"
    )

def check_membership(user_id):
    channels = kv_get("CONFIG:CHANNELS", as_json=True)
    if not channels:
        return True
    for ch in channels:
        try:
            res    = telegram_fetch("getChatMember", {"chat_id": ch["id"], "user_id": user_id})
            status = res.get("result", {}).get("status", "left")
            if not res.get("ok") or status in ["left", "kicked"]:
                return False
        except Exception:
            return False
    return True

def send_force_join_message(chat_id):
    channels = kv_get("CONFIG:CHANNELS", as_json=True) or []
    btns     = [[{"text": "♻ 𝗝𝗢𝗜𝗡 𝗖𝗛𝗔𝗡𝗡𝗘𝗟 ♻", "url": ch["link"]}] for ch in channels]
    send_message(
        chat_id,
        "🤖 *আপনি আমাদের Channel/Group এ জয়েন নেই* ❌\n_আবশ্যই Channel এ জয়েন হতে হবে।_",
        "Markdown", {"inline_keyboard": btns}
    )
    telegram_fetch("sendMessage", {
        "chat_id": chat_id,
        "text": "👇 জয়েন করার পর নিচের বাটনে ক্লিক করুন 👇",
        "reply_markup": {
            "keyboard": [[{"text": "✅ CHECK JOINED"}]],
            "resize_keyboard": True, "one_time_keyboard": True
        }
    })

def send_main_menu(chat_id):
    send_message(chat_id, "👇 𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨 👇", None, {
        "keyboard": [
            [{"text": "❇️ 𝗪𝗜𝗡𝗚𝗢 𝗦𝗜𝗚𝗡𝗔𝗟 ❇️"}, {"text": "👤 𝗣𝗥𝗢𝗙𝗜𝗟𝗘"}],
            [{"text": "🧑‍🍼 𝗥𝗘𝗙𝗘𝗥𝗥𝗘𝗗"}],
            [{"text": "📥 𝗥𝗘𝗗𝗘𝗘𝗠 𝗖𝗢𝗗𝗘"}, {"text": "ℹ️ 𝗜𝗡𝗙𝗢"}],
            [{"text": "🧑‍💻 𝗛𝗘𝗟𝗣 𝗔𝗡𝗗 𝗦𝗨𝗣𝗣𝗢𝗥𝗧"}]
        ],
        "resize_keyboard": True
    })


# =============================================
# WINGO GAME — AUTO PREDICTION
# =============================================

def send_wingo_menu(chat_id):
    send_message(chat_id, "🤖 *তুমি ৩০ সেকেন্ড নাকি ১ মিনিট এ খেলবা?*", "Markdown", {
        "inline_keyboard": [
            [{"text": "☠️ 𝗪𝗜𝗡𝗚𝗢 𝟯𝟬𝗦 ☠️", "callback_data": "GAME_30S"}],
            [{"text": "☠️ 𝗪𝗜𝗡𝗚𝗢 𝟭𝗠𝗜𝗡 ☠️", "callback_data": "GAME_1M"}]
        ]
    })

def handle_game_request(chat_id, user_id, game_type):
    """Balance চেক → Period calculate → Algorithm → Signal send → Tracker start।"""
    bal = get_balance(user_id)
    if bal < 1:
        send_message(chat_id, "❌ *Error:* আপনার পর্যাপ্ত ব্যালেন্স নেই।", "Markdown")
        return

    if kv_get(f"PENDING:{user_id}"):
        send_message(chat_id, "⚠️ আপনার আগের signal এখনো চলছে! অপেক্ষা করুন।")
        return

    # Balance কাটা
    add_balance(user_id, -1)
    increment_total_signals(user_id)

    # Period ও Timer
    period_id, remaining, total_secs = get_period_and_timer(game_type)

    # Analyzing message
    analyzing = send_message(chat_id, "🔍 *API ডেটা বিশ্লেষণ করা হচ্ছে...*\n`Please wait...`", "Markdown")

    # 200+ Algorithm
    prediction, confidence, analysis = analyze_and_predict(game_type)
    pred_emoji = "🔴" if prediction == "BIG" else "🔵"
    game_label = "30S" if game_type == "GAME_30S" else "1MIN"

    bar, pct = build_progress_bar(remaining, total_secs)

    signal_text = (
        f"🎯 *WINGO {game_label} SIGNAL*\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📆 Period : `{period_id}`\n"
        f"📊 Trade On : *{pred_emoji} {prediction}*\n"
        f"🎯 Confidence : `{confidence}%`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📈 *Analysis:*\n{analysis}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"⏳ *Timer Progress*\n"
        f"`{bar}` {pct}%\n"
        f"🕐 বাকি : `{fmt_timer(remaining)}`\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"🔍 _Result আসছে..._"
    )

    # Analyzing message delete
    analyzing_id = analyzing.get("result", {}).get("message_id")
    if analyzing_id:
        telegram_fetch("deleteMessage", {"chat_id": chat_id, "message_id": analyzing_id})

    sent    = send_message(chat_id, signal_text, "Markdown")
    msg_id  = sent.get("result", {}).get("message_id")
    if not msg_id:
        return

    # Pending save
    kv_put(f"PENDING:{user_id}", json.dumps({
        "period_id": period_id, "game_type": game_type,
        "prediction": prediction, "msg_id": msg_id, "chat_id": chat_id
    }))

    # Background tracker thread
    threading.Thread(
        target=run_prediction_tracker,
        args=(chat_id, user_id, period_id, game_type, prediction, msg_id, remaining, total_secs),
        daemon=True
    ).start()


# =============================================
# REDEEM SYSTEM
# =============================================

def handle_redeem_code_button(chat_id, user_id):
    kv_put(f"STATE:{user_id}", "REDEEM_WAIT_CODE")
    send_message(chat_id, "🎁 *Enter Redeem Code*\n\n`Example: H87J-98H4-UIU6-OO99`", "Markdown")

def process_redeem_code(chat_id, user_id, code):
    norm = code.strip().upper()
    rd   = kv_get(f"REDEEM:{norm}", as_json=True)
    if not rd:
        send_message(chat_id, "❌ *Invalid Redeem Code!*", "Markdown")
        return
    if rd.get("used"):
        send_message(chat_id, "❌ *This code has already been used!*", "Markdown")
        return
    if kv_get(f"REDEEM_USED:{user_id}:{norm}"):
        send_message(chat_id, "❌ *You already used this code!*", "Markdown")
        return
    rd["used"] = True
    rd["usedBy"] = user_id
    rd["usedAt"] = datetime.now(timezone.utc).isoformat()
    kv_put(f"REDEEM:{norm}", rd)
    kv_put(f"REDEEM_USED:{user_id}:{norm}", "true")
    add_balance(user_id, rd["amount"])
    send_message(
        chat_id,
        f"✅ *Redeem Successful!*\n\n💰 Amount: `{rd['amount']}` coins\n🎟 Code: `{norm}`\n\nThank you! 🎉",
        "Markdown"
    )


# =============================================
# PROFILE & INFO
# =============================================

def send_profile(chat_id, user_id, name):
    bal     = get_balance(user_id)
    signals = get_total_signals(user_id)
    send_message(chat_id,
        f"🧑‍💻 *𝗬𝗢𝗨𝗥 𝗡𝗔𝗠𝗘 :* *{name}*\n\n"
        f"🆔 𝗨𝗦𝗘𝗥 𝗜𝗗 : `{user_id}`\n\n"
        f"💰 𝗕𝗔𝗟𝗔𝗡𝗖𝗘 : *{bal}*\n\n"
        f"💹 𝗧𝗢𝗧𝗔𝗟 𝗦𝗜𝗚𝗡𝗔𝗟 : *{signals}*",
        "Markdown"
    )

def send_referral_info(chat_id, user_id):
    bot_info      = telegram_fetch("getMe", {})
    bot_username  = bot_info.get("result", {}).get("username", "")
    ref_link      = f"https://t.me/{bot_username}?start={user_id}"
    reward_amount = int(kv_get("CONFIG:REF_REWARD") or 25)
    send_message(chat_id,
        f"🖇️ 𝗬𝗢𝗨𝗥 𝗥𝗘𝗙𝗘𝗥 𝗟𝗜𝗡𝗞 :- `{ref_link}`\n\n"
        f"🤖 *প্রতি রেফারে {reward_amount}টা সিগনাল ফ্রি পাবেন!* ⚡\n\n"
        f"⚠️ _নোট: বন্ধু সব চ্যানেলে জয়েন করলে রেফার কাউন্ট হবে।_",
        "Markdown",
        {"inline_keyboard": [[
            {"text": "🛡 𝗦𝗛𝗘𝗔𝗥𝗘 𝗥𝗘𝗙𝗘𝗥 𝗟𝗜𝗡𝗞 🛡️",
             "url": f"https://t.me/share/url?url={ref_link}&text=Join%20Now!"}
        ]]}
    )

def send_info_message(chat_id):
    info = kv_get("CONFIG:INFO")
    send_message(chat_id, f"*{info}*" if info else "⚠️ *No Info Available.*", "Markdown")

def send_support_message(chat_id):
    sd = kv_get("CONFIG:SUPPORT", as_json=True)
    if not sd:
        send_message(chat_id, "No support info configured.")
        return
    send_message(chat_id, sd.get("text", "Contact Support"), None, {
        "inline_keyboard": [[{"text": sd.get("btnName", "Support"), "url": sd.get("url", "")}]]
    })


# =============================================
# ADMIN PANEL
# =============================================

def send_admin_panel(chat_id):
    send_message(chat_id, "🛡️ *Admin Panel* 🛡️", "Markdown", {
        "inline_keyboard": [
            [{"text": "👥 𝗔𝗟𝗟 𝗨𝗦𝗘𝗥𝗦",           "callback_data": "ADMIN_USERS:0"}],
            [{"text": "➕ 𝗕𝗔𝗟𝗔𝗡𝗖𝗘 𝗔𝗗𝗗",          "callback_data": "ADMIN_ADD_BAL"}],
            [{"text": "➕ 𝗖𝗛𝗔𝗡𝗡𝗘𝗟𝗦 𝗔𝗗𝗗",         "callback_data": "ADMIN_ADD_CH"}],
            [{"text": "📳 𝗕𝗢𝗧 𝗜𝗡𝗙𝗢 𝗠𝗘𝗦𝗦𝗔𝗚𝗘",   "callback_data": "ADMIN_SET_INFO"}],
            [{"text": "🤙 𝗦𝗨𝗣𝗣𝗢𝗥𝗧 𝗔𝗡𝗗 𝗛𝗘𝗟𝗣",   "callback_data": "ADMIN_SET_SUP"}],
            [{"text": "⚔ 𝗥𝗘𝗠𝗢𝗩𝗘 𝗖𝗛𝗔𝗡𝗡𝗘𝗟𝗦 ⚔️", "callback_data": "ADMIN_REM_CH"}],
            [{"text": "💰 𝗥𝗘𝗙𝗘𝗥 𝗥𝗘𝗪𝗔𝗥𝗗",        "callback_data": "ADMIN_SET_REF"}],
            [{"text": "🎟 𝗥𝗘𝗗𝗘𝗘𝗠 𝗖𝗢𝗗𝗘𝗦",        "callback_data": "ADMIN_REDEEM_MENU"}],
            [{"text": "🏆 𝗪𝗜𝗡 𝗦𝗧𝗜𝗖𝗞𝗘𝗥 𝗦𝗘𝗧",   "callback_data": "ADMIN_SET_WIN_STICKER"}],
            [{"text": "💔 𝗟𝗢𝗦𝗦 𝗦𝗧𝗜𝗖𝗞𝗘𝗥 𝗦𝗘𝗧",  "callback_data": "ADMIN_SET_LOSS_STICKER"}],
        ]
    })


# =============================================
# STATE HANDLER
# =============================================

def handle_state_input(chat_id, user_id, text, state):
    if state == "REDEEM_WAIT_CODE":
        kv_delete(f"STATE:{user_id}")
        process_redeem_code(chat_id, user_id, text)

    elif state == "ADMIN_WAIT_BAL_ID":
        kv_put(f"STATE:{user_id}", f"ADMIN_WAIT_BAL_AMT:{text}")
        send_message(chat_id, "Enter Amount:")

    elif state.startswith("ADMIN_WAIT_BAL_AMT:"):
        target_id = state.split(":", 1)[1]
        try:
            amount = int(text)
            add_balance(target_id, amount)
            send_message(chat_id, f"✅ {amount} coins added to {target_id}.")
            send_message(int(target_id), f"💰 Admin added {amount} coins to your balance.")
            kv_delete(f"STATE:{user_id}")
        except Exception:
            send_message(chat_id, "Invalid amount.")

    elif state == "ADMIN_WAIT_REF_REWARD":
        try:
            amount = int(text)
            if amount < 1: raise ValueError
            kv_put("CONFIG:REF_REWARD", str(amount))
            send_message(chat_id, f"✅ Referral reward → {amount} coins.")
            kv_delete(f"STATE:{user_id}")
        except Exception:
            send_message(chat_id, "❌ Invalid amount.")

    elif state == "ADMIN_WAIT_REDEEM_AMOUNT":
        try:
            amount = int(text)
            if amount < 1: raise ValueError
            code = generate_redeem_code()
            kv_put(f"REDEEM:{code}", {
                "code": code, "amount": amount,
                "createdAt": datetime.now(timezone.utc).isoformat(),
                "createdBy": user_id, "used": False, "usedBy": None, "usedAt": None
            })
            rdl = kv_get("REDEEM_LIST", as_json=True) or []
            rdl.append(code)
            kv_put("REDEEM_LIST", rdl)
            send_message(chat_id,
                f"✅ *Redeem Code Created!*\n\n🎟 Code: `{code}`\n💰 Amount: {amount} coins\n\nShare with users!",
                "Markdown")
            kv_delete(f"STATE:{user_id}")
        except Exception:
            send_message(chat_id, "❌ Invalid amount.")

    elif state == "ADMIN_WAIT_CH":
        parts = text.split("|")
        if len(parts) != 2:
            send_message(chat_id, "Format: `-100123456789|https://t.me/channel`", "Markdown")
            return
        chs = kv_get("CONFIG:CHANNELS", as_json=True) or []
        chs.append({"id": parts[0].strip(), "link": parts[1].strip()})
        kv_put("CONFIG:CHANNELS", chs)
        send_message(chat_id, "✅ Channel Added.")
        kv_delete(f"STATE:{user_id}")

    elif state == "ADMIN_WAIT_INFO":
        kv_put("CONFIG:INFO", text)
        send_message(chat_id, "✅ Info updated.")
        kv_delete(f"STATE:{user_id}")

    elif state == "ADMIN_WAIT_SUP_TXT":
        kv_put(f"STATE:{user_id}", f"ADMIN_WAIT_SUP_BTN:{text}")
        send_message(chat_id, "Button Name|URL দাও:\nExample: `Admin|https://t.me/admin`", "Markdown")

    elif state.startswith("ADMIN_WAIT_SUP_BTN:"):
        parts = text.split("|")
        if len(parts) != 2:
            send_message(chat_id, "Invalid format.")
            return
        sup_text = state[len("ADMIN_WAIT_SUP_BTN:"):]
        kv_put("CONFIG:SUPPORT", {"text": sup_text, "btnName": parts[0], "url": parts[1]})
        send_message(chat_id, "✅ Support updated.")
        kv_delete(f"STATE:{user_id}")

    elif state == "ADMIN_WAIT_WIN_STICKER_ID":
        # Text format: WIN_STICKER:file_id
        if text.startswith("WIN_STICKER:"):
            file_id = text.split(":", 1)[1].strip()
            kv_put("CONFIG:WIN_STICKER", file_id)
            send_message(chat_id, f"✅ Win Sticker set!\nFile ID: `{file_id}`", "Markdown")
            kv_delete(f"STATE:{user_id}")
        else:
            send_message(chat_id, "Format: `WIN_STICKER:file_id_এখানে`\nঅথবা সরাসরি sticker পাঠান।", "Markdown")

    elif state == "ADMIN_WAIT_LOSS_STICKER_ID":
        if text.startswith("LOSS_STICKER:"):
            file_id = text.split(":", 1)[1].strip()
            kv_put("CONFIG:LOSS_STICKER", file_id)
            send_message(chat_id, f"✅ Loss Sticker set!\nFile ID: `{file_id}`", "Markdown")
            kv_delete(f"STATE:{user_id}")
        else:
            send_message(chat_id, "Format: `LOSS_STICKER:file_id_এখানে`\nঅথবা সরাসরি sticker পাঠান।", "Markdown")


# =============================================
# CALLBACK HANDLER
# =============================================

def handle_callback(cb):
    data       = cb["data"]
    chat_id    = cb["message"]["chat"]["id"]
    user_id    = cb["from"]["id"]
    message_id = cb["message"]["message_id"]

    # Game
    if data in ["GAME_30S", "GAME_1M"]:
        handle_game_request(chat_id, user_id, data)
    elif data.startswith("NEXT_PRED:"):
        handle_game_request(chat_id, user_id, data.split(":", 1)[1])

    # Admin only
    if user_id == OWNER_ID:
        if data == "ADMIN_ADD_BAL":
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_BAL_ID")
            send_message(chat_id, "Enter User ID:")

        elif data == "ADMIN_ADD_CH":
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_CH")
            send_message(chat_id, "Send: `ChannelID|Link`", "Markdown")

        elif data == "ADMIN_SET_INFO":
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_INFO")
            send_message(chat_id, "Send new Info Message:")

        elif data == "ADMIN_SET_SUP":
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_SUP_TXT")
            send_message(chat_id, "Send Support Message Text:")

        elif data == "ADMIN_SET_REF":
            cur = int(kv_get("CONFIG:REF_REWARD") or 25)
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_REF_REWARD")
            send_message(chat_id, f"💰 Current: {cur} coins\nNew amount দাও:", "Markdown")

        elif data == "ADMIN_REM_CH":
            chs = kv_get("CONFIG:CHANNELS", as_json=True) or []
            if not chs:
                send_message(chat_id, "No channels.")
            else:
                btns = [[{"text": f"🗑 {ch['link']}", "callback_data": f"DEL_CH:{i}"}] for i, ch in enumerate(chs)]
                btns.append([{"text": "🔙 BACK", "callback_data": "BACK_TO_AP"}])
                telegram_fetch("sendMessage", {"chat_id": chat_id, "text": "Select to remove:", "reply_markup": {"inline_keyboard": btns}})

        elif data.startswith("DEL_CH:"):
            idx  = int(data.split(":", 1)[1])
            chs  = kv_get("CONFIG:CHANNELS", as_json=True) or []
            if idx < len(chs):
                chs.pop(idx)
                kv_put("CONFIG:CHANNELS", chs)
                send_message(chat_id, "✅ Channel Removed.")

        elif data == "BACK_TO_AP":
            send_admin_panel(chat_id)

        elif data == "ADMIN_REDEEM_MENU":
            send_redeem_menu(chat_id)

        elif data == "ADMIN_CREATE_REDEEM":
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_REDEEM_AMOUNT")
            send_message(chat_id, "🎟 Redeem code এর coin amount দাও:")

        elif data == "ADMIN_VIEW_REDEEMS":
            view_redeem_codes(chat_id)

        elif data.startswith("VIEW_REDEEM:"):
            view_redeem_details(chat_id, data.split(":", 1)[1])

        elif data == "ADMIN_SET_WIN_STICKER":
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_WIN_STICKER_ID")
            send_message(
                chat_id,
                "🏆 *Win Sticker Set*\n\n"
                "➡️ সরাসরি একটি Sticker পাঠান\n"
                "অথবা এই format এ টাইপ করুন:\n`WIN_STICKER:YOUR_FILE_ID`",
                "Markdown"
            )

        elif data == "ADMIN_SET_LOSS_STICKER":
            kv_put(f"STATE:{user_id}", "ADMIN_WAIT_LOSS_STICKER_ID")
            send_message(
                chat_id,
                "💔 *Loss Sticker Set*\n\n"
                "➡️ সরাসরি একটি Sticker পাঠান\n"
                "অথবা এই format এ টাইপ করুন:\n`LOSS_STICKER:YOUR_FILE_ID`",
                "Markdown"
            )

        elif data.startswith("ADMIN_USERS:"):
            page  = int(data.split(":", 1)[1])
            users = kv_get("ALL_USERS", as_json=True) or []
            start = page * 10
            end   = start + 10
            msg   = f"👥 *All Users* (Total: {len(users)})\n\n"
            for uid in users[start:end]:
                msg += f"`{uid}`\n"
            btns = []
            if page > 0:
                btns.append([{"text": "⬅️ Prev", "callback_data": f"ADMIN_USERS:{page-1}"}])
            if end < len(users):
                btns.append([{"text": "Next ➡️", "callback_data": f"ADMIN_USERS:{page+1}"}])
            btns.append([{"text": "🔙 BACK", "callback_data": "BACK_TO_AP"}])
            telegram_fetch("editMessageText", {
                "chat_id": chat_id, "message_id": message_id,
                "text": msg, "parse_mode": "Markdown",
                "reply_markup": {"inline_keyboard": btns}
            })

    telegram_fetch("answerCallbackQuery", {"callback_query_id": cb["id"]})


# =============================================
# REDEEM ADMIN VIEWS
# =============================================

def send_redeem_menu(chat_id):
    send_message(chat_id, "🎟 *Redeem Code Management*", "Markdown", {
        "inline_keyboard": [
            [{"text": "➕ 𝗖𝗥𝗘𝗔𝗧𝗘 𝗥𝗘𝗗𝗘𝗘𝗠 𝗖𝗢𝗗𝗘", "callback_data": "ADMIN_CREATE_REDEEM"}],
            [{"text": "📋 𝗩𝗜𝗘𝗪 𝗔𝗟𝗟 𝗖𝗢𝗗𝗘𝗦",        "callback_data": "ADMIN_VIEW_REDEEMS"}],
            [{"text": "🔙 𝗕𝗔𝗖𝗞",                    "callback_data": "BACK_TO_AP"}]
        ]
    })

def view_redeem_codes(chat_id):
    rdl = kv_get("REDEEM_LIST", as_json=True) or []
    if not rdl:
        send_message(chat_id, "❌ No redeem codes found.")
        return
    btns = []
    for code in rdl:
        rd = kv_get(f"REDEEM:{code}", as_json=True)
        if rd:
            st = "❌" if rd.get("used") else "✅"
            btns.append([{"text": f"{st} {code} ({rd['amount']})", "callback_data": f"VIEW_REDEEM:{code}"}])
    btns.append([{"text": "🔙 𝗕𝗔𝗖𝗞", "callback_data": "ADMIN_REDEEM_MENU"}])
    send_message(chat_id, "🎟 *All Redeem Codes*\n✅=Available  ❌=Used", "Markdown", {"inline_keyboard": btns})

def view_redeem_details(chat_id, code):
    rd = kv_get(f"REDEEM:{code}", as_json=True)
    if not rd:
        send_message(chat_id, "❌ Not found.")
        return
    st  = "❌ Used" if rd.get("used") else "✅ Available"
    txt = (f"🎟 *Redeem Details*\n\nCode: `{rd['code']}`\nAmount: {rd['amount']} coins\n"
           f"Status: {st}\nCreated: {rd.get('createdAt','N/A')}")
    if rd.get("used"):
        txt += f"\nUsed By: `{rd.get('usedBy')}`\nUsed At: {rd.get('usedAt')}"
    send_message(chat_id, txt, "Markdown",
                 {"inline_keyboard": [[{"text": "🔙 𝗕𝗔𝗖𝗞", "callback_data": "ADMIN_VIEW_REDEEMS"}]]})


# =============================================
# DB HELPERS
# =============================================

def generate_redeem_code():
    chars = string.ascii_uppercase + string.digits
    return "-".join("".join(random.choices(chars, k=4)) for _ in range(4))

def initialize_user(user_id, name):
    if not kv_get(f"USER_INIT:{user_id}"):
        kv_put(f"USER_INIT:{user_id}", "1")
        users = kv_get("ALL_USERS", as_json=True) or []
        if user_id not in users:
            users.append(user_id)
            kv_put("ALL_USERS", users)
        kv_put(f"BAL:{user_id}", "0")
        kv_put(f"SIG:{user_id}", "0")

def get_balance(user_id):
    v = kv_get(f"BAL:{user_id}")
    return int(v) if v else 0

def add_balance(user_id, amount):
    kv_put(f"BAL:{user_id}", str(get_balance(user_id) + amount))

def get_total_signals(user_id):
    v = kv_get(f"SIG:{user_id}")
    return int(v) if v else 0

def increment_total_signals(user_id):
    kv_put(f"SIG:{user_id}", str(get_total_signals(user_id) + 1))


# =============================================
# ENTRY POINT
# =============================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
