import re
import os
import subprocess
import json
from datetime import datetime, timedelta

import requests
from google.cloud import speech

# =========================
# CONFIG
# =========================

BITRIX_WEBHOOK = "https://itcmedia.bitrix24.ru/rest/1/jt2ne3cxjgbujoqn/"
BITRIX_DOMAIN = "https://itcmedia.bitrix24.ru"

SAVE_FOLDER = "records"
CACHE_FILE = "processed_calls.json"

# ✅ теперь универсально
FFMPEG_PATH = "ffmpeg"

USE_CREATE_DATE = 0

os.makedirs(SAVE_FOLDER, exist_ok=True)

# ❗️ ВАЖНО: ничего не задаем руками
# credentials подхватятся из окружения (GitHub Actions)

client = speech.SpeechClient()

# =========================
# CACHE
# =========================

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(list(cache), f)

processed = load_cache()

# =========================
# UTILS
# =========================

def safe_request(url, payload):
    try:
        return requests.post(url, json=payload, timeout=20).json()
    except Exception as e:
        print("❌ Request error:", e)
        return {}

def parse_date(t):
    try:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S%z")
    except:
        return None

def format_time(t):
    dt = parse_date(t)
    if dt:
        return dt.strftime("%d.%m.%Y %H:%M")
    return t or "—"

# =========================
# ПРОВЕРКА ДУБЛЯ
# =========================

def already_in_timeline(deal_id, activity_id):
    data = safe_request(BITRIX_WEBHOOK + "crm.timeline.comment.list", {
        "filter": {
            "ENTITY_TYPE": "deal",
            "ENTITY_ID": deal_id
        }
    })

    for x in data.get("result", []):
        txt = (x.get("COMMENT") or "").lower()
        if f"activity_id={activity_id}" in txt:
            return True

    return False

# =========================
# СДЕЛКИ
# =========================

def get_deals():
    start = 0
    all_items = []

    date_from = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    date_field = ">=DATE_CREATE" if USE_CREATE_DATE == 1 else ">=DATE_MODIFY"

    while True:
        data = safe_request(BITRIX_WEBHOOK + "crm.deal.list", {
            "filter": {date_field: date_from},
            "select": ["ID","TITLE"],
            "start": start
        })

        batch = data.get("result", [])
        all_items.extend(batch)

        if "next" not in data:
            break

        start = data["next"]

    return all_items

# =========================
# ЗВОНКИ
# =========================

def get_calls(deal_id):
    start = 0
    all_calls = []

    while True:
        data = safe_request(BITRIX_WEBHOOK + "crm.activity.list", {
            "filter": {
                "OWNER_TYPE_ID": 2,
                "OWNER_ID": deal_id,
                "TYPE_ID": 2
            },
            "select": ["ID","FILES","START_TIME","CREATED"],
            "start": start
        })

        batch = data.get("result", [])
        all_calls.extend(batch)

        if "next" not in data:
            break

        start = data["next"]

    calls = []

    for a in all_calls:
        if not a.get("FILES"):
            continue

        call_time = a.get("START_TIME") or a.get("CREATED")

        for f in a["FILES"]:
            file_id = f.get("id")

            file_data = safe_request(
                BITRIX_WEBHOOK + "disk.file.get",
                {"id": file_id}
            )

            url = file_data.get("result", {}).get("DOWNLOAD_URL")

            if url:
                if url.startswith("/"):
                    url = BITRIX_DOMAIN + url

                calls.append({
                    "activity_id": a.get("ID"),
                    "url": url,
                    "time": call_time
                })

    calls.sort(key=lambda x: parse_date(x["time"]) or datetime.min, reverse=True)

    return calls[:3]

# =========================
# DOWNLOAD
# =========================

def download_audio(url, path):
    try:
        r = requests.get(url, timeout=20)

        if r.status_code != 200:
            return False

        with open(path, "wb") as f:
            f.write(r.content)

        return os.path.getsize(path) > 10000

    except Exception as e:
        print("❌ Download error:", e)
        return False

# =========================
# CONVERT
# =========================

def convert_to_wav(input_path, output_path):
    result = subprocess.run([
        FFMPEG_PATH,
        "-y",
        "-i", input_path,
        "-ac", "1",
        "-ar", "16000",
        output_path
    ], capture_output=True)

    if result.returncode != 0:
        print("❌ ffmpeg error:", result.stderr.decode())
        return False

    return os.path.exists(output_path)

# =========================
# SPLIT
# =========================

def split_audio(input_path, chunk_length=50):
    base = input_path.replace(".wav", "")
    chunks = []

    subprocess.run([
        FFMPEG_PATH,
        "-i", input_path,
        "-f","segment",
        "-segment_time", str(chunk_length),
        "-c","copy",
        base + "_%03d.wav"
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    i = 0
    while True:
        chunk = f"{base}_{i:03d}.wav"
        if not os.path.exists(chunk):
            break
        chunks.append(chunk)
        i += 1

    return chunks

# =========================
# TRANSCRIBE
# =========================

def transcribe(file_path):
    chunks = split_audio(file_path)
    full_text = ""

    for chunk in chunks:
        with open(chunk, "rb") as f:
            content = f.read()

        audio = speech.RecognitionAudio(content=content)

        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
            sample_rate_hertz=16000,
            language_code="ru-RU",
            enable_automatic_punctuation=True
        )

        try:
            response = client.recognize(config=config, audio=audio)

            for r in response.results:
                full_text += r.alternatives[0].transcript + " "

        except Exception as e:
            print("❌ Speech error:", e)

    return full_text.strip()

# =========================
# SAVE
# =========================

def save_to_timeline(deal_id, activity_id, text, call_time):

    comment = f"""📞 Звонок (activity_id={activity_id})
🕒 {format_time(call_time)}

{text}
"""

    safe_request(BITRIX_WEBHOOK + "crm.timeline.comment.add", {
        "fields": {
            "ENTITY_ID": deal_id,
            "ENTITY_TYPE": "deal",
            "COMMENT": comment
        }
    })

# =========================
# MAIN
# =========================

for deal in get_deals():

    print(f"\n💼 {deal['TITLE']}")

    calls = get_calls(deal["ID"])

    if not calls:
        print("❌ Нет звонков")
        continue

    for call in calls:

        aid = str(call["activity_id"])

        if aid in processed:
            print("⏭ Уже в кеше:", aid)
            continue

        if already_in_timeline(deal["ID"], aid):
            print("⏭ Уже есть в Bitrix:", aid)
            processed.add(aid)
            save_cache(processed)
            continue

        raw_path = f"{SAVE_FOLDER}/{aid}.mp3"
        wav_path = f"{SAVE_FOLDER}/{aid}.wav"

        print("⬇️ Скачивание")

        if not download_audio(call["url"], raw_path):
            continue

        print("🔄 Конвертация")

        if not convert_to_wav(raw_path, wav_path):
            continue

        print("⏳ Распознавание")

        text = transcribe(wav_path)

        if not text or len(text) < 10:
            print("⚠️ Пусто")
            continue

        print("📝 Сохранение")

        save_to_timeline(
            deal["ID"],
            aid,
            text,
            call["time"]
        )

        processed.add(aid)
        save_cache(processed)

print("\n🚀 Готово")
