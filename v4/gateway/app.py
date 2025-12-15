import redis
import json
import requests
from flask import Flask, jsonify, request
from datetime import datetime
import threading
import time
from threading import Lock

# ----------------- Redis -----------------
r = redis.Redis(host='redis', port=6379, db=0) 

def enqueue_rating_update(user_name, increment):
    """Добавляем обновление рейтинга в очередь Redis"""
    r.rpush("rating_queue", json.dumps({"user": user_name, "increment": increment}))

def process_rating_queue():
    """Фоновый процесс для обработки очереди рейтинга"""
    while True:
        item = r.lpop("rating_queue")
        if not item:
            time.sleep(1)
            continue
        data = json.loads(item)
        user_name = data["user"]
        increment = data["increment"]
        try:
            resp = requests.get(f"{RATING_URL}/rating", headers={"X-User-Name": user_name}, timeout=2)
            resp.raise_for_status()
            current_stars = resp.json().get("stars", 1)
            requests.post(f"{RATING_URL}/rating", json={"username": user_name, "stars": current_stars + increment})
        except requests.RequestException:
            # если сервис недоступен — возвращаем задачу обратно в очередь
            r.rpush("rating_queue", item)
            time.sleep(2)

# Запускаем фоновый поток для обработки очереди
threading.Thread(target=process_rating_queue, daemon=True).start()


class CircuitBreaker:
    def __init__(self, failure_threshold=3, retry_timeout=10):
        self.failure_threshold = failure_threshold
        self.retry_timeout = retry_timeout
        self.failure_count = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.last_failure_time = None
        self.lock = Lock()

    def call(self, func, *args, **kwargs):
        with self.lock:
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.retry_timeout:
                    self.state = "HALF_OPEN"
                else:
                    return self.fallback(*args, **kwargs)

        try:
            result = func(*args, **kwargs)
        except Exception:
            with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
            return self.fallback(*args, **kwargs)

        # Успешный ответ → сброс
        with self.lock:
            self.failure_count = 0
            self.state = "CLOSED"
        return result

    def fallback(self, *args, **kwargs):
        return {"message": "Bonus Service unavailable"}
    
library_cb = CircuitBreaker(failure_threshold=3, retry_timeout=10)
rating_cb = CircuitBreaker(failure_threshold=3, retry_timeout=10)
reservation_cb = CircuitBreaker(failure_threshold=3, retry_timeout=10)

app = Flask(__name__)

LIBRARY_URL = "http://library_service:8060"
RATING_URL = "http://rating_service:8050"
RESERVATION_URL = "http://reservation_service:8070"

# -------------------- Вспомогательные функции для запросов --------------------
def fetch_libraries(city, page, size):
    params = {"city": city, "page": page, "size": size}
    resp = requests.get(f"{LIBRARY_URL}/libraries", params=params, timeout=2)
    resp.raise_for_status()
    return resp.json()

def fetch_books(library_uid, page, size, show_all):
    params = {"page": page, "size": size, "showAll": show_all}
    resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}/books", params=params, timeout=2)
    resp.raise_for_status()
    return resp.json()

def fetch_rating(user_name):
    headers = {"X-User-Name": user_name}
    resp = requests.get(f"{RATING_URL}/rating", headers=headers, timeout=2)
    resp.raise_for_status()
    return resp.json()

def fetch_reservations(user_name):
    # Получаем все бронирования пользователя
    resp = requests.get(f"{RESERVATION_URL}/reservations/{user_name}", timeout=2)
    resp.raise_for_status()
    reservations_json = resp.json()
    result = []

    for reservation in reservations_json:
        reservation_uid = reservation.get("reservationUid")
        book_uid = reservation.get("bookUid")
        library_uid = reservation.get("libraryUid")
        start_date = reservation.get("startDate")
        till_date = reservation.get("tillDate")
        status = reservation.get("status", "RENTED")

        # Получаем информацию о книге
        book_data = {}
        if book_uid and library_uid:
            book_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}/{book_uid}", timeout=2)
            if book_resp.status_code == 200:
                book_data = book_resp.json()

        # Получаем информацию о библиотеке
        library_data = {}
        if library_uid:
            library_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}", timeout=2)
            if library_resp.status_code == 200:
                library_data = library_resp.json()

        result.append({
            "reservationUid": reservation_uid,
            "status": status,
            "startDate": start_date,
            "tillDate": till_date,
            "book": {
                "bookUid": book_uid,
                "name": book_data.get("name", ""),
                "author": book_data.get("author", ""),
                "genre": book_data.get("genre", "")
            },
            "library": {
                "libraryUid": library_uid,
                "name": library_data.get("name", ""),
                "address": library_data.get("address", ""),
                "city": library_data.get("city", "")
            }
        })

    return result

# -------------------- Получение библиотек --------------------
@app.route("/api/v1/libraries", methods=["GET"])
def get_libraries():
    city = request.args.get("city", "Москва")
    page = request.args.get("page", 1)
    size = request.args.get("size", 1)

    data = library_cb.call(fetch_libraries, city, page, size)
    return jsonify(data), 200 if "message" not in data else 503

# -------------------- Получение книг --------------------
@app.route("/api/v1/libraries/<library_uid>/books", methods=["GET"])
def get_books(library_uid):
    page = request.args.get("page", 1)
    size = request.args.get("size", 1)
    show_all = request.args.get("showAll", "false").lower() == "true"

    data = library_cb.call(fetch_books, library_uid, page, size, show_all)
    return jsonify(data), 200 if "message" not in data else 503


# -------------------- Получение рейтинга --------------------
@app.route("/api/v1/rating", methods=["GET"])
def get_rating():
    user_name = request.headers.get("X-User-Name")
    if not user_name:
        return jsonify({"error": "X-User-Name header is missing"}), 400

    data = rating_cb.call(fetch_rating, user_name)
    return jsonify(data), 200 if "message" not in data else 503

@app.route("/api/v1/reservations", methods=["GET"])
def get_reservations():
    user_name = request.headers.get("X-User-Name")
    if not user_name:
        return jsonify({"error": "X-User-Name header is missing"}), 400

    data = reservation_cb.call(fetch_reservations, user_name)
    return jsonify(data), 200 if "message" not in data else 503


# -------------------- Создание бронирования --------------------
# ----------------- Create Reservation -----------------
@app.route("/api/v1/reservations", methods=["POST"])
def create_reservation():
    user_name = request.headers.get("X-User-Name")
    data = request.get_json()
    book_uid = data.get("bookUid")
    library_uid = data.get("libraryUid")
    till_date = data.get("tillDate")

    # Проверка лимита по количеству книг
    rented_count = 0
    try:
        rented_resp = requests.get(f"{RESERVATION_URL}/reservations/{user_name}/count")
        if rented_resp.status_code == 200:
            rented_count = rented_resp.json().get("rentedCount", 0)
    except requests.RequestException:
        pass

    stars = 1
    try:
        rating_resp = requests.get(f"{RATING_URL}/rating", headers={"X-User-Name": user_name}, timeout=2)
        rating_resp.raise_for_status()
        stars = rating_resp.json().get("stars", 1)
    except requests.RequestException:
        # сервис недоступен — используем fallback
        stars = 1

    if rented_count >= stars:
        return jsonify({"message": "Maximum number of rented books reached"}), 400

    # Получаем информацию о книге и библиотеке
    book_data = {}
    library_data = {}
    try:
        book_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}/{book_uid}")
        if book_resp.status_code == 200:
            book_data = book_resp.json()
    except requests.RequestException:
        pass

    try:
        library_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}")
        if library_resp.status_code == 200:
            library_data = library_resp.json()
    except requests.RequestException:
        pass

    # Создаём запись в Reservation Service
    payload = {"bookUid": book_uid, "libraryUid": library_uid, "tillDate": till_date}
    headers = {"X-User-Name": user_name, "Content-Type": "application/json"}
    try:
        res = requests.post(f"{RESERVATION_URL}/reservations", json=payload, headers=headers, timeout=2)
        reservation_json = res.json() if res.status_code == 200 else {}
    except requests.RequestException:
        reservation_json = {}

    # Уменьшаем доступные книги
    try:
        requests.patch(f"{LIBRARY_URL}/libraries/{library_uid}/books/{book_uid}/decrement")
    except requests.RequestException:
        pass

    # Обновляем рейтинг через очередь
    try:
        # если сервис доступен — обновляем сразу
        resp = requests.get(f"{RATING_URL}/rating", headers={"X-User-Name": user_name}, timeout=2)
        resp.raise_for_status()
        current_stars = resp.json().get("stars", 1)
        requests.post(f"{RATING_URL}/rating", json={"username": user_name, "stars": current_stars + 1})
    except requests.RequestException:
        # если недоступен — добавляем в очередь
        enqueue_rating_update(user_name, 1)

    response = {
        "reservationUid": reservation_json.get("reservationUid"),
        "status": reservation_json.get("status", "RENTED"),
        "startDate": reservation_json.get("startDate"),
        "tillDate": till_date,
        "book": {
            "bookUid": book_uid,
            "name": book_data.get("name", ""),
            "author": book_data.get("author", ""),
            "genre": book_data.get("genre", "")
        },
        "library": {
            "libraryUid": library_uid,
            "name": library_data.get("name", ""),
            "address": library_data.get("address", ""),
            "city": library_data.get("city", "")
        },
        "rating": {"stars": stars}
    }

    return jsonify(response), 200
# -------------------- Возврат книги --------------------
@app.route("/api/v1/reservations/<reservation_uid>/return", methods=["POST"])
def return_book(reservation_uid):
    user_name = request.headers.get("X-User-Name")
    data = request.get_json()
    returned_condition = data.get("condition")
    returned_date_str = data.get("date")
    returned_date = datetime.strptime(returned_date_str, "%Y-%m-%d").date()

    headers = {"X-User-Name": user_name}
    try:
        resp = requests.get(f"{RESERVATION_URL}/reservations/{reservation_uid}/return", headers=headers, timeout=2)
        if resp.status_code != 200:
            return jsonify({"message": "Reservation not found"}), resp.status_code
        reservation = resp.json()
    except requests.RequestException:
        return jsonify({"message": "Reservation Service unavailable"}), 503

    till_date = datetime.strptime(reservation["tillDate"], "%Y-%m-%d").date()
    status = "RETURNED"
    if returned_date > till_date:
        status = "EXPIRED"

    payload = {"condition": returned_condition, "date": returned_date_str}
    try:
        requests.post(f"{RESERVATION_URL}/reservations/{reservation_uid}/return", json=payload, headers=headers)
    except requests.RequestException:
        pass

    # обновляем рейтинг через очередь
    penalty = 10 if status == "EXPIRED" else 0
    total_increment = 1 + penalty

    try:
        resp = requests.get(f"{RATING_URL}/rating", headers={"X-User-Name": user_name}, timeout=2)
        resp.raise_for_status()
        current_stars = resp.json().get("stars", 1)
        requests.post(f"{RATING_URL}/rating", json={"username": user_name, "stars": current_stars + total_increment})
    except requests.RequestException:
        enqueue_rating_update(user_name, total_increment)

    return "", 204

# -------------------- Health --------------------
@app.route("/manage/health", methods=["GET"])
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
