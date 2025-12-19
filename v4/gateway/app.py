import redis
import json
import requests
from flask import Flask, jsonify, request
from datetime import datetime
import threading
import time
from threading import Lock, Thread
from queue import Queue, Empty

rating_queue = Queue()



class CircuitBreaker:
    def __init__(self, failure_threshold=3, retry_timeout=10):
        self.failure_threshold = failure_threshold
        self.retry_timeout = retry_timeout
        self.failure_count = 0
        self.state = "CLOSED" 
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

def rating_queue_worker():
    while True:
        try:
            task = rating_queue.get(timeout=5)
        except Empty:
            continue

        user_name = task["user_name"]
        delta = task["delta"]

        try:
            # 1. Получаем ТЕКУЩИЙ рейтинг
            resp = requests.get(
                f"{RATING_URL}/rating",
                headers={"X-User-Name": user_name},
                timeout=2
            )
            resp.raise_for_status()
            current = resp.json().get("stars", 1)

            # 2. Применяем операцию
            new_stars = current + delta

            # 3. Обновляем рейтинг
            requests.post(
                f"{RATING_URL}/rating",
                json={"username": user_name, "stars": new_stars},
                timeout=2
            )

            rating_queue.task_done()

        except Exception:
            # сервис всё ещё недоступен
            rating_queue.put(task)
            rating_queue.task_done()
            time.sleep(3)


Thread(target=rating_queue_worker, daemon=True).start()


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
@app.route("/api/v1/reservations", methods=["POST"])
def create_reservation():
    user_name = request.headers.get("X-User-Name")
    data = request.get_json()
    book_uid = data.get("bookUid")
    library_uid = data.get("libraryUid")
    till_date = data.get("tillDate")

    # Проверка лимита по количеству книг
    rented_resp = requests.get(f"{RESERVATION_URL}/reservations/{user_name}/count")
    rented_count = rented_resp.json().get("rentedCount", 0) if rented_resp.status_code == 200 else 0

    # Получаем рейтинг пользователя через Circuit Breaker
    stars_resp = rating_cb.call(fetch_rating, user_name)
    if "message" in stars_resp:
        return jsonify(stars_resp), 503

    stars = stars_resp.get("stars", 1)

    if rented_count >= stars:
        return jsonify({"message": "Maximum number of rented books reached"}), 400

    # Получаем информацию о книге и библиотеке
    book_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}/{book_uid}")
    library_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}")
    book_data = book_resp.json() if book_resp.status_code == 200 else {}
    library_data = library_resp.json() if library_resp.status_code == 200 else {}

    # Создаём запись в Reservation Service
    payload = {"bookUid": book_uid, "libraryUid": library_uid, "tillDate": till_date}
    headers = {"X-User-Name": user_name, "Content-Type": "application/json"}
    res = requests.post(f"{RESERVATION_URL}/reservations", json=payload, headers=headers)
    reservation_json = res.json()

    try:
        # Если сервис доступен — обновляем сразу
        resp = requests.get(f"{RATING_URL}/rating", headers={"X-User-Name": user_name}, timeout=2)
        resp.raise_for_status()
        current_stars = resp.json().get("stars", 1)
        requests.post(f"{RATING_URL}/rating", json={"username": user_name, "stars": current_stars + 1})
    except requests.RequestException:
        # если сервис недоступен — добавляем в очередь
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


@app.route("/api/v1/reservations/<reservation_uid>/return", methods=["POST"])
def return_book(reservation_uid):
    user_name = request.headers.get("X-User-Name")
    data = request.get_json()
    returned_condition = data.get("condition")
    returned_date_str = data.get("date")
    returned_date = datetime.strptime(returned_date_str, "%Y-%m-%d").date()

    headers = {"X-User-Name": user_name}
    resp = requests.get(f"{RESERVATION_URL}/reservations/{reservation_uid}/return", headers=headers)

    if resp.status_code == 404:
        return jsonify({"message": "Reservation not found"}), 404
    elif resp.status_code != 200:
        return jsonify({"message": "Failed to fetch reservation"}), resp.status_code

    reservation = resp.json()
    till_date = datetime.strptime(reservation["tillDate"], "%Y-%m-%d").date()

    # Определяем новый статус
    status = "RETURNED"
    penalty = 0
    if returned_date > till_date:
        status = "EXPIRED"
        penalty += 1  # штраф за просрочку

    # Обновляем Reservation Service
    try:
        requests.post(f"{RESERVATION_URL}/reservations/{reservation_uid}/return",
                      json={"condition": returned_condition, "date": returned_date_str},
                      headers=headers)
    except:
        pass 
    
    # Обновляем рейтинг через Circuit Breaker
    try:
        stars_resp = rating_cb.call(fetch_rating, user_name)
        if "message" in stars_resp:
            # rating_service недоступен - пропускаем обновление рейтинга
            stars_count = None
        else:
            stars_count = stars_resp.get("stars", 1)
    except:
        stars_count = None

    if stars_count is not None:
        try:
            requests.post(...)
        except:
            rating_queue.put({"user_name": user_name, "delta": 1})
    else:
        rating_queue.put({"user_name": user_name, "delta": 1})

    return "", 204

# -------------------- Health --------------------
@app.route("/manage/health", methods=["GET"])
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)