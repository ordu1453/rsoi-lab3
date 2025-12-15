from flask import Flask, jsonify, request
import requests
from datetime import datetime

app = Flask(__name__)

LIBRARY_URL = "http://library_service:8060"
RATING_URL = "http://rating_service:8050"
RESERVATION_URL = "http://reservation_service:8070"

# -------------------- Получение библиотек --------------------
@app.route("/api/v1/libraries", methods=["GET"])
def get_libraries():
    city = request.args.get("city", "Москва")
    page = request.args.get("page", 1)
    size = request.args.get("size", 1)

    params = {"city": city, "page": page, "size": size}
    resp = requests.get(f"{LIBRARY_URL}/libraries", params=params)
    return jsonify(resp.json()), resp.status_code

# -------------------- Получение книг --------------------
@app.route("/api/v1/libraries/<library_uid>/books", methods=["GET"])
def get_books(library_uid):
    page = request.args.get("page", 1)
    size = request.args.get("size", 1)
    show_all = request.args.get("showAll", "false").lower() == "true"

    params = {"page": page, "size": size, "showAll": show_all}
    resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}/books", params=params)
    return jsonify(resp.json()), resp.status_code

# -------------------- Получение рейтинга --------------------
@app.route("/api/v1/rating", methods=["GET"])
def get_rating():
    user_name = request.headers.get("X-User-Name")
    headers = {"X-User-Name": user_name}
    resp = requests.get(f"{RATING_URL}/rating", headers=headers)
    return jsonify(resp.json()), resp.status_code

@app.route("/api/v1/reservations", methods=["GET"])
def get_reservations():
    user_name = request.headers.get("X-User-Name")
    if not user_name:
        return jsonify({"error": "X-User-Name header is missing"}), 400

    # Получаем все бронирования пользователя
    reservations_resp = requests.get(f"{RESERVATION_URL}/reservations/{user_name}")
    if reservations_resp.status_code != 200:
        return jsonify({"error": "Failed to fetch reservations"}), 500

    reservations_json = reservations_resp.json()
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
            book_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}/{book_uid}")
            if book_resp.status_code == 200:
                book_data = book_resp.json()

        # Получаем информацию о библиотеке
        library_data = {}
        if library_uid:
            library_resp = requests.get(f"{LIBRARY_URL}/libraries/{library_uid}")
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

    return jsonify(result), 200

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

    rating_resp = requests.get(f"{RATING_URL}/rating", headers={"X-User-Name": user_name})
    stars = rating_resp.json().get("stars", 1) if rating_resp.status_code == 200 else 1

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

    # Уменьшаем доступные книги
    requests.patch(f"{LIBRARY_URL}/libraries/{library_uid}/books/{book_uid}/decrement")

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
    resp = requests.get(f"{RESERVATION_URL}/reservations/{reservation_uid}/return", headers=headers)

    if resp.status_code == 404:
        return jsonify({"message": "Reservation not found"}), 404
    elif resp.status_code != 200:
        return jsonify({"message": "Failed to fetch reservation"}), resp.status_code

    reservation = resp.json()


    till_date = datetime.strptime(reservation["tillDate"], "%Y-%m-%d").date()
    # original_condition = reservation["bookUid"].get("condition", "EXCELLENT")

    # Определяем новый статус
    status = "RETURNED"
    if returned_date > till_date:
        status = "EXPIRED"

    # Обновляем Reservation Service
    payload = {"condition": returned_condition, "date": returned_date_str}
    requests.post(f"{RESERVATION_URL}/reservations/{reservation_uid}/return", json=payload, headers=headers)

    # # Обновляем Library Service
    # book_uid = reservation["book"]["bookUid"]
    # library_uid = reservation["library"]["libraryUid"]
    # requests.patch(f"{LIBRARY_URL}/libraries/{library_uid}/books/{book_uid}/increase")

    # Обновляем рейтинг
    penalty = 0
    if status == "EXPIRED":
        penalty += 10
    # if returned_condition != original_condition:
    #     penalty += 10


    headersss = {"X-User-Name":user_name}
    rating_req = requests.get(f"{RATING_URL}/rating", headers=headersss)
    stars = rating_req.json()
    stars_count = stars.get("stars")

    if penalty > 0:
        requests.post(f"{RATING_URL}/rating", json={"username":user_name,"stars": stars_count+1})
    else:
        requests.post(f"{RATING_URL}/rating", json={"username":user_name,"stars":  stars_count+1})

    return "", 204

# -------------------- Health --------------------
@app.route("/manage/health", methods=["GET"])
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
