from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta, timezone
import os
import uuid
from zoneinfo import ZoneInfo


app = Flask(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://program:test@postgres:5432/reservations"
)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
db = SQLAlchemy(app)


class Reservation(db.Model):
    __tablename__ = 'reservations'

    id = db.Column(db.Integer, primary_key=True)
    reservation_uid = db.Column(db.String(36), default=lambda: str(uuid.uuid4()), unique=True)
    username = db.Column(db.String(80), nullable=False)
    book_uid = db.Column(db.String(36), nullable=False)
    library_uid = db.Column(db.String(36), nullable=False)
    status = db.Column(db.String(20), default='RENTED') # 'RENTED', 'RETURNED', 'EXPIRED'
    start_date = db.Column(db.Date, default=lambda: datetime.now(ZoneInfo("UTC")).date())
    till_date = db.Column(db.Date, nullable=False)

    def to_dict(self):
        return {
            "reservationUid": self.reservation_uid,
            "username": self.username,
            "bookUid": self.book_uid,
            "libraryUid": self.library_uid,
            "status": self.status,
            "startDate": self.start_date.isoformat(),
            "tillDate": self.till_date.isoformat()
        }

with app.app_context():
    db.create_all()

    # if not Reservation.query.first():
    #     sample = Reservation(
    #         reservation_uid = "e82cc1c9-0d5e-46c4-ae94-d815677a5673",
    #         username="Test Max",
    #         book_uid="f7cdc58f-2caf-4b15-9727-f89dcc629b27",
    #         library_uid="83575e12-7ce0-48ee-9931-51919ff3c9ee",
    #         start_date = datetime(2025, 12, 10),
    #         till_date=datetime(2025, 12, 31)
    #     )
    #     db.session.add(sample)
    #     db.session.commit()



@app.route('/reservations', methods=['GET'])
def get_all_reservations():
    reservations = Reservation.query.all()
    return jsonify([r.to_dict() for r in reservations]), 200

@app.route('/reservations/<username>/count', methods=['GET'])
def get_user_rented_count(username):
    count = Reservation.query.filter_by(username=username, status='RENTED').count()
    return jsonify({"rentedCount": count}), 200

@app.route('/reservations/<username>', methods=['GET'])
def get_user_reservations(username):
    reservations = Reservation.query.filter_by(username=username).all()
    return jsonify([r.to_dict() for r in reservations]), 200


@app.route('/reservations', methods=['POST'])
def create_reservation():
    data = request.get_json()
    book_uid = data.get('bookUid')
    library_uid = data.get('libraryUid')
    till_date = data.get('tillDate')

    content_type = request.headers.get("Content-Type")
    user_name = request.headers.get("X-User-Name")

    if not all([ book_uid, library_uid, till_date]):
        return jsonify({"error": "Missing required fields"}), 400

    reservation = Reservation(
        username = user_name,
        book_uid=book_uid,
        library_uid=library_uid,
        till_date=datetime.fromisoformat(till_date)
    )
    db.session.add(reservation)
    db.session.commit()

    return jsonify(reservation.to_dict()), 200


@app.route('/reservations/<reservation_uid>/return', methods=['POST', 'GET'])
def return_book(reservation_uid):
    reservation = Reservation.query.filter_by(reservation_uid=reservation_uid).first()
    if not reservation:
        user_name = request.headers.get("X-User-Name")
        reservation = Reservation.query.filter_by(username=user_name).first()
    if not reservation:
        return jsonify({"error": "Reservation not found"}), 404
    if request.method == 'GET':
        return jsonify(reservation.to_dict()), 200
    else:
        reservation.status = 'RETURNED'
        db.session.commit()
        return jsonify({"message": "OK"}), 204




@app.route('/manage/health', methods=['GET'])
def health():
    return "OK", 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8070)
