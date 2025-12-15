from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import CheckConstraint
import os

app = Flask(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://program:test@postgres:5432/ratings"
)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
db = SQLAlchemy(app)

class Rating(db.Model):
    __tablename__ = 'rating'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    stars = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        CheckConstraint('stars BETWEEN 0 AND 100', name='stars_range_check'),
    )
    def __repr__(self):
        return f"<Rating username={self.username} stars={self.stars}>"

with app.app_context():
    db.create_all()

@app.route("/rating", methods=["GET"])
def get_rating():
    username = request.headers.get("X-User-Name")
    if not username:
        return jsonify({"message": "X-User-Name header is required"}), 400

    rating = Rating.query.filter_by(username=username).first()
    if rating:
        return jsonify({"stars": rating.stars})
    else:
        # Если пользователя нет, создаем с 1 звездой по умолчанию
        new_rating = Rating(username=username, stars=1)
        db.session.add(new_rating)
        db.session.commit()
        return jsonify({"stars": new_rating.stars})

@app.route("/rating", methods=["POST"])
def update_rating():
    data = request.get_json()
    username = data.get("username")
    stars = data.get("stars")
    if not username or stars is None:
        return jsonify({"message": "username and stars required"}), 400

    if not (0 <= stars <= 100):
        return jsonify({"message": "stars must be between 0 and 100"}), 400

    rating = Rating.query.filter_by(username=username).first()
    if rating:
        rating.stars = stars
    else:
        rating = Rating(username=username, stars=stars)
        db.session.add(rating)

    db.session.commit()
    return jsonify(rating.to_dict()), 200

@app.route('/manage/health', methods=['GET'])
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)