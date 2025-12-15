from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship
from uuid import uuid4
import os

app = Flask(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://program:test@postgres:5432/libraries"
)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
db = SQLAlchemy(app)


class Library(db.Model):
    __tablename__ = 'library'
    id = db.Column(db.Integer, primary_key=True)
    library_uid = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid4()))
    name = db.Column(db.String(80), nullable=False)
    city = db.Column(db.String(255), nullable=False)
    address = db.Column(db.String(255), nullable=False)
    books = relationship('LibraryBook', back_populates='library')


class Book(db.Model):
    __tablename__ = 'books'
    id = db.Column(db.Integer, primary_key=True)
    book_uid = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid4()))
    name = db.Column(db.String(255), nullable=False)
    author = db.Column(db.String(255))
    genre = db.Column(db.String(255))
    condition = db.Column(db.String(20), default='EXCELLENT')  # EXCELLENT, GOOD, BAD
    libraries = relationship('LibraryBook', back_populates='book')


class LibraryBook(db.Model):
    __tablename__ = 'library_books'
    book_id = db.Column(db.Integer, db.ForeignKey('books.id'), primary_key=True)
    library_id = db.Column(db.Integer, db.ForeignKey('library.id'), primary_key=True)
    available_count = db.Column(db.Integer, nullable=False)
    book = relationship('Book', back_populates='libraries')
    library = relationship('Library', back_populates='books')


@app.before_request
def create_tables():
    db.create_all()



    if not Library.query.first():
        library = Library(
            id=1,
            library_uid="83575e12-7ce0-48ee-9931-51919ff3c9ee",
            name="Библиотека имени 7 Непьющих",
            city="Москва",
            address="2-я Бауманская ул., д.5, стр.1"
        )
        db.session.add(library)

        book = Book(
            id=1,
            book_uid="f7cdc58f-2caf-4b15-9727-f89dcc629b27",
            name="Краткий курс C++ в 7 томах",
            author="Бьерн Страуструп",
            genre="Научная фантастика",
            condition="EXCELLENT"
        )
        db.session.add(book)

        lib_book = LibraryBook(
            book_id=book.id,
            library_id=library.id,
            available_count=1
        )
        db.session.add(lib_book)

        db.session.commit()
        print("Test data created")

@app.route('/libraries/<library_uid>', methods=['GET'])
def get_library(library_uid):
    library = Library.query.filter_by(library_uid=library_uid).first()
    if not library:
        return jsonify({"message": "Library not found"}), 404
    items = {
            "libraryUid": library.library_uid,
            "name": library.name,
            "address": library.address,
            "city": library.city
        }
    return jsonify(items)
    
@app.route('/libraries/<library_uid>/books/<book_uid>/decrement', methods=['PATCH'])
def decrement_book_count(library_uid, book_uid):
    # Находим библиотеку
    library = Library.query.filter_by(library_uid=library_uid).first()
    if not library:
        return jsonify({"message": "Library not found"}), 404

    # Находим связь книги с библиотекой
    lib_book = LibraryBook.query.join(Book).filter(
        LibraryBook.library_id == library.id,
        Book.book_uid == book_uid
    ).first()

    if not lib_book:
        return jsonify({"message": "Book not found in library"}), 404

    # Уменьшаем доступное количество, не ниже 0
    if lib_book.available_count > 0:
        lib_book.available_count -= 1
        db.session.commit()

    return jsonify({"availableCount": lib_book.available_count}), 200

@app.route('/libraries', methods=['GET'])
def get_libraries():
    def safe_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    city = request.args.get('city')
    page = safe_int(request.args.get('page'), 1)
    size = safe_int(request.args.get('size'), 1)

    if not city:
        return jsonify({"message": "City parameter is required"}), 400

    query = Library.query.filter_by(city=city)
    total = query.count()
    libraries = query.all()
    items = [
        {
            "libraryUid": lib.library_uid,
            "name": lib.name,
            "address": lib.address,
            "city": lib.city
        } for lib in libraries
    ]
    return jsonify({
        "page": page,
        "pageSize": size,
        "totalElements": total,
        "items": items
    })

@app.route('/libraries/<library_uid>/<book_uid>', methods=['GET'])
def get_book_data(library_uid, book_uid):
    book = Book.query.filter_by(book_uid=book_uid).first()
    if not book:
        return jsonify({"message": "book not found"}), 404
    
    return jsonify({
        "name": book.name,
        "genre": book.genre,
        "condition": book.condition,
        "author": book.author
    })
    




@app.route('/libraries/<library_uid>/books', methods=['GET'])
def get_books(library_uid):
    DEFAULT_PAGE = 1
    DEFAULT_SIZE = 1

    def safe_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    show_all = request.args.get('showAll', 'false').lower() == 'true'
    page = safe_int(request.args.get('page'), DEFAULT_PAGE)
    size = safe_int(request.args.get('size'), DEFAULT_SIZE)


    library = Library.query.filter_by(library_uid=library_uid).first()
    if not library:
        return jsonify({"message": "Library not found"}), 404

    query = LibraryBook.query.filter_by(library_id=library.id)
    # if not show_all:
    #     query = query.filter(LibraryBook.available_count > 0)

    total = query.count()
    library_books = query.all()

    items = [
        {
            "bookUid": lb.book.book_uid,
            "name": lb.book.name,
            "author": lb.book.author,
            "genre": lb.book.genre,
            "condition": lb.book.condition,
            "availableCount": lb.available_count
        } for lb in library_books
    ]

    return jsonify({
        "page": page,
        "pageSize": size,
        "totalElements": total,
        "items": items
    })


@app.route('/manage/health', methods=['GET'])
def health():
    return "OK", 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8060, debug=True)
