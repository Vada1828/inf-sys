from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from faker import Faker
import random
from flask_cors import CORS
from sqlalchemy import text

# --------------------------
# APP CONFIG
# --------------------------
app = Flask(__name__)
CORS(app)

app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://vada:vada@tr_db:5432/transactional"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)

BaseTR = declarative_base()

# --------------------------
# TR MODELS
# --------------------------
class Customer(db.Model):
    __tablename__ = 'customers'
    customer_id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(50))
    second_name = db.Column(db.String(50))
    phone_number = db.Column(db.String(50))

class Manager(db.Model):
    __tablename__ = 'managers'
    manager_id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(50))
    second_name = db.Column(db.String(50))

class Product(db.Model):
    __tablename__ = 'products'
    product_id = db.Column(db.Integer, primary_key=True)
    product_name = db.Column(db.String(100))
    price = db.Column(db.Float)

class Order(db.Model):
    __tablename__ = 'orders'
    order_id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(50))
    order_price = db.Column(db.Float)
    customer_id = db.Column(db.Integer)
    manager_id = db.Column(db.Integer)

class OrderDetail(db.Model):
    __tablename__ = 'order_details'
    order_detail_id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer)
    product_id = db.Column(db.Integer)
    quantity = db.Column(db.Integer)
    phone_number = db.Column(db.String(50))


# --------------------------
# SEED
# --------------------------
def seed_tr():
    fake = Faker()

    # === create managers ===
    managers = []
    for _ in range(5):
        m = Manager(
            first_name=fake.first_name(),
            second_name=fake.last_name()
        )
        db.session.add(m)
        managers.append(m)

    # === create customers ===
    customers = []
    for _ in range(20):
        c = Customer(
            first_name=fake.first_name(),
            second_name=fake.last_name(),
            phone_number=fake.phone_number()
        )
        db.session.add(c)
        customers.append(c)

    # === create products ===
    products = []
    for _ in range(20):
        p = Product(
            product_name=fake.word(),
            price=round(random.uniform(5, 200), 2)
        )
        db.session.add(p)
        products.append(p)

    db.session.commit()   # commit so IDs exist

    # === create orders + order_details ===
    for _ in range(50):
        cust = random.choice(customers)
        mgr = random.choice(managers)

        order = Order(
            status=random.choice(["pending", "completed", "cancelled"]),
            order_price=0,
            customer_id=cust.customer_id,
            manager_id=mgr.manager_id
        )
        db.session.add(order)
        db.session.flush()  # get order_id

        total_price = 0

        for _ in range(random.randint(1, 5)):
            product = random.choice(products)
            qty = random.randint(1, 5)

            # total_price = price * quantity
            total_price += product.price * qty

            detail = OrderDetail(
                order_id=order.order_id,
                product_id=product.product_id,
                quantity=qty,
                phone_number=cust.phone_number  # ТЫ САМ ЗАДАЛ ЭТО ПОЛЕ, Я СТАВЛЮ CUSTOMER PHONE
            )
            db.session.add(detail)

        order.order_price = total_price

    db.session.commit()


# --------------------------
# ROUTES
# --------------------------
@app.route("/tr/tables")
def tr_tables():
    return jsonify(["customers", "managers", "products", "orders", "order_details"])


@app.route("/tr/rows/<table>")
def tr_rows(table):
    model = {
        "customers": Customer,
        "managers": Manager,
        "products": Product,
        "orders": Order,
        "order_details": OrderDetail
    }.get(table)

    if not model:
        return jsonify({"error": "unknown table"}), 400

    rows = model.query.all()
    return jsonify([{c.name: getattr(r, c.name) for c in r.__table__.columns} for r in rows])


@app.route("/tr/insert/<table>", methods=["POST"])
def tr_insert(table):
    data = request.json

    model = {
        "customers": Customer,
        "managers": Manager,
        "products": Product,
    }.get(table)

    if not model:
        return jsonify({"error": "insert allowed only for base tables"}), 400

    row = model(**data)
    db.session.add(row)
    db.session.commit()

    return jsonify({"message": "inserted"})


@app.route("/tr/reset")
def tr_reset():
    db.drop_all()
    db.create_all()
    seed_tr()
    return jsonify({"message": "TR recreated"})


# --------------------------
# ETL EXTRACT FOR app2.py
# --------------------------
@app.route("/etl/extract")
def etl_extract():
    result = db.session.execute(text("""
        SELECT 
            o.order_id,
            c.first_name || ' ' || c.second_name AS customer_name,
            m.first_name || ' ' || m.second_name AS manager_name,
            p.product_name,
            od.quantity,
            p.price,
            (od.quantity * p.price) AS total
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN managers m ON m.manager_id = o.manager_id
        JOIN order_details od ON od.order_id = o.order_id
        JOIN products p ON p.product_id = od.product_id
    """)).fetchall()

    keys = ["order_id","customer_name","manager_name","product_name","quantity","price","total"]
    data = [dict(zip(keys, row)) for row in result]

    return jsonify({"data": data})


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        if Customer.query.count() == 0:
            seed_tr()

    app.run(host="0.0.0.0", port=5000, debug=True)
