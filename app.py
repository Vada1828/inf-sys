

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import inspect
from flask import jsonify
from flask import send_file
from sqlalchemy_schemadisplay import create_schema_graph
from io import BytesIO
import tempfile
import os
from faker import Faker
import random
from flask import request
from sqlalchemy import text

def seed_database():
    fake = Faker()

    # Seed managers
    for _ in range(5):
        m = Manager(first_name=fake.first_name(), second_name=fake.last_name())
        db.session.add(m)

    # Seed customers
    for _ in range(20):
        c = Customer(first_name=fake.first_name(), second_name=fake.last_name(), phone_number=fake.phone_number())
        db.session.add(c)

    # Seed products
    for _ in range(20):
        p = Product(product_name=fake.word(), price=round(random.uniform(5.0, 200.0), 2))
        db.session.add(p)

    db.session.commit()

    managers = Manager.query.all()
    customers = Customer.query.all()
    products = Product.query.all()

    # Seed orders and details
    for _ in range(500):
        order = Order(
            status=random.choice(['pending', 'completed', 'cancelled']),
            order_price=0,
            customer=random.choice(customers),
            manager=random.choice(managers)
        )
        db.session.add(order)
        db.session.flush()

        quantity = random.randint(1, 5)
        product = random.choice(products)
        od = OrderDetail(
            order=order,
            product=product,
            quantity=quantity,
            phone_number=order.customer.phone_number
        )
        order.order_price += product.price * quantity
        db.session.add(od)

    db.session.commit()


app = Flask(__name__)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://vada:vada@db:5432/vada'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)

class Customer(db.Model):
    __tablename__ = 'customers'

    customer_id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(50))
    second_name = db.Column(db.String(50))
    phone_number = db.Column(db.String(50))
    orders = db.relationship('Order', backref='customer', lazy=True)

class Manager(db.Model):
    __tablename__ = 'managers'

    manager_id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(50))
    second_name = db.Column(db.String(50))
    orders = db.relationship('Order', backref='manager', lazy=True)

class Product(db.Model):
    __tablename__ = 'products'

    product_id = db.Column(db.Integer, primary_key=True)
    product_name = db.Column(db.String(100))
    price = db.Column(db.Float)
    order_details = db.relationship('OrderDetail', backref='product', lazy=True)

class Order(db.Model):
    __tablename__ = 'orders'

    order_id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(50))
    order_price = db.Column(db.Float)
    customer_id = db.Column(db.Integer, db.ForeignKey('customers.customer_id'))
    manager_id = db.Column(db.Integer, db.ForeignKey('managers.manager_id'))
    order_details = db.relationship('OrderDetail', backref='order', lazy=True)

class OrderDetail(db.Model):
    __tablename__ = 'order_details'

    order_detail_id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.order_id'))
    product_id = db.Column(db.Integer, db.ForeignKey('products.product_id'))
    quantity = db.Column(db.Integer)
    phone_number = db.Column(db.String(50))

@app.route('/tables')
def list_tables():
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    return jsonify(tables)

@app.route('/db-diagram')
def db_diagram():
    try:
        graph = create_schema_graph(
            metadata=db.metadata,
            engine=db.engine,
            show_datatypes=False,
            show_indexes=False,
            rankdir='LR',
            concentrate=False
        )
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            graph.write_png(tmp.name)
            tmp_path = tmp.name

        return send_file(tmp_path, mimetype='image/png', download_name='db_schema.png')

    except Exception as e:
        return f"Error generating diagram: {str(e)}", 500

    finally:
        # Clean up the temporary file after request is done
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

@app.route('/seed-db')
def seed_db_route():
    try:
        seed_database()
        return jsonify({"message": "Database seeded successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/customers')
def get_customers():
    customers = Customer.query.all()
    return jsonify([{
        "customer_id": c.customer_id,
        "first_name": c.first_name,
        "second_name": c.second_name,
        "phone_number": c.phone_number
    } for c in customers])

@app.route('/managers')
def get_managers():
    managers = Manager.query.all()
    return jsonify([{
        "manager_id": m.manager_id,
        "first_name": m.first_name,
        "second_name": m.second_name
    } for m in managers])

@app.route('/products')
def get_products():
    products = Product.query.all()
    return jsonify([{
        "product_id": p.product_id,
        "product_name": p.product_name,
        "price": p.price
    } for p in products])

@app.route('/orders')
def get_orders():
    orders = Order.query.all()
    return jsonify([{
        "order_id": o.order_id,
        "status": o.status,
        "order_price": o.order_price,
        "customer_id": o.customer_id,
        "manager_id": o.manager_id
    } for o in orders])

@app.route('/order-details')
def get_order_details():
    details = OrderDetail.query.all()
    return jsonify([{
        "order_detail_id": d.order_detail_id,
        "order_id": d.order_id,
        "product_id": d.product_id,
        "quantity": d.quantity,
        "phone_number": d.phone_number
    } for d in details])

@app.route('/execute-sql', methods=['POST'])
def execute_sql():
    data = request.get_json()
    sql_query = data.get('query')
    
    if not sql_query:
        return jsonify({"error": "No SQL query provided"}), 400

    try:
        # Wrap raw SQL query string as text() object
        stmt = text(sql_query)
        result_proxy = db.session.execute(stmt)
        result_set = result_proxy.fetchall()
        keys = result_proxy.keys()
        results = [dict(zip(keys, row)) for row in result_set]
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
if __name__ == "__main__":
    with app.app_context():
        if Customer.query.count() == 0:
            seed_database()
    app.run(host="0.0.0.0", port=5000)
