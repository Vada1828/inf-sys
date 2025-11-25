from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
import requests
from sqlalchemy import text

app = Flask(__name__)
CORS(app)

app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://vada:vada@wh_db:5432/warehouse"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)

# --------------------------
# WH MODELS
# --------------------------
class DimProduct(db.Model):
    __tablename__ = "dim_product"
    product_key = db.Column(db.Integer, primary_key=True, autoincrement=True)
    product_id = db.Column(db.Integer)
    product_name = db.Column(db.String(100))
    load_id = db.Column(db.Integer)

class DimCustomer(db.Model):
    __tablename__ = "dim_customer"
    customer_key = db.Column(db.Integer, primary_key=True, autoincrement=True)
    customer_id = db.Column(db.Integer)
    customer_name = db.Column(db.String(100))
    load_id = db.Column(db.Integer)

class DimManager(db.Model):
    __tablename__ = "dim_manager"
    manager_key = db.Column(db.Integer, primary_key=True, autoincrement=True)
    manager_id = db.Column(db.Integer)
    manager_name = db.Column(db.String(100))
    load_id = db.Column(db.Integer)

class FactSales(db.Model):
    __tablename__ = "fact_sales"
    sale_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    product_key = db.Column(db.Integer)
    customer_key = db.Column(db.Integer)
    manager_key = db.Column(db.Integer)
    quantity = db.Column(db.Integer)
    total_price = db.Column(db.Float)
    load_id = db.Column(db.Integer)

# --------------------------
# ROUTES
# --------------------------
@app.route("/wh/tables")
def wh_tables():
    return jsonify(["dim_product","dim_customer","dim_manager","fact_sales"])


@app.route("/wh/rows/<t>")
def wh_rows(t):
    valid = {
        "dim_product": "dim_product",
        "dim_customer": "dim_customer",
        "dim_manager": "dim_manager",
        "fact_sales": "fact_sales"
    }

    if t not in valid:
        return jsonify({"error": "invalid table"}), 400

    table = valid[t]

    # ВАЖНО: сначала держим result, потом .keys()
    result = db.session.execute(text(f'SELECT * FROM "{table}"'))
    keys = result.keys()
    rows = result.fetchall()

    return jsonify([dict(zip(keys, r)) for r in rows])




@app.route("/wh/reset")
def wh_reset():
    db.drop_all()
    db.create_all()
    return jsonify({"message": "warehouse recreated"})


# --------------------------
# ETL LOAD (получает данные из app.py)
# --------------------------
@app.route("/etl/load")
def etl_load():
    try:
        extract = requests.get("http://localhost:5000/etl/extract").json()["data"]

        # получить новый load_id
        load_id = db.session.execute(text("SELECT COALESCE(MAX(load_id),0)+1 FROM fact_sales")).scalar()

        # КЭШ для поиска существующих записей
        existing_products = {
            name: key for (key, name) in 
            db.session.execute(text("SELECT product_key, product_name FROM dim_product")).all()
        }
        existing_customers = {
            name: key for (key, name) in 
            db.session.execute(text("SELECT customer_key, customer_name FROM dim_customer")).all()
        }
        existing_managers = {
            name: key for (key, name) in 
            db.session.execute(text("SELECT manager_key, manager_name FROM dim_manager")).all()
        }

        # === вставляем новые записи в DIM ===
        for r in extract:
            # PRODUCT
            if r["product_name"] not in existing_products:
                new_p = DimProduct(product_name=r["product_name"], load_id=load_id)
                db.session.add(new_p)
                db.session.flush()
                existing_products[r["product_name"]] = new_p.product_key

            # CUSTOMER
            if r["customer_name"] not in existing_customers:
                new_c = DimCustomer(customer_name=r["customer_name"], load_id=load_id)
                db.session.add(new_c)
                db.session.flush()
                existing_customers[r["customer_name"]] = new_c.customer_key

            # MANAGER
            if r["manager_name"] not in existing_managers:
                new_m = DimManager(manager_name=r["manager_name"], load_id=load_id)
                db.session.add(new_m)
                db.session.flush()
                existing_managers[r["manager_name"]] = new_m.manager_key

        db.session.commit()

        # === FACT SALES ===
        for r in extract:
            db.session.add(FactSales(
                product_key=existing_products[r["product_name"]],
                customer_key=existing_customers[r["customer_name"]],
                manager_key=existing_managers[r["manager_name"]],
                quantity=r["quantity"],
                total_price=r["total"],
                load_id=load_id
            ))

        db.session.commit()

        return jsonify({"message": "warehouse loaded", "load_id": load_id})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --------------------------
# SQL CONSOLE
# --------------------------
@app.route("/wh/sql", methods=["POST"])
def wh_sql():
    query_text = request.json.get("query")
    app.logger.info(f"SQL query: {query_text}")

    try:
        result = db.session.execute(text(query_text))
        rows = result.fetchall()
        keys = result.keys()
        return jsonify([dict(zip(keys, r)) for r in rows])
    except Exception as e:
        app.logger.info(f"SQL error: {e}")
        return jsonify({"error": str(e)})




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
