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
    product_new_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # product_id = db.Column(db.Integer)
    product_name = db.Column(db.String(100))
    load_id = db.Column(db.Integer)

class DimCustomer(db.Model):
    __tablename__ = "dim_customer"
    customer_new_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # customer_id = db.Column(db.Integer)
    customer_name = db.Column(db.String(100))
    load_id = db.Column(db.Integer)

class DimManager(db.Model):
    __tablename__ = "dim_manager"
    manager_new_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # manager_id = db.Column(db.Integer)
    manager_name = db.Column(db.String(100))
    load_id = db.Column(db.Integer)

class FactSales(db.Model):
    __tablename__ = "fact_sales"
    sale_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    product_new_id = db.Column(db.Integer)
    customer_new_id = db.Column(db.Integer)
    manager_new_id = db.Column(db.Integer)
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

        # получить новый номер загрузки
        load_id = db.session.execute(text(
            "SELECT COALESCE(MAX(load_id), 0) + 1 FROM fact_sales"
        )).scalar()

        # КЭШ существующих DIM строк: {name → new_id}
        existing_products = {
            name: new_id for (new_id, name) in
            db.session.execute(text("SELECT product_new_id, product_name FROM dim_product")).all()
        }
        existing_customers = {
            name: new_id for (new_id, name) in
            db.session.execute(text("SELECT customer_new_id, customer_name FROM dim_customer")).all()
        }
        existing_managers = {
            name: new_id for (new_id, name) in
            db.session.execute(text("SELECT manager_new_id, manager_name FROM dim_manager")).all()
        }

        # === DIM INSERT ===
        for r in extract:

            # PRODUCT
            pname = r["product_name"]
            if pname not in existing_products:
                new_p = DimProduct(
                    product_name=pname,
                    load_id=load_id
                )
                db.session.add(new_p)
                db.session.flush()
                existing_products[pname] = new_p.product_new_id

            # CUSTOMER
            cname = r["customer_name"]
            if cname not in existing_customers:
                new_c = DimCustomer(
                    customer_name=cname,
                    load_id=load_id
                )
                db.session.add(new_c)
                db.session.flush()
                existing_customers[cname] = new_c.customer_new_id

            # MANAGER
            mname = r["manager_name"]
            if mname not in existing_managers:
                new_m = DimManager(
                    manager_name=mname,
                    load_id=load_id
                )
                db.session.add(new_m)
                db.session.flush()
                existing_managers[mname] = new_m.manager_new_id

        db.session.commit()

        # === FACT INSERT ===
        for r in extract:
            db.session.add(FactSales(
                product_new_id=existing_products[r["product_name"]],
                customer_new_id=existing_customers[r["customer_name"]],
                manager_new_id=existing_managers[r["manager_name"]],
                quantity=r["quantity"],
                total_price=r["total"],
                load_id=load_id
            ))

        db.session.commit()

        # === CLEAN FACT DUPLICATES ===
        db.session.execute(text("""
            DELETE FROM fact_sales a
            USING fact_sales b
            WHERE a.sale_id > b.sale_id
            AND a.product_new_id = b.product_new_id
            AND a.customer_new_id = b.customer_new_id
            AND a.manager_new_id = b.manager_new_id
            AND a.quantity = b.quantity
            AND a.total_price = b.total_price;
        """))

        
        db.session.commit()

        return jsonify({
            "message": "warehouse loaded",
            "load_id": load_id
        })

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
