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

    order_id = db.Column(db.Integer)
    customer_new_id = db.Column(db.Integer)
    manager_new_id = db.Column(db.Integer)

    quantity = db.Column(db.Integer)      # aggregated
    total_price = db.Column(db.Float)     # aggregated
    status = db.Column(db.String(50))

    load_id = db.Column(db.Integer)


# --------------------------
# ROUTES
# --------------------------
@app.route("/wh/tables")
def wh_tables():
    return jsonify(["dim_customer","dim_manager","fact_sales"])


@app.route("/wh/rows/<t>")
def wh_rows(t):
    valid = {
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

        # === FACT INSERT WITH ORDER-LEVEL AGGREGATION ===
        for r in extract:

            order_id = r["order_id"]
            new_status = r["status"]

            # ищем последний факт по этому order_id
            old = db.session.execute(text("""
                SELECT sale_id, status, load_id
                FROM fact_sales
                WHERE order_id = :oid
                ORDER BY load_id DESC
                LIMIT 1
            """), {"oid": order_id}).fetchone()

            # если записи еще нет — просто добавляем первую версию
            if old is None:
                db.session.add(FactSales(
                    order_id=order_id,
                    customer_new_id=existing_customers[r["customer_name"]],
                    manager_new_id=existing_managers[r["manager_name"]],
                    quantity=r["quantity"],
                    total_price=r["total"],
                    status=new_status,
                    load_id=load_id
                ))
                continue

            old_status = old.status

            # если статус не изменился — НОВОЙ версии не нужно
            if old_status == new_status:
                continue

            # если статус изменился — создаем новую версию (новый load)
            db.session.add(FactSales(
                order_id=order_id,
                customer_new_id=existing_customers[r["customer_name"]],
                manager_new_id=existing_managers[r["manager_name"]],
                quantity=r["quantity"],
                total_price=r["total"],
                status=new_status,
                load_id=load_id
            ))


        # db.session.commit()

        # # === CLEAN FACT DUPLICATES (leave latest version per order) ===
        # db.session.execute(text("""
        #     DELETE FROM fact_sales a
        #     USING fact_sales b
        #     WHERE a.sale_id < b.sale_id
        #     AND a.order_id = b.order_id;
        # """))


        
        db.session.commit()

        return jsonify({
            "message": "warehouse loaded",
            "load_id": load_id
        })

    except Exception as e:
        app.logger.info(f"HUI: {str(e)}")
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
