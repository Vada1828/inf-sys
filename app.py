

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import inspect
from flask import jsonify

app = Flask(__name__)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://vada:vada@db:5432/vada'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), nullable=False)

@app.route('/hello')
def hello():
    return "Hello, World!"

@app.route('/tables')
def list_tables():
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    return jsonify(tables)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
