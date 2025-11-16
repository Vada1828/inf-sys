#!/bin/sh

# Run migrations
flask db migrate -m "Auto migration"
flask db upgrade

# Start the Flask application
exec python app.py
