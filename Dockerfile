FROM python:3.10-slim


RUN apt-get update && apt-get install -y graphviz

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# Entrypoint script to run migrations then start app
RUN echo '#!/bin/sh\nflask db migrate -m "Auto migration"\nflask db upgrade\nexec python app.py' > /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
