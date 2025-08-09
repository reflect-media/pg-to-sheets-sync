FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# הגדרת זיכרון Python מוגבל
ENV PYTHONHASHSEED=0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# הרצה עם הגדרות זיכרון מוגבלות
CMD ["python", "-O", "main.py"]
