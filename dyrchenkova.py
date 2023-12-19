# Achievement #2- Линейная клиент-сервер архитектура
# Необходимо спроектировать и описать на языке UML классическую линейную 3-х уровневую архитектуру: [КЛИЕНТК]<->ВЕБ-СЕРВЕР<->СЕРВЕР ПРИЛОЖЕНИЙ<->БАЗА ДАННЫХ.
# Задача которую должна выполнять система:
# Обрабатывать НТТ POST запрос в котором передается натуральное число от 0 до N, в ответ на запрос отправлять число увеличенное на единицу.
# Обрабатывать исключительную ситуацию #1: если число уже поступало то выводить ошибку в ответ и лог.
# Обрабатывать исключительную ситуацию #2: если поступившее число на единицу меньше уже обработанного числа то выводить ошибку в ответ и лог.

# Высокая сложность - выполнить задание средней сложности. Разработать систему на одном из языков программирования PYTHON:

from fastapi import FastAPI, HTTPException
import psycopg2
from pydantic import BaseModel
import uvicorn


# Инициализация FastAPI приложения
app = FastAPI()

# Инициализация подключения к базе данных
DB_NAME = "chisla"
DB_USER = "dychenkova"
DB_PASS = "parol"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"

conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)

# Определение таблицы в базе данных
with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS notes (
        id SERIAL PRIMARY KEY,
        number INTEGER
    )
    """)

# Модель для входных данных
# Определение модели данных
class Number(BaseModel):
    number: int

with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS numbers (
            id SERIAL PRIMARY KEY,
            number INTEGER UNIQUE
        )
    """)
    conn.commit()

# Обработчик POST запроса
@app.post("/")
async def process_number(number: Number):
    # Проверка наличия числа в базе данных
    query = "SELECT number FROM numbers WHERE number = %s"
    with conn.cursor() as cur:
        cur.execute(query, (number.number,))
        result = cur.fetchone()
        if result:
            raise HTTPException(status_code=400, detail="Number already exists in database")

    # Проверка числа на единицу меньше уже обработанного числа
    query = "SELECT number FROM numbers WHERE number = %s"
    with conn.cursor() as cur:
        cur.execute(query, (number.number+1,))
        result = cur.fetchone()
        if result and number.number < result[0]:
            raise HTTPException(status_code=400, detail="Number is less than the last processed number")

    # Вставка числа в базу данных
    query = "INSERT INTO numbers (number) VALUES (%s)"
    with conn.cursor() as cur:
        cur.execute(query, (number.number,))
        conn.commit()

    # Возврат числа, увеличенного на единицу
    return {"number": number.number + 1}

# Запуск приложения
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
