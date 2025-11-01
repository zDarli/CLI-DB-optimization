import sqlite3
from contextlib import contextmanager
from pathlib import Path

# Путь к файлу базы данных
DB_PATH = Path("app.db")


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


@contextmanager
def conn_ctx():
    conn = get_conn()
    try:
        yield conn            # возвращаем соединение в блок with
        conn.commit()         # если ошибок не было — сохраняем изменения
    except Exception as e:
        conn.rollback()       # при ошибке отменяем транзакцию
        raise e
    finally:
        conn.close()          # в любом случае закрываем соединение
