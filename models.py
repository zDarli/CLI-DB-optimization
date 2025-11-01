from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Iterator, Sequence
import time
import sqlite3


#SQL для создания таблицы сотрудников
DDL = """
CREATE TABLE IF NOT EXISTS employees (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name   TEXT    NOT NULL,
    last_name   TEXT    NOT NULL,
    birth_date  TEXT    NOT NULL,    -- хранится в ISO-формате YYYY-MM-DD
    gender      TEXT    NOT NULL CHECK(gender IN ('Male', 'Female', 'Other'))
);
CREATE INDEX IF NOT EXISTS idx_employees_name ON employees(full_name);
"""


#Класс для работы с сотрудниками
@dataclass
class Employee:
    full_name: str
    birth_date: date
    gender: str
    id: int | None = None

    # альтернативный конструктор: из строки даты
    @classmethod
    def from_cli(cls, full_name: str, birth_date_str: str, gender: str) -> Employee:
        """Создаёт объект Employee из аргументов командной строки."""
        y, m, d = map(int, birth_date_str.split("-"))
        return cls(full_name=full_name.strip(), birth_date=date(y, m, d), gender=gender)

    # рассчитать возраст (полных лет)
    # def age_years(self, today: date | None = None) -> int:
    #     if today is None:
    #         today = date.today()
    #     years = today.year - self.birth_date.year
    #     # если день рождения ещё не наступил в этом году — отнимаем 1
    #     if (today.month, today.day) < (self.birth_date.month, self.birth_date.day):
    #         years -= 1
    #     return years

    @staticmethod
    def last_name_from_full(full_name: str) -> str:
        parts = full_name.strip().split()
        return parts[-1] if parts else ""

    # подготовить данные к записи в БД
    def to_row(self) -> tuple:
        """Возвращает кортеж значений в нужном порядке для INSERT-запроса."""
        last_name = self.last_name_from_full(self.full_name)
        return (self.full_name, last_name, self.birth_date.isoformat(), self.gender)

    # SQL для вставки одной записи
    @staticmethod
    def insert_sql() -> str:
        return "INSERT INTO employees(full_name, last_name, birth_date, gender) VALUES (?, ?, ?, ?)"

    # сохранить объект в БД
    def save(self, conn) -> Employee:
        """Отправляет объект в БД, обновляет его id."""
        cur = conn.execute(self.insert_sql(), self.to_row())
        self.id = cur.lastrowid
        return self

    # для массовой вставки (executemany)
    @classmethod
    def executemany_payload(cls, employees: list[Employee]) -> tuple[str, list[tuple]]:
        """Возвращает SQL и список кортежей для executemany."""
        sql = cls.insert_sql()
        rows = [e.to_row() for e in employees]
        return sql, rows

    @classmethod
    def bulk_insert(
            cls,
            conn: sqlite3.Connection,
            employees: Iterable["Employee"],
            *,
            chunk_size: int = 10_000,
            optimize_pragmas: bool = True,
    ) -> dict:
        """
        Пакетно вставляет сотрудников. Принимает и список, и поток (итератор).
        Возвращает метрики: вставлено строк и секундах времени.
        """
        t0 = time.perf_counter()
        cur = conn.cursor()

        # Небольшие ускорители на время загрузки (безопасные для WAL)
        # Если у тебя уже WAL — ок. Если нет — SQLite переключит.
        # Возврат в прошлое состояние при необходимости можно добавить.
        if optimize_pragmas:
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")  # быстрее, чем FULL
            cur.execute("PRAGMA temp_store=MEMORY;")

        sql = cls.insert_sql()

        inserted = 0
        buf: list[tuple] = []

        try:
            cur.execute("BEGIN;")

            for e in employees:
                buf.append(e.to_row())
                if len(buf) >= chunk_size:
                    cur.executemany(sql, buf)
                    inserted += len(buf)
                    buf.clear()
                    print(f"Inserted {chunk_size} rows")

            if buf:
                cur.executemany(sql, buf)
                inserted += len(buf)

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        dt = time.perf_counter() - t0
        return {"rows": inserted, "seconds": dt}

