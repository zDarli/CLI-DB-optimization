from db import conn_ctx
from faker import Faker
import random
from pytils.translit import translify

from models import Employee

fake = Faker("ru_RU")
genders = ["Male", "Female"]

SQL_INDEX = """
            CREATE INDEX idx_emp_gender_last
            ON employees (gender, last_name COLLATE NOCASE);
            """

def gen_employees(n:int, starts_with = None, pref_gen = None):
    employees = 0
    while employees<n:
        if pref_gen:
            gender = pref_gen
        else:
            gender = random.choice(genders)
        if gender == "Male":
            full_name = fake.parse("{{ first_name_male }}  {{ last_name_male }}")
        else:
            full_name = fake.parse("{{ first_name_female }}  {{ last_name_female }}")
        full_name = translify(full_name)
        birth = fake.date_of_birth(minimum_age=18,maximum_age=66)
        #опциональная фильтрация по 1 букве фамилии
        if starts_with:
            last_name = full_name.split()[-1]
            if not last_name.lower().startswith(starts_with.lower()):
                continue

        employees += 1
        yield Employee(full_name, birth, gender)

def run(args):
    with conn_ctx() as c:
        try:
            metrics = Employee.bulk_insert(c,gen_employees(1_000_000),chunk_size=50_000)
            print(metrics)
            Employee.bulk_insert(c,gen_employees(100, "F", "Male"))
            c.execute(SQL_INDEX)#добавление индекса
            c.commit()
        except:
            print("Создайте сначала таблицу")
