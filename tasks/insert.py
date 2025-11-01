from db import conn_ctx
from models import Employee


def run(args):
    emp = Employee.from_cli(args.full_name, args.birth_date, args.gender)
    with conn_ctx() as c:
        try:
            emp.save(c)
            print(f"Добавлена запись с id={emp.id}")
        except:
            print("Создайте сначала таблицу")
