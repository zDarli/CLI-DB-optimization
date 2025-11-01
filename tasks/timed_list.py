from db import conn_ctx
import time
from statistics import mean

SQL_old = """SELECT full_name, birth_date, gender
FROM employees
WHERE gender = 'Male' AND full_name like '% F%'"""


def run(args):
    N = 10 # кол-во повторов
    times = []


    with conn_ctx() as c:
        for i in range(N):
            start = time.perf_counter()
            rows = c.execute(SQL_old).fetchall()
            duration = time.perf_counter()-start
            times.append(duration)

            print(f"Прогон {i+1}: {duration:.4f} сек, найдено {len(rows)} записей")
    if not rows:
        print("Нет данных.")
        return
    avg_time = mean(times)
    print("\n=== Итог ===")
    print(f"Среднее время выполнения за {N} прогонов: {avg_time:.4f} сек")
    # print("Вывод: ФИО,ДР,Пол")
    # for full_name, birth_date, gender in rows:
    #     print(f"{full_name}\t{birth_date}\t{gender}")
    # print(f"\nВремя выполнения запроса: {duration:.4f} секунд")