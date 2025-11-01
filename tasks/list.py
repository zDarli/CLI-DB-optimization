from db import conn_ctx

SQL = """SELECT full_name, birth_date, gender, (strftime('%Y','now') - strftime('%Y', birth_date))
         - (strftime('%m-%d','now') < strftime('%m-%d', birth_date)) AS age 
FROM employees
GROUP BY full_name, birth_date
ORDER BY full_name"""

def run(args):
    with conn_ctx() as c:
        rows = c.execute(SQL).fetchall()
    if not rows:
        print("Нет данных.")
        return
    print("Вывод: ФИО,ДР,Пол,Полных лет")
    for full_name, birth_date, gender, age in rows:
        print(f"{full_name}\t{birth_date}\t{gender}\t{age}")