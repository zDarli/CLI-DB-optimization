import argparse
from tasks.create import run as task_create
from tasks.insert import run as task_insert
from tasks.list import run as task_list
from tasks.fill import run as task_fill
from tasks.timed_list import run as task_timed_list


def main():
    parser = argparse.ArgumentParser(description="CLI: задачи 1–5")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("1", help="Создать таблицы")
    p1.set_defaults(func=task_create)

    p2 = sub.add_parser("2", help="Создать запись")
    p2.add_argument("full_name")
    p2.add_argument("birth_date")
    p2.add_argument("gender", choices=["Male","Female"])
    p2.set_defaults(func=task_insert)

    p3 = sub.add_parser("3", help="Вывод всех строк")
    p3.set_defaults(func=task_list)

    p4 = sub.add_parser("4", help="Автоматическое заполнение 1000000 строк")
    p4.set_defaults(func=task_fill)

    p5 = sub.add_parser("5", help = "Выборка по критерию")
    p5.set_defaults(func=task_timed_list)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()