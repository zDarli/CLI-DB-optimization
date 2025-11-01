from db import conn_ctx
from models import DDL

def run(args):
    with conn_ctx() as c:
        c.executescript(DDL)
    print("Таблица успешно создана или уже существует в данной директории.")