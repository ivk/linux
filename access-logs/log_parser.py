import re
from collections import namedtuple
from datetime import datetime
import sqlite3

FIELD_NAMES = ["ip", "date", "method", "url", "status", "bytes", "duration"]


# import sqlalchemy


class PsLineInvalid(Exception):
    def __init__(self, line):
        self.line = line

    def __str__(self):
        return "Строчка процесса не распознана: " + self.line


# "ip", "something", "username", "date", "request", "status", "bytes", "referer", "ua", "duration"
reg = r"([\d\.]+)\s(.+)\s(.+)\s+\[(.+)\]\s\"(.*?)\"\s(\d+)\s(\d+|-)\s\"(.*?)\"\s\"(.*?)\"\s(\d+)"
log_string = namedtuple("log_string",
                        FIELD_NAMES)

CREATE_TABLE = '''
        CREATE TABLE IF NOT EXISTS accessShort (
        id INTEGER PRIMARY KEY,
        ip TEXT NOT NULL,
        date datetime NOT NULL,
        method TEXT NOT NULL,
        url TEXT NOT NULL,
        status INTEGER NOT NULL,
        bytes INTEGER,
        duration INTEGER
    )
'''
DROP_TABLE = '''
    drop table if exists accessShort
'''


def log_parser(line):
    ret = {}
    match = re.match(reg, line)
    if not match:
        raise PsLineInvalid(line)
    columns = match.groups()
    request = columns[4].split(' ')
    ret = log_string(
        columns[0],
        datetime.strptime(columns[3], "%d/%b/%Y:%H:%M:%S %z"),
        # columns[3],
        request[0],
        request[1],
        int(columns[5]),
        int(columns[6]),
        int(columns[9])
    )

    return ret


def log_reader(filename):
    parsed_log = []
    with open(filename, "r") as fp:
        for line in fp:
            try:
                parsed_log.append(log_parser(line.rstrip()))
            except PsLineInvalid as ex:
                print(ex)

    return parsed_log


# def db_use(lines):
#     connection_string = 'sqlite://'
#     db = sqlalchemy.create_engine(connection_string)
#     engine = db.connect()
#     meta = sqlalchemy.MetaData(engine)
#
#     columns = (
#         sqlalchemy.Column('id', sqlalchemy.Integer, autoincrement="auto", primary_key=True),
#         sqlalchemy.Column('ip', sqlalchemy.Text, nullable=False),
#         sqlalchemy.Column('date', sqlalchemy.DateTime),
#         sqlalchemy.Column('method', sqlalchemy.Text, nullable=False),
#         sqlalchemy.Column('url', sqlalchemy.Text, nullable=False),
#         sqlalchemy.Column('status', sqlalchemy.Integer, nullable=False),
#         sqlalchemy.Column('bytes', sqlalchemy.Integer, nullable=True),
#         sqlalchemy.Column('duration', sqlalchemy.Integer, nullable=True),
#
#     )
#     sqlalchemy.Table("accesslog", meta, *columns)
#     meta.create_all()
#     table = sqlalchemy.table("accesslog", *columns)
#
#     statements = [
#         table.insert().values(log_string)
#         for log_string in lines
#     ]
#     [engine.execute(stmt) for stmt in statements]


def main():
    lines = log_reader('logs/access-short.log')
    # db_use(lines)
    print(lines)
    connection = sqlite3.connect('sqlite-db/db_logs.db')
    cursor = connection.cursor()
    cursor.execute(CREATE_TABLE)
    connection.commit()

    fields_template = '?'*len(FIELD_NAMES)
    fields_template = ','.join(fields_template)

    ins_query = f"insert into accessShort ({','.join(FIELD_NAMES)}) values ({fields_template})"
    for line in lines:
        cursor.execute(ins_query, line)
    connection.commit()

    connection.close()


if __name__ == '__main__':
    main()