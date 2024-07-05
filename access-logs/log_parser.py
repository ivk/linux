import re
from collections import namedtuple
from datetime import datetime
import sqlite3
import json

FIELD_NAMES = ["ip", "date", "method", "url", "status", "bytes", "duration"]
log_string = namedtuple("log_string", FIELD_NAMES)

# "ip", "something", "username", "date", "request", "status", "bytes", "referer", "ua", "duration"
reg = r"([\d\.]+)\s(.+)\s(.+)\s+\[(.+)\]\s\"(.*?)\"\s(\d+)\s(\d+|-)\s\"(.*?)\"\s\"(.*?)\"\s(\d+)"

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
CREATE_INDEX = (
    "CREATE INDEX 'ip' ON 'accessShort' ('ip');",
    "CREATE INDEX 'method' ON 'accessShort' ('method');",
    "CREATE INDEX 'status' ON 'accessShort' ('status');",
    "CREATE INDEX 'url' ON 'accessShort' ('url');"
)
DROP_TABLE = '''
    drop table if exists accessShort
'''
COUNT_ALL = '''
    SELECT count(*) from accessShort 
'''
METHODS_COUNT = '''
    SELECT DISTINCT(method), count(*) 
        from accessShort 
    group by method order by 2 desc
'''
TOP3_IP = '''
    SELECT distinct(ip), count(*) 
        from accessShort 
    group by ip order by 2 desc 
    limit 3
'''
TOP3_DURATION = '''
    SELECT method, url, ip, duration, date 
        from accessShort 
    order by duration desc limit 3
'''


class PsLineInvalid(Exception):
    def __init__(self, line):
        self.line = line

    def __str__(self):
        return "Couldn't recognize this log line: " + self.line


def log_parser(line):
    """
    parse a line from access log file to database-compatible list
    :param line:
    :return:
    """
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
        int(columns[6]) if columns[6] != '-' else 0,
        int(columns[9])
    )

    return ret


def log_reader(file_object, chunk_size=1024):
    """
    generator, reads a portion of lines from file
    :param file_object: opened file obj
    :param chunk_size:  lines from file to be read
    :return: a portion of lines
    """
    ret = []
    while True:
        data = file_object.readline()
        if not data:
            yield ret
            break
        ret.append(data)
        if len(ret) == chunk_size:
            yield ret
            ret = []


def write_to_base(connection, lines):
    """
    put a portion of formatted lines into the table
    :param connection: sqlite db connect
    :param lines: values to insert
    :return:
    """
    cursor = connection.cursor()
    fields_template = '?' * len(FIELD_NAMES)
    fields_template = ','.join(fields_template)

    ins_query = f"insert into accessShort ({','.join(FIELD_NAMES)}) values ({fields_template})"
    for line in lines:
        cursor.execute(ins_query, line)
    connection.commit()


def get_some_analytics(connection):
    # let's get some analytics
    res = connection.cursor().execute(COUNT_ALL)
    total_requests = res.fetchone()[0]

    res = connection.cursor().execute(METHODS_COUNT)
    total_stat= {}
    for stat in res.fetchall():
        total_stat[stat[0]] = stat[1]

    res = connection.cursor().execute(TOP3_DURATION)
    top_longest = []
    for stat in res.fetchall():
        top_longest.append({"ip": stat[0], "date": stat[1], "method": stat[2], "url":stat[3], "duration": stat[4]})

    res = connection.cursor().execute(TOP3_IP)
    top_ips = {}
    for stat in res.fetchall():
        top_ips[stat[0]] = stat[1]

    data = {
        "top_ips": top_ips,
        "top_longest": top_longest,
        "total_stat": total_stat,
        "total_requests": total_requests
    }

    return data


def write_result(data):
    with open('res/result.json', "w") as f:
        s = json.dumps(data, indent=4)
        f.write(s)


def main():
    # prepare a db
    connection = sqlite3.connect('sqlite-db/db_logs.db')
    connection.cursor().execute(DROP_TABLE)
    connection.cursor().execute(CREATE_TABLE)
    for indx in CREATE_INDEX:
        connection.cursor().execute(indx)
    connection.commit()

    counter = 1
    # read a file first
    with open('logs/access.log') as f:
        bunch = log_reader(f)
        for lines in bunch:
            # print(lines)

            # parse a bunch of lines - one by one, it takes time
            parsed_lines = []
            for line in lines:
                try:
                    parsed_lines.append(log_parser(line.rstrip()))
                except PsLineInvalid as ex:
                    print(ex)
            # print(parsed_lines)

            # put parsed strings into db
            write_to_base(connection, parsed_lines)

            # print a point as a sign of a life
            print(".", end="")
            if counter == 64:
                print("\n")
                counter = 1
            counter += 1

    data = get_some_analytics(connection)
    write_result(data)

    connection.close()


if __name__ == '__main__':
    start = datetime.now()
    main()
    end = datetime.now()
    print("\n", "Finished. Total time was",  end - start)
