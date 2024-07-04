import subprocess
import platform
import re
from datetime import datetime
from collections import Counter, namedtuple


class PsLineInvalid(Exception):
    def __init__(self, line):
        self.line = line

    def __str__(self):
        return "Строчка процесса не распознана: " + self.line


def get_sys_info():
    return platform.system(), platform.freedesktop_os_release()['PRETTY_NAME']


def cmd_users_info():
    return subprocess.getoutput('cut -d: -f1 /etc/passwd')


def cmd_process_quantity():
    return subprocess.getoutput('ps ax | wc -l')


def cmd_process_list():
    return subprocess.getoutput('ps aux')


def cmd_get_process_user():
    return subprocess.getoutput("ps -eo user")


def count_process_by_user():
    process_by_user = cmd_get_process_user().split("\n")
    process_by_user.pop(0)
    return Counter(process_by_user)


def parse_ps_aux():
    ps_aux_out = cmd_process_list().split("\n")

    ps_headers = re.sub("\s+", ",", ps_aux_out.pop(0)).split(",")
    ps_headers = [s.replace('%', '') for s in ps_headers]
    proc = namedtuple("proc", ps_headers)

    # avahi        857  0.0  0.0   7992  4088 ?        Ss   июн24   0:12 avahi-daemon: running [wacer.local]
    reg = r"(\S+)\s+(\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+([DIRSTtWNXZMLsl<\+]+)\s+(\S+)\s+(\d+:\d{1,2})\s+(.+)"

    parsed_proc_list = []
    for ps in ps_aux_out:
        match = re.match(reg, ps)
        if not match:
            raise PsLineInvalid(ps)
        columns = match.groups()
        p = proc(*columns)
        parsed_proc_list.append(p)

    return parsed_proc_list


def main():
    out_lines: list[str] = [f"System : {get_sys_info()}"]

    users_info = cmd_users_info().replace("\n", ", ")
    out_lines.append(f'Пользователи системы : {users_info}')
    out_lines.append(f"Процессов запущено :  {cmd_process_quantity()}")

    out_lines.append("Пользовательских процессов : ")
    proc = count_process_by_user()
    for key, value in proc.most_common():
        out_lines.append(f"\t{key} : {value}")

    try:
        process = parse_ps_aux()
        mem_sum = cpu_sum = 0
        max_mem_proc = None
        max_cpu_proc = None
        for p in process:
            mem = float(p.MEM)
            cpu = float(p.CPU)
            mem_sum += mem
            cpu_sum += cpu
            if max_cpu_proc is None or mem > float(max_mem_proc.CPU):
                max_mem_proc = p
            if max_mem_proc is None or cpu > float(max_mem_proc.MEM):
                max_cpu_proc = p

    except PsLineInvalid as ex:
        print(ex)

    out_lines.append(f"Всего памяти используется: {mem_sum}%")
    out_lines.append(f"Всего CPU используется: {cpu_sum}%")

    proc_name_mem = max_mem_proc.COMMAND[:20].rstrip() + "..." if len(max_mem_proc.COMMAND) > 20 else max_mem_proc.COMMAND
    proc_name_cpu = max_cpu_proc.COMMAND[:20].rstrip() + "..." if len(max_cpu_proc.COMMAND) > 20 else max_cpu_proc.COMMAND

    out_lines.append(f"Больше всего памяти использует: {proc_name_mem} - {max_mem_proc.MEM}%")
    out_lines.append(f"Больше всего CPU использует: {proc_name_cpu} - {max_mem_proc.CPU}%")

    return out_lines


if __name__ == '__main__':
    data = main()
    now = datetime.today()
    filename = now.strftime('%d-%m-%Y-%H:%M:%S') + "-scan.txt"
    with open(filename, "w") as fp:
        fp.writelines(([string + '\n' for string in data]))



