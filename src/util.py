import os
import importlib
import configparser
from typing import List
from loguru import logger
from conn.base import Result
import shutil


################# database operation #################
def get_conn(target: str, database: str):
    conn_config = configparser.ConfigParser()
    conn_config.read('./config/conn.ini')

    db_config = {
        'user': conn_config[target]['user'],
        'password': conn_config[target]['password'],
        'host': conn_config[target]['host'],
        'port': int(conn_config[target]['port']),
        'database': database
    }

    # Get blacklist from config
    res_blacklist = conn_config[target].get('res_blacklist', [])
    if isinstance(res_blacklist, str):
        # Convert string representation of list to actual list
        res_blacklist = eval(res_blacklist)
    db_config['res_blacklist'] = res_blacklist  # Pass blacklist to connection
    
    conn_class_path = conn_config[target]['conn']
    module_name, class_name = conn_class_path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    conn_class = getattr(module, class_name)
    return conn_class(**db_config)


################# file operation #################
def clean_dir(path: str):
    if os.path.exists(path) and os.path.isdir(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):  # Handle subdirectories
                    shutil.rmtree(file_path)
            except PermissionError as e:
                logger.error(f"Permission denied: {file_path}. Reason: {e}")
            except Exception as e:
                logger.error(f"Failed to delete {file_path}. Reason: {e}")


def read_file(path: str):
    with open(path, 'r', encoding='utf-8') as file:
        lines = [line.strip() for line in file if line.strip()]
    return lines


def log_res(sz: str, op: str, res1: List[Result], res2: List[Result], insert_res: List[Result], name: str, out_path: str):

    def sql_to_file(sql, filepath: str):
        dir_path = os.path.join(out_path, f'{op}-{sz}')
        os.makedirs(dir_path, exist_ok=True)  
        full_path = os.path.join(dir_path, filepath)

        with open(full_path, "w", encoding='utf-8') as file:
            for s in sql:
                file.write(s)
                if not s.endswith(";"):
                    file.write(";")
                file.write("\n")

    if op == '/':
        op = 'div'
    ori_sql_list, dest_sql_list, insert_sql_list = [], [], [r.sql for r in insert_res]
    error = []
    diff = False
    for i in range(len(res1)):
        r1, r2 = res1[i], res2[i]
        ori_sql_list.append(r1.sql)
        dest_sql_list.append(r2.sql)
        if r1 != r2 and i != 0:
            if r1.blacklisted or r2.blacklisted:
                continue
            diff = True
            logger.error("-------------------------------------------------" + name)
            logger.error("origin: " + str(r1.sql))
            logger.error("dest: " + str(r2.sql))
            r1.print_log("error")
            r2.print_log("error")
            error.extend(r1.get_res())
            error.extend(r2.get_res())

    if diff:
        log_sql = []
        log_sql.extend(error)
        log_sql.append(ori_sql_list[0])
        log_sql.extend(insert_sql_list)
        log_sql.append(dest_sql_list[0])

        for s1, s2 in zip(ori_sql_list[1:], dest_sql_list[1:]):
            log_sql.append(s1)
            log_sql.append(s2)
        sql_to_file(log_sql, f'{name}.sql')