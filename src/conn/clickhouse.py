import decimal
import json
from clickhouse_connect import get_client
from loguru import logger
from conn.base import Connection, Result


class ClickHouseConnection(Connection):
    def __init__(self, user: str, password: str, host: str, port: int, database: str, res_blacklist: list):
        def clean_database(conn, database: str):
            try:
                conn.query('DROP DATABASE IF EXISTS ' + database)
                conn.query('CREATE DATABASE ' + database)
            except Exception as e:
                logger.error('Create database {} failed, reason: {}', database, e)
            finally:
                conn.close()

        tmp_config = {
            'user': user,
            'password': password,
            'host': host,
            'database': 'test',
            'port': port,
        }
        tmp_conn = self.create_conn(tmp_config)
        clean_database(tmp_conn, database)

        super().__init__(user, password, host, port, database, res_blacklist)

    def create_conn(self, config: dict):
        return get_client(username=config['user'], password=config['password'], host=config['host'],
                          port=config['port'], database=config['database'])

    def execute(self, sql: str):
        try:
            res = []
            rows = self.conn.query(sql).result_rows
            for row in rows:
                tmp_row = []
                for col in row:
                    if isinstance(col, (bytes, bytearray)):
                        r = col.hex()
                    elif isinstance(col, float) and col.is_integer():
                        r = str(col) if 'e' in str(col) or 'E' in str(col) else int(col)
                    elif isinstance(col, decimal.Decimal) and (float(col)).is_integer():
                        r = str(col) if 'e' in str(col) or 'E' in str(col) else int(col)
                    elif isinstance(col, dict):
                        r = json.dumps(col)
                    elif isinstance(col, list):
                        r = sorted(col)
                    else:
                        r = str(col)
                    tmp_row.append(r)
                res.append(' * '.join(map(str, tmp_row)))
            return Result(sql=sql, res=res)
        except Exception as e:
            if any(blacklisted in repr(e).upper() for blacklisted in self.res_blacklist):
                return Result(sql=sql, error_msg=repr(e), blacklisted=True)
            else:
                return Result(sql=sql, error_msg=repr(e))

    def clean(self):
        database = self.config['database']
        tmp_config = {
            'user': self.config['user'],
            'password': self.config['password'],
            'host': self.config['host'],
            'port': self.config['port'],
            'database': 'test',
        }
        tmp_conn = self.create_conn(tmp_config)
        cursor = tmp_conn.cursor()
        
        try:
            cursor.execute('DROP DATABASE IF EXISTS ' + database)
        except Exception as e:
            logger.error('Clean database {} failed, reason: {}', database, e)
        finally:
            cursor.close()