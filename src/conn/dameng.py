import decimal
import json
import dmPython
from loguru import logger
from conn.base import Connection, Result


class DamengConnection(Connection):
    def __init__(self, user: str, password: str, host: str, port: int, database: str, res_blacklist: list):
        def clean_database(conn, database: str):
            cursor = conn.cursor()
            try:
                cursor.execute(f'DROP SCHEMA IF EXISTS {database}' + ' CASCADE')
                cursor.execute(f'CREATE SCHEMA {database}')
            except Exception as e:
                logger.error('Create database {} failed, reason: {}', database, e)
            finally:
                cursor.close()
                conn.close()

        tmp_config = {
            'user': user,
            'password': password,
            'host': host,
            'port': port,
            'database': 'test',
        }
        tmp_conn = self.create_conn(tmp_config)
        clean_database(tmp_conn, database)
        super().__init__(user, password, host, port, database, res_blacklist)

    def create_conn(self, config: dict):
        try:
            return dmPython.connect(
                user=config['user'],
                password=config['password'],
                host=config['host'],
                port=config['port'],
                schema=config['database'],
            )
        except Exception as e:
            logger.error("Failed to connect to Dameng database, reason: {}", e)
            raise e

    def execute(self, sql: str):
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            if cursor.description is None:
                return Result(sql=sql, update_num=cursor.rowcount)
            else:
                res = []
                for row in cursor.fetchall():
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
            if any(blacklisted in str(e.args[0]).upper() for blacklisted in self.res_blacklist):
                return Result(sql=sql, error_msg=str(e.args[0]), blacklisted=True)
            else:
                return Result(sql=sql, error_msg=str(e.args[0]))
        finally:
            cursor.close()
    
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
            cursor.execute(f'DROP SCHEMA {database}' + ' CASCADE')
        except Exception as e:
            logger.error('Clean database {} failed, reason: {}', database, e)
        finally:
            cursor.close()
            tmp_conn.close()
