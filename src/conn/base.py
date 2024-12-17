from enum import Enum
from loguru import logger
from abc import abstractmethod


class Connection:
    def __init__(self, user: str, password: str, host: str, port: int, database: str, res_blacklist: list):
        self.config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'port': port,
        }
        self.conn = self.create_conn(self.config)
        self.res_blacklist = res_blacklist
    @abstractmethod
    def create_conn(self, config: dict):
        pass

    @abstractmethod
    def execute(self, sql: str):
        pass

    @abstractmethod
    def clean(self):
        pass

    def close(self):
        self.clean()
        try:
            self.conn.close()
        except Exception as e:
            logger.error('Connection closed failed, reason: {}', e)


class Result:
    class __ResultType(Enum):
        DEFAULT = 0
        ERROR = 1
        RESULT = 2
        UPDATE = 3

    def __init__(self, sql, error_msg=None, update_num=None, res=None, blacklisted=False):
        self.sql = sql
        self.type = self.__ResultType.DEFAULT
        self.update_num = None
        self.sorted_res = []
        self.error = None
        self.error_msg = None
        self.blacklisted = blacklisted

        if update_num is not None:
            self.update_num = update_num
            self.type = self.__ResultType.UPDATE
        elif res is not None:
            self.sorted_res = sorted(res)
            self.type = self.__ResultType.RESULT
        if error_msg is not None:
            if "(" in error_msg:
                self.error = error_msg.split("(")[0]
            else:
                self.error = ''
            self.error_msg = error_msg
            self.type = self.__ResultType.ERROR

    def is_error(self):
        return self.type == self.__ResultType.ERROR

    def get_res(self):
        if self.type == self.__ResultType.ERROR:
            return [f"-- error: {self.error}, message: {self.error_msg}"]
        elif self.type == self.__ResultType.UPDATE:
            return [f"-- update: {self.update_num}"]
        elif self.type == self.__ResultType.RESULT:
            res = [f"-- result: length {len(self.sorted_res)}"]
            for s in self.sorted_res:
                res.append("-- " + s)
            return res

    def print_log(self, level: str):
        if level == "info":
            if self.type == self.__ResultType.RESULT:
                logger.info(f"result: length {len(self.sorted_res)}")
                for s in self.sorted_res:
                    logger.info(s)
            elif self.type == self.__ResultType.UPDATE:
                logger.info("update: {}", self.update_num)
            elif self.type == self.__ResultType.ERROR:
                logger.info("error: {}, message: {}", self.error, self.error_msg)
        if level == "error":
            if self.type == self.__ResultType.RESULT:
                logger.error(f"result: length {len(self.sorted_res)}")
                for s in self.sorted_res:
                    logger.error(s)
            elif self.type == self.__ResultType.UPDATE:
                logger.error("update: {}", self.update_num)
            elif self.type == self.__ResultType.ERROR:
                logger.error("error: {}, message: {}", self.error, self.error_msg)

    def __eq__(self, other):
        if isinstance(other, Result):
            # if both are error, we consider them are equal
            if self.type == self.__ResultType.ERROR and other.type == self.__ResultType.ERROR:
                return True
            else:
                return self.type == other.type and self.update_num == other.update_num and \
                    len(self.sorted_res) == len(other.sorted_res) and self.sorted_res == other.sorted_res
        return False

