import sys
import random
import argparse
from loguru import logger
from config import config
from util import clean_dir, get_conn, log_res, read_file
from sql.sql_generator import SQLGenerator
import traceback
from enum import Enum

class OpType(Enum):
    AGGREGATE = 1
    FUNCTION = 2
    PREDICATE = 3

def generate_equal_expr(op: str, op_type: OpType, column_types: list, column_names: list):
    if op_type == OpType.AGGREGATE:
        return f'{op}({",".join(column_names)})'
    elif op_type == OpType.FUNCTION:
        if op in ['+', '-', '*', '/', '%']:
            return op.join(column_names)
        else:
            return f'{op}({",".join(column_names)})'
    elif op_type == OpType.PREDICATE:
        if (op in ['IS NULL', 'IS NOT NULL']) and len(column_names) == 1:
            return f'{column_names[0]} {op}'
        elif (op in ['LIKE', 'NOT LIKE', 'IS']) and len(column_names) == 2:
            return f'{column_names[0]} {op} {column_names[1]}'
        elif op in ['>', '<', '<=', '>=', '<>'] and len(column_names) == 2:
            return f'{column_names[0]} {op} {column_names[1]}'
        elif op == 'BETWEEN' and len(column_names) == 3:
            return f'{column_names[0]} {op} {column_names[1]} AND {column_names[2]}'
        elif (op in ['IN', 'NOT IN']) and len(column_names) > 1:
            return f'{column_names[0]} {op} ({",".join(column_names[1:])})'
        else:
            raise ValueError(f'Invalid operation type and op combination: {op}, {column_types}')
    else:
        raise ValueError(f'Invalid operation type and op combination: {op}, {column_types}')


def get_derived_type(conn, target: str, derived_table: str, col: str):
    derived_type = None
    if target == 'monetdb':
        res = conn.execute(f"SELECT c.type AS data_type FROM sys.columns c "
                                    f"JOIN sys.tables t ON c.table_id = t.id "
                                    f"WHERE t.name = '{derived_table}' AND c.name = '{col}';")
    elif target == 'duckdb':
        res = conn.execute(f"SELECT column_name, data_type "
                                    f"FROM information_schema.columns "
                                    f"WHERE table_name = '{derived_table}' AND column_name = '{col}';")
    elif target == 'dameng':
        res = conn.execute(f"""SELECT C.TYPE$
                                    FROM SYS.SYSCOLUMNS C
                                    JOIN SYS.SYSOBJECTS T ON C.ID = T.ID
                                    JOIN SYS.SYSOBJECTS S ON T.SCHID = S.ID
                                    WHERE C.NAME = '{col.upper()}'
                                    AND T.NAME = '{derived_table.upper()}'
                                    AND S.NAME = '{conn.config['database'].upper()}';""")
    else:
        res = conn.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE "
                                    f"TABLE_SCHEMA = '{conn.config['database']}' AND "
                                    f"TABLE_NAME = '{derived_table}' AND "
                                    f"COLUMN_NAME = '{col}'")
    if res.sorted_res:
        derived_type = res.sorted_res[0]
    else:
        raise ValueError(f'Failed to get data type of {col} in {derived_table}: {res.error_msg}, sql: {res.sql}')
    return derived_type


def construct_derived_table(conn, target: str, ori_table: str, derived_table: str, other_column_names: list, 
                          other_column_types: list, test_expr: str, op_type: OpType, dest_res: list, insert_res: list):
    """
    Constructs a derived table based on the target database system and operation type.
    
    Args:
        conn: Database connection
        target: Target database system ('clickhouse', 'tidb', or others)
        derived_table: Name of the derived table to create
        other_column_names: List of column names besides the test expression
        other_column_types: List of column types for other columns
        test_expr: The test expression to evaluate
        op_type: Type of operation (1 for aggregate, others for non-aggregate)
        ori_res: List to store original results
        insert_res: List to store insert results
    """
    # Prepare column definitions
    other_columns_with_alias = ", ".join([f"{col} AS {col}" for col in other_column_names])
    if other_columns_with_alias:
        other_columns_with_alias = f', {other_columns_with_alias}'
    
    # Prepare GROUP BY clause for aggregate operations
    group_by_clause = ''
    if op_type == OpType.AGGREGATE:
        group_by_clause = f'GROUP BY {", ".join(other_column_names)}'

    # Create table based on target database system
    if target == 'clickhouse':
        res = _create_clickhouse_table(conn, ori_table, derived_table, test_expr, other_columns_with_alias, 
                                     group_by_clause)
    elif target == 'tidb':
        try:
            res = _create_tidb_table(conn, ori_table, derived_table, test_expr, other_column_names, 
                                    other_column_types, group_by_clause, insert_res)
        except ValueError as e:
            raise ValueError(f'Failed to create derived table {derived_table}: {e}')
    else:
        res = _create_default_table(conn, ori_table, derived_table, test_expr, other_columns_with_alias, group_by_clause)
    dest_res.append(res)
    if res.is_error():
        raise ValueError(f'Failed to create derived table {derived_table}: {res.error_msg}, sql: {res.sql}')

def _create_clickhouse_table(conn, ori_table, derived_table, test_expr, other_columns_with_alias, group_by_clause):
    """Helper function for Clickhouse table creation"""
    base_sql = f'CREATE TABLE {derived_table} ORDER BY c0 AS (SELECT ({test_expr}) AS c0 {other_columns_with_alias} FROM {ori_table} {group_by_clause})'
    return conn.execute(base_sql)

def _create_tidb_table(conn, ori_table, derived_table, test_expr, other_column_names, other_column_types, 
                      group_by_clause, insert_res):
    """Helper function for TiDB table creation"""
    # Create temporary view to get derived type
    view_sql = f'CREATE OR REPLACE VIEW {derived_table} AS (SELECT ({test_expr}) AS c0 FROM {ori_table} {group_by_clause})'
    conn.execute(view_sql)
    derived_type = get_derived_type(conn, 'tidb', derived_table, 'c0')
    conn.execute(f'DROP VIEW {derived_table}')
    
    # Create table with proper column definitions
    other_columns = ', '.join([f'{col_name} {col_type}' for col_name, col_type in zip(other_column_names, other_column_types)])
    other_columns = f', {other_columns}' if other_columns else ''
    res = conn.execute(f'CREATE TABLE {derived_table} (c0 {derived_type} {other_columns})')
    
    # Insert data
    other_cols = f', {", ".join(other_column_names)}' if other_column_names else ''
    base_sql = f'INSERT INTO {derived_table} SELECT ({test_expr}) {other_cols} FROM {ori_table} {group_by_clause}'
    insert_res.append(conn.execute(base_sql))
    if insert_res[-1].is_error():
        raise ValueError(f'Failed to insert data into {derived_table}: {insert_res[-1].error_msg}, sql: {insert_res[-1].sql}')
    return res

def _create_default_table(conn, ori_table, derived_table, test_expr, other_columns_with_alias, group_by_clause):
    """Helper function for default table creation"""
    base_sql = f'CREATE TABLE {derived_table} AS (SELECT ({test_expr}) AS c0 {other_columns_with_alias} FROM {ori_table} {group_by_clause})'
    return conn.execute(base_sql)


def main():
    conn = None
    ori_res = []
    dest_res = []
    insert_res = []
    
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('target', type=str, help='The target database name, support mysql, mariadb, tidb, clickhouse')
        parser.add_argument('--debug', action='store_true', help='Enable debug logging')
        args = parser.parse_args()

        target = args.target
        type_path = f'./seed/{target}/type'
        agg_path = f'./seed/{target}/agg'
        func_path = f'./seed/{target}/func'
        pred_path = f'./seed/{target}/pred'
        log_path = f'../log/{target}/'
        out_path = f'../res/{target}/'

        logger.remove()
        logger.add(sys.stderr, level='ERROR')
        if args.debug:
            logger.add(log_path + 'error_{time}.log', 
                        format='{time} {level} {message}', 
                        level='ERROR', 
                        rotation='10 MB', 
                        compression='zip')
            logger.add(log_path + 'info_{time}.log', 
                        format='{time} {level} {message}', 
                        level='INFO', 
                        rotation='50 MB', 
                        compression='zip') 
        random.seed(config.seed)

        clean_dir(out_path)
        type_list = read_file(type_path)
        agg_list = read_file(agg_path)
        func_list = read_file(func_path)
        pred_list = read_file(pred_path)
        loop = 0
        while True:
            loop += 1
            test_column = random.randint(1, config.test_column_cnt)
            other_column = random.randint(0, config.other_column_cnt)
            column_types = []
            column_names = []
            op_type = random.choice(list(OpType))

            # select target operation
            if op_type == OpType.AGGREGATE:
                op = random.choice(agg_list)
                other_column = max(1, other_column)
            elif op_type == OpType.FUNCTION:
                op = random.choice(func_list)
            elif op_type == OpType.PREDICATE:
                op = random.choice(pred_list)
                other_column = max(1, other_column)
        
            # generate column types and names in original table
            for i in range(test_column + other_column):
                column_type = random.choice(type_list)
                if column_type.startswith('ENUM'):
                    enum_size = random.randint(2, 5)
                    enum_values = [f"'val{j}'" for j in range(enum_size)]
                    column_type = f"ENUM({','.join(enum_values)})"
                if column_type.startswith('CHAR') or column_type.startswith('VAR') or column_type == 'BINARY':
                    column_length = random.randint(1, 30)
                    column_type = f"{column_type}({column_length})"
                column_types.append(column_type)
                column_names.append(f'c{i}')
            
            test_column_names = column_names[:test_column]
            other_column_names, other_column_types = column_names[test_column:], column_types[test_column:]
            
            # generate test expression
            try:
                test_expr = generate_equal_expr(op, op_type, column_types, test_column_names)
            except ValueError as e:
                continue

            # create database and original table
            conn = get_conn(target, f'database{loop}')
            ori_table, derived_table = 't0', 't1'
            ori_res, dest_res, insert_res = [], [], []
            sql_generator = SQLGenerator(target)
            conn.execute(sql_generator.generate_drop(ori_table))
            res = conn.execute(sql_generator.generate_create(target, ori_table, column_types, column_names))
            ori_res.append(res)

            # test if the test expression is valid
            res1 = conn.execute(sql_generator.generate_insert(ori_table, column_types, column_names))
            insert_res.append(res1)
            res2 = conn.execute(f'SELECT {test_expr} FROM {ori_table}')
            if res1.is_error():
                logger.info(f'Early stop, reason: Failed to insert data into {ori_table}: {res1.error_msg}, sql: {res1.sql}')
                conn.close()
                continue
            if res2.is_error():
                logger.info(f'Early stop, reason: Failed to select data from {ori_table}: {res2.error_msg}, sql: {res2.sql}')
                conn.close()
                continue

            # insert data into original table
            for i in range(random.randint(1, 30)):
                res = conn.execute(sql_generator.generate_insert(ori_table, column_types, column_names))
                insert_res.append(res)
            
            # create and store data in derived table
            conn.execute(sql_generator.generate_drop(derived_table))
            try:
                construct_derived_table(conn, target, ori_table, derived_table, other_column_names, other_column_types, test_expr, op_type, dest_res, insert_res)
            except ValueError as e:
                logger.info(f'Early stop, reason: Failed to create derived table {derived_table}: {e}')
                conn.close()
                continue

            # generate equivalent select statement and check the consistency
            print(f'testing type: {column_types}, op: {op}')
            expr_col = 'c0'
            try:
                expr_type = get_derived_type(conn, target, derived_table, expr_col)
            except ValueError as e:
                logger.info(f'Early stop, reason: Failed to get data type of {expr_col} in {derived_table}: {e}')
                conn.close()
                continue

            for i in range(config.select_cnt):
                if op_type == OpType.AGGREGATE:
                    base_select, equal_select = sql_generator.generate_agg_select(ori_table, derived_table, test_expr, expr_type, expr_col, other_column_names, other_column_types)
                elif op_type == OpType.FUNCTION:
                    base_select, equal_select = sql_generator.generate_func_select(ori_table, derived_table, test_expr, expr_type, expr_col, other_column_names, other_column_types)
                elif op_type == OpType.PREDICATE:
                    base_select, equal_select = sql_generator.generate_pred_select(ori_table, derived_table, test_expr, expr_type, expr_col, other_column_names, other_column_types)
                try:
                    # Execute and check results
                    res1 = conn.execute(base_select)
                    res2 = conn.execute(equal_select)
                    ori_res.append(res1)
                    dest_res.append(res2)
                    
                    if res1.blacklisted or res2.blacklisted:
                        logger.info(f'Skipping blacklisted error: {res1.error_msg or res2.error_msg}')
                        continue
                    
                    if res1 != res2:
                        break

                except Exception as e:
                    # 记录导致崩溃的 SQL 和错误信息
                    logger.error(f"SQL execution error in loop {loop}: {str(e)}")
                    
                    # 即使发生错误也记录结果
                    log_res(test_column, op, ori_res, dest_res, insert_res, f'crash_test_{target}_{loop}_error', out_path)
                    continue  # 或者 continue

            conn.close()
            log_res(test_column, op, ori_res, dest_res, insert_res, f'test_{target}_{loop}', out_path)

    except Exception as e:
        logger.error(f"Main error: {e}")
        logger.error(traceback.format_exc())
        # 在主循环外的错误也记录结果
        if 'base_select' in locals() and 'equal_select' in locals():
            log_res(test_column, op, ori_res, dest_res, insert_res, f'crash_test_{target}_main_error', out_path)
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Critical error: {e}")
        logger.error(traceback.format_exc())