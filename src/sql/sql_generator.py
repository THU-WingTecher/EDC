import random
from sql.expr_generator import ExprGenerator


class SQLGenerator:
    def __init__(self, target: str):
        self.database = target
        self.expr_generator = ExprGenerator(target)

    def generate_agg_select(self, ori_table: str, derived_table: str, expr: str, expr_type: str, derived_column, other_column_names: list, other_column_types: list):
        other_cols = f', {", ".join(other_column_names)}' if other_column_names else ''
        group_by = f'GROUP BY {", ".join(other_column_names)}' if other_column_names else ''
        base_select = f'SELECT {expr}{other_cols} FROM {ori_table}'
        equal_select = f'SELECT {derived_column}{other_cols} FROM {derived_table}'
        base_where_cond = self.expr_generator.generate_expr_on_column(ori_table, other_column_names, other_column_types, random.randint(3, 4))
        equal_where_cond = base_where_cond.replace(ori_table, derived_table)
        # having condition is a simple expression, do not use subquery
        having_cond = self.expr_generator.generate_expr_on_column(ori_table, [expr], [expr_type], 2)
        equal_having_cond = having_cond.replace(ori_table, derived_table).replace(expr, derived_column)
        base_select += f' WHERE {base_where_cond} {group_by} HAVING {having_cond}'
        equal_select += f' WHERE {equal_where_cond} AND ({equal_having_cond})'
        return base_select, equal_select
    
    def generate_func_select(self, ori_table: str, derived_table: str, expr: str, expr_type: str, derived_column, other_column_names: list, other_column_types: list):
        all_cols = other_column_names + [f'({expr})']
        all_types = other_column_types + [expr_type]
        # random select subset of all_cols
        base_select = f'SELECT {", ".join(random.sample(all_cols, random.randint(1, len(all_cols))))} FROM {ori_table}'
        base_where_cond = self.expr_generator.generate_expr_on_column(ori_table, all_cols, all_types, random.randint(3, 5))
        base_select += f' WHERE {base_where_cond}'
        if random.random() < 0.5:
            order_cols = random.sample(all_cols, random.randint(1, len(all_cols)))
            order_dirs = [random.choice(['ASC', 'DESC']) for _ in order_cols]
            order_clause = f" ORDER BY {', '.join(f'{col} {dir}' for col, dir in zip(order_cols, order_dirs))}"
            base_select += order_clause
        equal_select = base_select.replace(ori_table, derived_table).replace(expr, derived_column)

        return base_select, equal_select
    
    def generate_pred_select(self, ori_table: str, derived_table: str, expr: str, expr_type: str, derived_column, other_column_names: list, other_column_types: list):
        base_select = f'SELECT {", ".join(random.sample(other_column_names, random.randint(1, len(other_column_names))))} FROM {ori_table}'
        # use expr as normal column
        if random.random() < 0.3:
            all_cols = other_column_names + [f'({expr})']
            all_types = other_column_types + [expr_type]
            base_where_cond = self.expr_generator.generate_expr_on_column(ori_table, all_cols, all_types, random.randint(3, 4))
        else:
            base_where_cond = self.expr_generator.generate_expr_on_column(ori_table, other_column_names, other_column_types, random.randint(3, 4))
            base_where_cond = f'({expr}) {random.choice(["AND", "OR"])} {base_where_cond}'
        base_select += f' WHERE {base_where_cond}'
        
        if random.random() < 0.5:
            order_cols = random.sample(other_column_names, random.randint(1, len(other_column_names)))
            order_dirs = [random.choice(['ASC', 'DESC']) for _ in order_cols]
            order_clause = f" ORDER BY {', '.join(f'{col} {dir}' for col, dir in zip(order_cols, order_dirs))}"
            base_select += order_clause
        equal_select = base_select.replace(ori_table, derived_table).replace(expr, derived_column)

        return base_select, equal_select

    def generate_drop(self, table: str):
        return f'DROP TABLE IF EXISTS {table}'

    def generate_create(self, target: str, table: str, column_types: list, column_names: list):
        columns = ', '.join([f'{col_name} {col_type}' for col_name, col_type in zip(column_names, column_types)])
        if target == 'clickhouse':
            return f'CREATE TABLE {table} ({columns}) ORDER BY c0'
        else:
            return f'CREATE TABLE {table} ({columns})'
    
    def generate_insert(self, table: str, column_types: list, column_names: list):
        values = ", ".join([str(self.expr_generator.generate_random_value(col_type)) for col_type in column_types])
        return f'INSERT INTO {table} ({", ".join(column_names)}) VALUES ({values})'

    def generate_set(self, config_list: list):
        if self.database in ['mysql', 'mariadb']:
            config = random.choice(config_list)
            # 随机生成配置值
            if config == 'sql_mode':
                # 生成随机的 sql_mode 值
                sql_modes = ["''", "'STRICT_TRANS_TABLES'", "'NO_ZERO_IN_DATE'", "'NO_ZERO_DATE'"]
                value = random.choice(sql_modes)
            elif config == 'time_zone':
                # 生成随机的时区
                time_zones = ["'+00:00'", "'+08:00'", "'-05:00'", "'Europe/London'"]
                value = random.choice(time_zones)
            elif config in ['character_set_results', 'character_set_client', 'character_set_connection']:
                # 生成随机字符集
                charsets = ["'utf8'", "'utf8mb4'", "'latin1'", "'ascii'"]
                value = random.choice(charsets)
            elif config in ['collation_connection']:
                # 生成随机排序规则
                collations = ["'utf8_general_ci'", "'utf8mb4_general_ci'", "'latin1_swedish_ci'"]
                value = random.choice(collations)
            elif config in ['foreign_key_checks', 'sql_safe_updates']:
                # 生成随机布尔值
                value = random.choice([0, 1])
            elif config in ['query_cache_type']:
                # 生成随机查询缓存类型
                query_cache_types = [0, 1, 2]  # OFF, ON, DEMAND
                value = random.choice(query_cache_types)
            elif config in ['explicit_defaults_for_timestamp']:
                # 生成随机布尔值
                value = random.choice([0, 1])
            else:
                raise ValueError(f"Unsupported config type {config} for SET statements.")
            set_statement = f"SET {config} = {value};"
            return set_statement
        else:
            raise ValueError("Unsupported database type for SET statements.")
            