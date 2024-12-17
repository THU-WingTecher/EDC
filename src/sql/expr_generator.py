import datetime
import random
import string
import uuid


class Constant:
    def __init__(self, value: str, ori_type: str = None, dest_type: str = None):
        self.value = value
        self.ori_type = ori_type
        self.dest_type = dest_type

    def __str__(self):
        if self.dest_type:
            return f"(CAST(({self.value}) AS {self.dest_type}))"
        else:
            return str(self.value)

    def get_type(self):
        return self.ori_type


def generate_expr_on_column(table: str, column_names: list, column_types: list, depth: int) -> str:       
    type = [] 
    if column_names:
        if depth == 1:
            type += ['COLUMN', 'CONSTANT']
        if depth > 1:
            type += ['=', '!=', '>', '<=', 'AND', 'OR']
        if depth > 2:
            type += ['CASE', 'SUBQUERY']

    op = random.choice(type)
    if op == 'CONSTANT':
        if random.randint(0, 5) < 3:
            return str(generate_single_constant(random.choice(column_types))) if column_types else generate_single_constant()
        elif random.randint(0, 1) == 0:
            return str(generate_expr_constant(random.choice(column_types))) if column_types else generate_expr_constant()
        else:
            return 'NULL'
    elif op == 'COLUMN':
        return random.choice(column_names)
    elif op == 'CASE':
        return f'(CASE WHEN {generate_expr_on_column(table, column_names, column_types, depth - 1)} THEN ({generate_expr_constant(random.choice(column_types))}) ELSE ({generate_expr_constant(random.choice(column_types))}) END)'
    elif op == 'SUBQUERY':
        aggs = ['IN', 'NOT IN']
        agg = random.choice(aggs)
        return f'{generate_expr_on_column(table, column_names, column_types, depth - 1)} {agg} (SELECT {random.choice(column_names)} FROM {table} WHERE {generate_expr_on_column(table, column_names, column_types, depth - 1)})'
    else:
        return f'({generate_expr_on_column(table, column_names, column_types, depth - 1)} {op} {generate_expr_on_column(table, column_names, column_types, depth - 1)})'


def generate_expr_constant(ori_typ: str = None, dest_typ: str = None) -> Constant:
    expr_op = ['+', '*', '/', '<<', '>>', '&', '|', '^']
    op = random.choice(expr_op)
    size = random.randint(1, 1)
    args = []
    for i in range(size):
        # 不加dest_typ是为了避免表达式内部的CAST
        args.append(generate_single_constant(ori_typ))
    expr = op.join([str(arg) for arg in args])
    return Constant(f'({expr})', ori_typ, dest_typ)


def generate_single_constant(ori_typ: str = None, dest_typ: str = None) -> Constant:
    value = generate_random_value(ori_typ)
    return Constant(value, ori_typ, dest_typ)


def generate_random_value(data_type, depth=0, max_depth=3):
    # Prevent stack overflow with depth check
    if depth >= max_depth:
        return '0'  # Return a safe default value when max depth is reached
    
    data_type = data_type.upper() if data_type else 'INT'

    if data_type.startswith('ARRAY'):
        inner_type = data_type[data_type.find('(')+1:data_type.find(')')]
        size = random.randint(1, 2)  # Reduced size to minimize nesting
        values = [str(generate_random_value(inner_type, depth + 1, max_depth)) for _ in range(size)]
        return f"[{', '.join(values)}]"
    elif data_type.startswith('TUPLE'):
        if '(' in data_type and ')' in data_type:
            inner_types = data_type[data_type.find('(')+1:data_type.find(')')].split(',')
            values = []
            for typ in inner_types:
                typ = typ.strip()
                values.append(str(generate_random_value(typ, depth + 1, max_depth)))
            return f"({', '.join(values)})"
        return "(0, 0)"
    elif data_type.startswith('MAP'):
        if '(' in data_type and ')' in data_type:
            type_part = data_type[data_type.find('(')+1:data_type.find(')')]
            key_type, value_type = [t.strip() for t in type_part.split(',', 1)]
            
            size = random.randint(1, 2)  # Reduced size to minimize nesting
            pairs = []
            for _ in range(size):
                key = generate_random_value(key_type, depth + 1, max_depth)
                value = generate_random_value(value_type, depth + 1, max_depth)
                pairs.append(f"{key}: {value}")
            return f"{{{', '.join(pairs)}}}"
        return "{}"
    elif data_type == "JSON":
        def generate_json_value(curr_depth):
            if curr_depth >= max_depth:
                return '"value"'  # Return simple value at max depth
                
            types = ['string', 'number', 'boolean'] if curr_depth == max_depth - 1 else ['string', 'number', 'boolean', 'array']
            choice = random.choice(types)
            
            if choice == 'string':
                return f'"{random.choice(string.ascii_letters + string.digits)}"'
            elif choice == 'number':
                return str(random.randint(-100, 100))
            elif choice == 'boolean':
                return str(random.choice(['true', 'false'])).lower()
            elif choice == 'array':
                size = random.randint(0, 2)  # Reduced size
                values = [generate_json_value(curr_depth + 1) for _ in range(size)]
                return f'[{", ".join(values)}]'
                
        json_str = generate_json_value(depth)
        return f"'{json_str}'"
    elif data_type == "TINYINT" or data_type == "BOOL" or\
          data_type == "BOOLEAN" or data_type == "INT8" or data_type == "UINT8":
        return random.randint(-128, 127)
    elif data_type == "SMALLINT" or data_type == "INT16" or data_type == "UINT16":
        return random.randint(-32768, 32767)
    elif data_type == "MEDIUMINT" or data_type == "INT32" or data_type == "UINT32":
        return random.randint(-8388608, 8388607)
    elif data_type == "INT" or data_type == "INTEGER" or data_type == "UINTEGER" or\
          data_type == "INT32" or data_type == "UINT32":
        return random.randint(-2147483648, 2147483647)
    elif data_type == "BIGINT" or data_type == "HUGEINT" or\
          data_type == "UBIGINT" or data_type == "INT64" or data_type == "UINT64":
        return random.randint(-9223372036854775808, 9223372036854775807)
    elif data_type.startswith("INT128") or data_type.startswith("UINT128"):
        return random.randint(-170141183460469231731687303715884105728, 170141183460469231731687303715884105727)
    elif data_type.startswith("INT256") or data_type.startswith("UINT256"):
        return random.randint(-170141183460469231731687303715884105728, 170141183460469231731687303715884105727)
    elif "DECIMAL" in data_type or "DEC" in data_type:
        return round(random.uniform(-1e10, 1e10), 6)
    elif "FLOAT" in data_type or data_type == "REAL":
        value = round(random.uniform(-1e38, 1e38), 6)
        while value == float('inf') or value == float('-inf'):
            value = round(random.uniform(-1e38, 1e38), 6)
        return value
    elif data_type == "DOUBLE":
        value = round(random.uniform(-1e308, 1e308), 6)
        while value == float('inf') or value == float('-inf'):
            value = round(random.uniform(-1e30, 1e30), 6)
        return value
    elif data_type == "BIT":
        return random.randint(0, 1)
    elif data_type.startswith("VARBINARY") or data_type.startswith("BINARY"):
        if '(' in data_type and ')' in data_type:
            length = int(data_type[data_type.find('(')+1:data_type.find(')')])
        else:
            length = 10
        return "0x" + ''.join(random.choices('0123456789ABCDEF', k=length))
    elif data_type == "TINYBLOB" or data_type == "BLOB" or data_type == "MEDIUMBLOB" or data_type == "LONGBLOB":
        length = random.randint(1, 255)
        return "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=length)) + "'"
    elif data_type == "TINYTEXT" or data_type == "TEXT" or\
          data_type == "MEDIUMTEXT" or data_type == "LONGTEXT" or\
          data_type == "CLOB" or "STRING" in data_type:
        length = random.randint(1, 255)
        return "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=length)) + "'"
    elif data_type.startswith("VARCHAR") or data_type.startswith("CHAR") or data_type.startswith("FIXEDSTRING"):
        if '(' in data_type and ')' in data_type:
            length = int(data_type[data_type.find('(')+1:data_type.find(')')])
        else:
            length = 10
        return "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=length)) + "'"
    elif data_type.startswith("ENUM"):
        choices = data_type[data_type.find('(')+1:data_type.find(')')].split(',')
        choices = random.choice([choice.strip().strip("'") for choice in choices])
        return f"'{random.choice([choice for choice in choices])}'"
    elif data_type == "INET4" or data_type == "IPV4":
        return "'" + '.'.join(str(random.randint(0, 255)) for _ in range(4)) + "'"
    elif data_type == "INET6" or data_type == "IPV6":
        return "'" + ':'.join(''.join(random.choices('0123456789abcdef', k=4)) for _ in range(8)) + "'"
    elif data_type == "UUID":
        return "'" + str(uuid.uuid4()) + "'"
    elif data_type.startswith("DATETIME"):
        # DATETIME的范围为1000-01-01到9999-12-31
        return "'" + str(datetime.datetime(random.randint(1900, 2100), random.randint(1, 12), random.randint(1, 28),
                                           random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))) + "'"
    elif data_type.startswith("TIMESTAMP"):
        # TIMESTAMP的范围为1970-01-01到2038-01-19
        return "'" + str(datetime.datetime(random.randint(1970, 2035), random.randint(1, 12), random.randint(1, 28),
                                           random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))) + "'"
    elif data_type.startswith("DATE"):
        return "'" + str(datetime.date(random.randint(1900, 2100), random.randint(1, 12), random.randint(1, 28))) + "'"
    elif data_type.startswith("TIME"):
        return "'" + str(datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))) + "'"
    elif data_type == "YEAR":
        return str(random.randint(1900, 2100))
    elif data_type == "NULL":
        return 'NULL'
    elif data_type == "POINT" or data_type == "GEOMETRY":
        x = random.uniform(-180, 180)  # 经度范围
        y = random.uniform(-90, 90)    # 纬度范围
        return f"POINT({x} {y})"
    elif data_type == "LINESTRING":
        points = [(random.uniform(-180, 180), random.uniform(-90, 90)) for _ in range(3)]
        return f"LINESTRING({', '.join(f'{x} {y}' for x, y in points)})"
    elif data_type == "POLYGON":
        points = [(random.uniform(-180, 180), random.uniform(-90, 90)) for _ in range(4)]
        points.append(points[0])  # 闭合多边形
        return f"POLYGON(({', '.join(f'{x} {y}' for x, y in points)}))"
    elif data_type.startswith("INTERVAL DAY TO SECOND"):
        days = random.randint(-30, 30)  # Random number of days
        hours = random.randint(0, 23)    # Random number of hours
        minutes = random.randint(0, 59)  # Random number of minutes
        seconds = random.randint(0, 59)  # Random number of seconds
        return f"INTERVAL '{days} {hours}:{minutes}:{seconds}' DAY TO SECOND"  # Format the interval
    else:
        raise ValueError(f"Unsupported data type: {data_type}")