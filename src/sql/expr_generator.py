import uuid
import random
import string
import datetime


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
    

class ExprGenerator:
    def __init__(self, target: str):
        self.database = target

    def generate_expr_on_column(self, table: str, column_names: list, column_types: list, depth: int) -> str:       
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
                return str(self.generate_single_constant(random.choice(column_types))) if column_types else self.generate_single_constant()
            elif random.randint(0, 1) == 0:
                return str(self.generate_expr_constant(random.choice(column_types))) if column_types else self.generate_expr_constant()
            else:
                return 'NULL'
        elif op == 'COLUMN':
            return random.choice(column_names)
        elif op == 'CASE':
            return f'(CASE WHEN {self.generate_expr_on_column(table, column_names, column_types, depth - 1)} THEN ({self.generate_expr_constant(random.choice(column_types))}) ELSE ({self.generate_expr_constant(random.choice(column_types))}) END)'
        elif op == 'SUBQUERY':
            aggs = ['IN', 'NOT IN']
            agg = random.choice(aggs)
            return f'{self.generate_expr_on_column(table, column_names, column_types, depth - 1)} {agg} (SELECT {random.choice(column_names)} FROM {table} WHERE {self.generate_expr_on_column(table, column_names, column_types, depth - 1)})'
        else:
            return f'({self.generate_expr_on_column(table, column_names, column_types, depth - 1)} {op} {self.generate_expr_on_column(table, column_names, column_types, depth - 1)})'


    def generate_expr_constant(self, ori_typ: str = None, dest_typ: str = None) -> Constant:
        expr_op = ['+', '*', '/', '<<', '>>', '&', '|', '^']
        op = random.choice(expr_op)
        size = random.randint(1, 1)
        args = []
        for i in range(size):
            # 不加dest_typ是为了避免表达式内部的CAST
            args.append(self.generate_single_constant(ori_typ))
        expr = op.join([str(arg) for arg in args])
        return Constant(f'({expr})', ori_typ, dest_typ)


    def generate_single_constant(self, ori_typ: str = None, dest_typ: str = None) -> Constant:
        value = self.generate_random_value(ori_typ)
        return Constant(value, ori_typ, dest_typ)


    def generate_random_value(self, data_type, depth=0, max_depth=3):
        if depth >= max_depth:
            return '0'  
        
        data_type = data_type.upper() if data_type else 'INT'

        if data_type.startswith('ARRAY'):
            return self._generate_array_value(data_type, depth, max_depth)
        elif data_type.startswith('TUPLE'):
            return self._generate_tuple_value(data_type, depth, max_depth)
        elif data_type.startswith('MAP'):
            return self._generate_map_value(data_type, depth, max_depth)
        elif data_type == "JSON":
            return self._generate_json_value(depth, max_depth)
        elif data_type in ["TINYINT", "BOOL", "BOOLEAN", "INT8", "UINT8"]:
            return random.randint(-128, 127)
        elif data_type in ["SMALLINT", "INT16", "UINT16"]:
            return random.randint(-32768, 32767)
        elif data_type in ["MEDIUMINT", "INT32", "UINT32"]:
            return random.randint(-8388608, 8388607)
        elif data_type in ["INT", "INTEGER", "UINTEGER", "INT32", "UINT32"]:
            return random.randint(-2147483648, 2147483647)
        elif data_type in ["BIGINT", "HUGEINT", "UBIGINT", "INT64", "UINT64"]:
            return random.randint(-9223372036854775808, 9223372036854775807)
        elif data_type.startswith("DECIMAL") or "DEC" in data_type:
            return round(random.uniform(-1e10, 1e10), 6)
        elif data_type in ["FLOAT", "REAL"]:
            return self._generate_float_value()
        elif data_type == "DOUBLE":
            return self._generate_double_value()
        elif data_type == "BIT":
            return random.randint(0, 1)
        elif data_type.startswith("VARBINARY") or data_type.startswith("BINARY"):
            return self._generate_binary_value(data_type)
        elif data_type in ["TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"]:
            return self._generate_blob_value()
        elif data_type in ["TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "CLOB"]:
            return self._generate_text_value()
        elif data_type.startswith("VARCHAR") or data_type.startswith("CHAR") or data_type.startswith("FIXEDSTRING"):
            return self._generate_string_value(data_type)
        elif data_type.startswith("ENUM"):
            return self._generate_enum_value(data_type)
        elif data_type in ["INET4", "IPV4"]:
            return self._generate_ipv4_value()
        elif data_type in ["INET6", "IPV6"]:
            return self._generate_ipv6_value()
        elif data_type == "UUID":
            return f"'{str(uuid.uuid4())}'"
        elif data_type.startswith("DATETIME"):
            return self._generate_datetime_value()
        elif data_type.startswith("TIMESTAMP"):
            return self._generate_timestamp_value()
        elif data_type.startswith("DATE"):
            return self._generate_date_value()
        elif data_type.startswith("TIME"):
            return self._generate_time_value()
        elif data_type == "YEAR":
            return str(random.randint(1900, 2100))
        elif data_type.startswith("INTERVAL DAY TO SECOND"):
            return self._generate_interval_value()
        elif data_type == "GEOMETRY":
            data_type = random.choice(["POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"])
            return self._generate_spatial_value(data_type)
        elif data_type in ["POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMCOLLECTION", "GEOMETRYCOLLECTION"]:
            return self._generate_spatial_value(data_type)
        else:
            raise ValueError(f"Unsupported data type in {self.database}: {data_type}")

    def _generate_array_value(self, data_type, depth, max_depth):
        inner_type = data_type[data_type.find('(')+1:data_type.find(')')]
        size = random.randint(1, 2)  
        values = [str(self.generate_random_value(inner_type, depth + 1, max_depth)) for _ in range(size)]
        return f"[{', '.join(values)}]"

    def _generate_tuple_value(self, data_type, depth, max_depth):
        if '(' in data_type and ')' in data_type:
            inner_types = data_type[data_type.find('(')+1:data_type.find(')')].split(',')
            values = [str(self.generate_random_value(typ.strip(), depth + 1, max_depth)) for typ in inner_types]
            return f"({', '.join(values)})"
        return "(0, 0)"

    def _generate_map_value(self, data_type, depth, max_depth):
        if '(' in data_type and ')' in data_type:
            type_part = data_type[data_type.find('(')+1:data_type.find(')')]
            key_type, value_type = [t.strip() for t in type_part.split(',', 1)]
            size = random.randint(1, 2)  
            pairs = [f"{self.generate_random_value(key_type, depth + 1, max_depth)}: {self.generate_random_value(value_type, depth + 1, max_depth)}" for _ in range(size)]
            return f"{{{', '.join(pairs)}}}"
        return "{}"

    def _generate_json_value(self, curr_depth, max_depth):
        if curr_depth >= max_depth:
            return '"value"'  
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
            values = [self._generate_json_value(curr_depth + 1, max_depth) for _ in range(size)]
            return f'[{", ".join(values)}]'

    def _generate_float_value(self):
        value = round(random.uniform(-1e38, 1e38), 6)
        while value in [float('inf'), float('-inf')]:
            value = round(random.uniform(-1e30, 1e30), 6)
        return value

    def _generate_double_value(self):
        value = round(random.uniform(-1e308, 1e308), 6)
        while value in [float('inf'), float('-inf')]:
            value = round(random.uniform(-1e30, 1e30), 6)
        return value

    def _generate_binary_value(self, data_type):
        if '(' in data_type and ')' in data_type:
            length = int(data_type[data_type.find('(')+1:data_type.find(')')])
        else:
            length = 10
        return "0x" + ''.join(random.choices('0123456789ABCDEF', k=length))

    def _generate_blob_value(self):
        length = random.randint(1, 50)
        return "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=length)) + "'"

    def _generate_text_value(self):
        length = random.randint(1, 50)
        return "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=length)) + "'"

    def _generate_string_value(self, data_type):
        if '(' in data_type and ')' in data_type:
            length = int(data_type[data_type.find('(')+1:data_type.find(')')])
        else:
            length = 10
        return "'" + ''.join(random.choices(string.ascii_letters + string.digits, k=length)) + "'"

    def _generate_enum_value(self, data_type):
        choices = data_type[data_type.find('(')+1:data_type.find(')')].split(',')
        choices = random.choice([choice.strip().strip("'") for choice in choices])
        return f"'{random.choice([choice for choice in choices])}'"

    def _generate_ipv4_value(self):
        return "'" + '.'.join(str(random.randint(0, 255)) for _ in range(4)) + "'"

    def _generate_ipv6_value(self):
        return "'" + ':'.join(''.join(random.choices('0123456789abcdef', k=4)) for _ in range(8)) + "'"

    def _generate_datetime_value(self):
        return "'" + str(datetime.datetime(random.randint(1900, 2100), random.randint(1, 12), random.randint(1, 28),
                                            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))) + "'"

    def _generate_timestamp_value(self):
        return "'" + str(datetime.datetime(random.randint(1970, 2035), random.randint(1, 12), random.randint(1, 28),
                                            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))) + "'"

    def _generate_date_value(self):
        return "'" + str(datetime.date(random.randint(1900, 2100), random.randint(1, 12), random.randint(1, 28))) + "'"

    def _generate_time_value(self):
        return "'" + str(datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))) + "'"

    def _generate_interval_value(self):
        days = random.randint(-30, 30)  # Random number of days
        hours = random.randint(0, 23)    # Random number of hours
        minutes = random.randint(0, 59)  # Random number of minutes
        seconds = random.randint(0, 59)  # Random number of seconds
        return f"INTERVAL '{days} {hours}:{minutes}:{seconds}' DAY TO SECOND"  # Format the interval 

    def _generate_spatial_value(self, data_type):
        if self.database in ['mysql', 'oceanbase', 'percona'] :
            return self._generate_mysql_spatial_value(data_type)
        elif self.database == 'mariadb':
            return self._generate_mariadb_spatial_value(data_type)
        elif self.database == 'clickhouse':
            return self._generate_click_spatial_value(data_type)
        else:
            raise ValueError(f"Unsupported spatial type in {self.database}: {data_type}")

    def _generate_mysql_spatial_value(self, data_type):
        if data_type == "POINT":
            x = round(random.uniform(-180, 180), 6)
            y = round(random.uniform(-90, 90), 6)
            return f"ST_GeomFromText('POINT({x} {y})')"
        elif data_type == "LINESTRING":
            points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
            return f"ST_GeomFromText('LINESTRING({', '.join(points)})')"
        elif data_type == "POLYGON":
            points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(3, 6))]
            points.append(points[0])  # Ensure the polygon is closed
            return f"ST_GeomFromText('POLYGON(({', '.join(points)}))')"
        elif data_type == "MULTIPOINT":
            points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
            return f"ST_GeomFromText('MULTIPOINT({', '.join(points)})')"
        elif data_type == "MULTILINESTRING":
            lines = []
            for _ in range(random.randint(2, 3)):
                points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
                lines.append(f"({', '.join(points)})")
            return f"ST_GeomFromText('MULTILINESTRING({', '.join(lines)})')"
        elif data_type == 'MULTIPOLYGON':
            polygons = []
            for _ in range(random.randint(2, 3)):
                points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(3, 6))]
                points.append(points[0])  # Close the polygon
                polygons.append(f"(({', '.join(points)}))")
            return f"ST_GeomFromText('MULTIPOLYGON({', '.join(polygons)})')"
        elif data_type in ["GEOMETRYCOLLECTION", "GEOMCOLLECTION"]:
            geometries = []
            for _ in range(random.randint(2, 4)):
                geometry_type = random.choice(["POINT", "LINESTRING", "POLYGON"])
                if geometry_type == "POINT":
                    geometries.append(f"POINT({round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)})")
                elif geometry_type == "LINESTRING":
                    points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
                    geometries.append(f"LINESTRING({', '.join(points)})")
                elif geometry_type == "POLYGON":
                    points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(3, 6))]
                    points.append(points[0])  # Close the polygon
                    geometries.append(f"POLYGON(({', '.join(points)}))")
            return f"ST_GeomFromText('GEOMETRYCOLLECTION({', '.join(geometries)})')"
    
    def _generate_mariadb_spatial_value(self, data_type):
        if data_type == "POINT":
            x = round(random.uniform(-180, 180), 6)
            y = round(random.uniform(-90, 90), 6)
            return f"PointFromText('POINT({x} {y})')"
        elif data_type == "LINESTRING":
            points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
            return f"LineFromText('LINESTRING({', '.join(points)})')"
        elif data_type == "POLYGON":
            points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(3, 6))]
            points.append(points[0])  # Ensure the polygon is closed
            return f"PolygonFromText('POLYGON(({', '.join(points)}))')"
        elif data_type == "MULTIPOINT":
            points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
            return f"MultiPointFromText('MULTIPOINT({', '.join(points)})')"
        elif data_type == "MULTILINESTRING":
            lines = []
            for _ in range(random.randint(2, 3)):
                points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
                lines.append(f"({', '.join(points)})")
            return f"MultiLineStringFromText('MULTILINESTRING({', '.join(lines)})')"
        elif data_type == 'MULTIPOLYGON':
            polygons = []
            for _ in range(random.randint(2, 3)):
                points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(3, 6))]
                points.append(points[0])  # Close the polygon
                polygons.append(f"(({', '.join(points)}))")
            return f"MultiPolygonFromText('MULTIPOLYGON({', '.join(polygons)})')"
        elif data_type == "GEOMETRYCOLLECTION":
            geometries = []
            for _ in range(random.randint(2, 4)):
                geometry_type = random.choice(["POINT", "LINESTRING", "POLYGON"])
                if geometry_type == "POINT":
                    geometries.append(f"POINT({round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)})")
                elif geometry_type == "LINESTRING":
                    points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(2, 5))]
                    geometries.append(f"LINESTRING({', '.join(points)})")
                elif geometry_type == "POLYGON":
                    points = [f"{round(random.uniform(-180, 180), 6)} {round(random.uniform(-90, 90), 6)}" for _ in range(random.randint(3, 6))]
                    points.append(points[0])  # Close the polygon
                    geometries.append(f"POLYGON(({', '.join(points)}))")
            return f"GeomCollFromText('GEOMETRYCOLLECTION({', '.join(geometries)})')"
        
    def _generate_click_spatial_value(self, data_type):
        if data_type == "POINT":
            x = random.uniform(-100, 100)
            y = random.uniform(-100, 100)
            return f"({x:.6f}, {y:.6f})"
        elif data_type == "RING":
            x1, y1 = random.uniform(-100, 100), random.uniform(-100, 100)
            x2, y2 = random.uniform(x1, x1 + 50), random.uniform(y1, y1 + 50)
            ring = [(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)]
            return f"[{', '.join(f'({x:.6f}, {y:.6f})' for x, y in ring)}]"
        elif data_type == "LINESTRING":
            num_points = random.randint(3, 10)
            points = [(random.uniform(-100, 100), random.uniform(-100, 100)) for _ in range(num_points)]
            return f"[{', '.join(f'({x:.6f}, {y:.6f})' for x, y in points)}]"
        elif data_type == "MULTILINESTRING":
            num_lines = random.randint(2, 5)
            lines = [
                [(random.uniform(-100, 100), random.uniform(-100, 100)) for _ in range(random.randint(3, 10))]
                for _ in range(num_lines)
            ]
            return f"[{', '.join(f'[{", ".join(f"({x:.6f}, {y:.6f})" for x, y in line)}]' for line in lines)}]"
        elif data_type == "POLYGON":
            def random_ring():
                x1, y1 = random.uniform(-100, 100), random.uniform(-100, 100)
                x2, y2 = random.uniform(x1, x1 + 50), random.uniform(y1, y1 + 50)
                return [(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)]
            outer_ring = random_ring()
            if random.choice([True, False]):  # 随机决定是否有内边界
                inner_ring = random_ring()
                return f"[[{', '.join(f'({x:.6f}, {y:.6f})' for x, y in outer_ring)}], [{', '.join(f'({x:.6f}, {y:.6f})' for x, y in inner_ring)}]]"
            return f"[[{', '.join(f'({x:.6f}, {y:.6f})' for x, y in outer_ring)}]]"
        elif data_type == "MULTIPOLYGON":
            def random_polygon():
                x1, y1 = random.uniform(-100, 100), random.uniform(-100, 100)
                x2, y2 = random.uniform(x1, x1 + 50), random.uniform(y1, y1 + 50)
                return [[(x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)]]
            num_polygons = random.randint(2, 5)
            polygons = [random_polygon() for _ in range(num_polygons)]
            return f"[{', '.join(f'[[{", ".join(f"({x:.6f}, {y:.6f})" for x, y in polygon[0])}]]' for polygon in polygons)}]"
        else:
            raise ValueError(f"Unsupported spatial data type: {data_type}")
