# Run EDC

启动数据库之后运行：

```
cd src
python main.py mysql
```

* 参数：目前支持 `mysql`，`mariadb`, `clickhouse`, `tidb`
* 输出文件存放在 `/edc/res/{db}`下，log 存放在 `/edc/log/{db}`下

# Add a New DBMS

* `src/config/conn.ini`配置参数
* `src/conn/`适配新的数据库连接
* `src/seed/{db}`下增加新的数据库需要测试的类型和操作
  * 注意 pred 下的谓词操作生成对应 expr 是硬编码在 `main.py`下的 `generate_expr`中的，增加新谓词需要增加代码
* `main.py`中工具函数涉及数据库内部语法检查是否要修改：
  * `get_derived_type`：查询数据库系统表获得数据库自动推断的 derived column 的数据类型
  * `construct_derived_table`：生成派生表，如果支持 `CREATE TABLE AS` 的话就调用 `_create_default_table`，否则可能需要自己实现
