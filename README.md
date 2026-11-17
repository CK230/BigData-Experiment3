# 实验三 PageRank 算法实现

本仓库包含 HBase 操作、Hadoop MapReduce PageRank 和 Spark RDD PageRank，以及样例数据和运行命令。HBase 样例结果按原始数据和操作顺序推演，PageRank 样例结果按迭代公式计算。

## 实验环境

建议使用 Linux、JDK 8（8u371 或更新）、Maven 3.6.3 或更新版本、Hadoop 3.3.6、HBase 2.5.11、Spark 3.5.7 和 Python 3.10。运行前启动教学环境中的 HDFS、YARN 和 HBase，并在终端配置各组件的命令路径。以下命令从仓库根目录执行。

## HBase

`hbase/data` 下存放指导书中的 3 条学生记录、3 条课程记录和 6 条选课记录。Java 和 Shell 使用相同的文件。

| 表 | 行键 | 列族及字段 | 样例行数 |
| --- | --- | --- | ---: |
| `experiment3:Student` | 姓名 `S_Name` | `Info` 下存姓名、学号、性别和年龄 | 3 |
| `experiment3:Course` | 课程号 `C_No` | `Info` 下存课程号、课程名和学分 | 3 |
| `experiment3:SC` | `SC_Sno#SC_Cno` | `Info` 下存学号和课程号，`Score:Value` 存成绩 | 6 |

Mary 和 Lisi 都使用学号 `2015003`，按姓名建行可同时保留两人的记录。`2015002` 的选课记录原样存入 `SC`。`2015003` 的选课成绩归属需要更多信息才能确认。

### HBase Shell

```bash
hbase shell -n < hbase/lab.hbase
```

脚本创建 `experiment3` 命名空间，导入三张表，然后执行 `list`、`scan` 和 `count`。`StudentDemo` 用同一份学生数据初始化，脚本在这张表上演示增删列族、增删单元格、修改年龄、删除行和 `truncate`。重复执行会重建脚本涉及的同名实验表。

### Java 客户端

将 `HBASE_CONF_DIR` 指向存放 `hbase-site.xml` 的配置目录。以下命令假定已设置 `HBASE_HOME`。

```bash
export HBASE_CONF_DIR="$HBASE_HOME/conf"
(
  cd hbase &&
  mvn compile exec:java -Dexec.args='load data' &&
  mvn compile exec:java -Dexec.args='demo data'
)
```

`load` 按要求重建三张同名实验表并列出记录与行数。`demo` 重建 `StudentDemo`，提示输入 Lisi 的新年龄时可以输入 `25`。`createTable` 从字段名提取列族，`addRecord` 和 `modifyData` 接受 `列族:列限定符`，`scanColumn` 可读取整个列族或指定单列。Java 按列扫描逐行显示缺失值 `null`，Shell 的过滤扫描只显示匹配的单元格。

| `StudentDemo` 的操作 | 样例结果 |
| --- | --- |
| 插入 `Zhangsan` 的 `Demo:Note` | Zhangsan 的值为 `example`，Lisi 和 Mary 的值为 `null` |
| 删除 `Demo:Note`，随后删除 `Demo` 列族 | 删除单元格后，Java 按单列扫描的三个值都为 `null` |
| 修改 Lisi 的 `Info:S_Age` | 输入 `25` 时，Lisi 的年龄从 `24` 改为 `25` |
| 删除 Mary 的记录 | 3 行变为 2 行 |
| 清空 `StudentDemo` | 2 行变为 0 行 |

## PageRank

两份程序共用 `data/webgraph.tsv`。每行用制表符分隔网页名和邻接列表，出链之间用英文逗号分隔。每个网页提供一条记录，每条记录至少有一个出链，所有链接目标都在文件中定义。三个网页的邻接关系为 A 指向 B 和 C、B 指向 C、C 指向 A。

初始分数为 1.0，阻尼系数为 0.85。每轮由网页按出度平分上一轮分数，接收方以 `0.15 + 0.85 × 入链贡献之和` 更新分数。MapReduce 每轮提交一个任务，前九轮保留邻接信息供下一轮读取。Spark 使用 RDD 关联邻接表和分数，并按目标网页汇总贡献。两份程序都执行 10 轮，在输出时保留 10 位小数。

### 上传共用数据到 HDFS

```bash
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
LAB_HOME="/user/$USER/experiment3"
hdfs dfs -mkdir -p "$LAB_HOME"
hdfs dfs -put -f data/webgraph.tsv "$LAB_HOME/webgraph.tsv"
```

### Hadoop MapReduce

```bash
mvn -f mapreduce/pom.xml package
hadoop jar mapreduce/target/pagerank-mapreduce-1.0.0.jar \
  experiment3.pagerank.PageRank "$LAB_HOME/webgraph.tsv" "$LAB_HOME/mapreduce"
hdfs dfs -cat "$LAB_HOME/mapreduce/result/part-r-*"
```

### Spark RDD

```bash
spark-submit --master yarn --deploy-mode client spark/pagerank.py \
  "$LAB_HOME/webgraph.tsv" "$LAB_HOME/spark"
hdfs dfs -cat "$LAB_HOME/spark/part-*"
```

重跑时给程序指定一个新输出目录，也要相应修改查看结果的路径。MapReduce 第 1 至 9 轮的数据位于 `round-01` 至 `round-09`，最终结果位于 `result`。Spark 直接将最终结果写入指定目录。

### 实验结果

以下为三节点样例第 10 轮的理论计算值。

```text
(A,1.1667391764)
(B,0.6432494118)
(C,1.1900114118)
```

第一轮 C 从 A 获得 0.5、从 B 获得 1.0，代入公式得 `PR(C) = 0.15 + 0.85 × (0.5 + 1.0) = 1.425`。第 10 轮排名为 `C > A > B`，分数之和约为 3。
