# 作业一补充内容

## 1. Flume 写入 HDFS，用于离线任务

### 基础数据字段

| 字段 | 含义 |
| --- | --- |
| `imsi` | 用户 imsi |
| `laccell` | 基站编码 |
| `latitude` | 纬度 |
| `longitude` | 经度 |
| `procedureStartTime` | 进入该基站的时间 |

### `flume.conf` 配置文件内容

文件位置：`flume/conf/flume-hdfs.conf`

```properties
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /data/spool/hdfs
a1.sources.r1.fileHeader = true
a1.sources.r1.basenameHeader = true
a1.sources.r1.deletePolicy = immediate
a1.sources.r1.inputCharset = UTF-8

a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /data/flume-hdfs/checkpoint
a1.channels.c1.dataDirs = /data/flume-hdfs/data

a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = hdfs://hadoop:9000/flume/telecom/%Y/%m/%d/%H
a1.sinks.k1.hdfs.filePrefix = telecom
a1.sinks.k1.hdfs.fileType = DataStream
a1.sinks.k1.hdfs.writeFormat = Text
a1.sinks.k1.hdfs.useLocalTimeStamp = true
a1.sinks.k1.hdfs.rollInterval = 0
a1.sinks.k1.hdfs.rollSize = 0
a1.sinks.k1.hdfs.rollCount = 10
a1.sinks.k1.hdfs.idleTimeout = 5
a1.sinks.k1.hdfs.callTimeout = 600000

a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

### 成功写入 HDFS 的截图

请将命令输出截图粘贴到这里：

```powershell
docker compose exec hadoop hdfs dfs -ls -R /flume/telecom
docker compose exec hadoop hdfs dfs -cat /flume/telecom/*/*/*/*/*
```

如果上述命令在你的环境中提示 `hdfs` 命令不存在，可改用下面这组等价命令：

```powershell
docker compose exec hadoop /home/hduser/hadoop-3.3.3/bin/hdfs dfs -ls -R /flume/telecom
docker compose exec hadoop /home/hduser/hadoop-3.3.3/bin/hdfs dfs -cat /flume/telecom/*/*/*/*/*
```

建议粘贴文件：`artifacts/hdfs-success.png`

### Spark 读取 HDFS 文件代码

文件位置：`spark/read_hdfs.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

schema = StructType([
    StructField("imsi", StringType(), False),
    StructField("laccell", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("procedureStartTime", LongType(), False),
])

spark = (
    SparkSession.builder
    .appName("ReadTelecomHdfs")
    .master("local[*]")
    .getOrCreate()
)

input_path = "hdfs://hadoop:9000/flume/telecom/*/*/*/*"
df = (
    spark.read
    .option("delimiter", "|")
    .schema(schema)
    .csv(input_path)
)

df.printSchema()
df.show(10, truncate=False)
spark.stop()
```

运行命令：

```powershell
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/read_hdfs.py
```

## 2. Flume 写入 Kafka，用于实时任务

### `flume.conf` 配置文件内容

文件位置：`flume/conf/flume-kafka.conf`

```properties
a1.sources = r1
a1.channels = c1
a1.sinks = k1

a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /data/spool/kafka
a1.sources.r1.fileHeader = true
a1.sources.r1.basenameHeader = true
a1.sources.r1.deletePolicy = immediate
a1.sources.r1.inputCharset = UTF-8

a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.kafka.bootstrap.servers = kafka:9092
a1.sinks.k1.kafka.topic = telecom-realtime
a1.sinks.k1.kafka.flumeBatchSize = 20
a1.sinks.k1.kafka.producer.acks = 1

a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

### 成功写入 Kafka 且可正常消费的截图

请将以下命令输出截图粘贴到这里：

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic telecom-realtime --bootstrap-server kafka:9092
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic telecom-realtime --from-beginning --max-messages 8
```

建议粘贴文件：

- `artifacts/kafka-topic.png`
- `artifacts/kafka-consume.png`

### Kafka topic 创建、生产者和消费者命令

#### 创建 topic

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic telecom-realtime --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

#### 生产者命令

```powershell
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic telecom-realtime
```

#### 消费者命令

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic telecom-realtime --from-beginning
```

## 3. 项目运行说明

### 一键执行脚本

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-demo.ps1
```

### 脚本执行完成后建议截图的命令

```powershell
docker compose exec hadoop hdfs dfs -ls -R /flume/telecom
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic telecom-realtime --bootstrap-server kafka:9092
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic telecom-realtime --from-beginning --max-messages 8
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/read_hdfs.py
```

如果第一条 HDFS 命令无效，可替换为：

```powershell
docker compose exec hadoop /home/hduser/hadoop-3.3.3/bin/hdfs dfs -ls -R /flume/telecom
```

