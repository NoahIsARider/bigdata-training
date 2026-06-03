# Docker Hadoop Assignment Demo

本项目使用 `Docker Compose` 在当前目录启动一套最小可运行的大数据作业环境，完成以下闭环：

- 生成基础数据：`imsi|laccell|latitude|longitude|procedureStartTime`
- 使用 Flume 将数据写入 HDFS，支撑离线任务
- 使用 Flume 将数据写入 Kafka，支撑实时任务
- 使用 Spark 读取 HDFS 中已经落盘的数据

## 目录结构

```text
.
|-- assignment-answer.md
|-- docker-compose.yml
|-- flume
|   |-- Dockerfile
|   |-- conf
|   |   |-- flume-hdfs.conf
|   |   `-- flume-kafka.conf
|   `-- start-flume.sh
|-- generator
|   |-- Dockerfile
|   `-- generate_data.py
|-- scripts
|   `-- run-demo.ps1
`-- spark
    `-- read_hdfs.py
```

## 数据格式

每条样例数据均为一行文本，字段分隔符为 `|`：

```text
imsi|laccell|latitude|longitude|procedureStartTime
748c35aa-854c-4efa-ab52-f90e09f6d8eb|20001 00001|23.7367148|40.3515728|1718175430523
ddb32488-2980-4b87-93d5-a16b06a154f3|50001 00001|6.82555455|16.2644781|1718175431526
0f62ea73-e7b1-4e5d-ab84-840680981ef4|70001 00001|59.0283434|33.9339193|1718175432541
```

## 一键演示

在当前目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-demo.ps1
```

该脚本会依次完成：

- 启动 `hadoop`、`kafka`
- 创建 Kafka topic `telecom-realtime`
- 启动 `flume-hdfs`、`flume-kafka`
- 运行数据生成器，写入两份相同数据到两个 `spooldir`
- 输出 HDFS、Kafka、Spark 的验证命令

## 手动验证命令

### 1. 查看 HDFS 是否已写入

```powershell
docker compose exec hadoop hdfs dfs -ls -R /flume/telecom
docker compose exec hadoop hdfs dfs -cat /flume/telecom/*/*/*/*/*
```

如果你的环境里提示 `hdfs` 命令不存在，可改用：

```powershell
docker compose exec hadoop /home/hduser/hadoop-3.3.3/bin/hdfs dfs -ls -R /flume/telecom
docker compose exec hadoop /home/hduser/hadoop-3.3.3/bin/hdfs dfs -cat /flume/telecom/*/*/*/*/*
```

### 2. 查看 Kafka topic

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic telecom-realtime --bootstrap-server kafka:9092
```

### 3. 消费 Kafka 数据

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic telecom-realtime --from-beginning --max-messages 8
```

### 4. 运行 Spark 读取 HDFS

```powershell
docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/read_hdfs.py
```

## Kafka 命令

### 创建 topic

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic telecom-realtime --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

### 控制台生产者

```powershell
docker compose exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic telecom-realtime
```

### 控制台消费者

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic telecom-realtime --from-beginning
```

## 截图建议

为了方便完成作业截图，建议保留以下命令输出：

- `docker compose exec hadoop hdfs dfs -ls -R /flume/telecom`
- `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic telecom-realtime --bootstrap-server kafka:9092`
- `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic telecom-realtime --from-beginning --max-messages 8`
- `docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/read_hdfs.py`

## 清理环境

```powershell
docker compose down -v
```


