param(
  [string]$TopicName = "telecom-realtime"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/5] Start Hadoop and Kafka services..."
docker compose up -d hadoop kafka

Write-Host "[2/5] Create Kafka topic: $TopicName ..."
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $TopicName --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

Write-Host "[3/5] Start both Flume agents..."
docker compose up -d flume-hdfs flume-kafka

Write-Host "[4/5] Run the data generator..."
docker compose run --rm generator

Write-Host "[5/5] Verification commands"
Write-Host "HDFS files: docker compose exec hadoop hdfs dfs -ls -R /flume/telecom"
Write-Host "If 'hdfs' is unavailable, use: docker compose exec hadoop /home/hduser/hadoop-3.3.3/bin/hdfs dfs -ls -R /flume/telecom"
Write-Host "Kafka consume: docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $TopicName --from-beginning --max-messages 8"
Write-Host "Spark read: docker compose run --rm spark /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/read_hdfs.py"


