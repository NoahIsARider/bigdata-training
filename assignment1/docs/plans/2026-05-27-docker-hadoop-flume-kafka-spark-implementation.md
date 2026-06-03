# Docker Hadoop Flume Kafka Spark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Docker-based demo project that generates telecom base data, writes one copy to HDFS for offline processing, writes another copy to Kafka for real-time processing, and includes Spark code plus assignment-ready documentation.

**Architecture:** `docker-compose.yml` starts Hadoop HDFS, Kafka, two Flume agents, and a lightweight data generator. The generator writes the same pipe-delimited records into two spool directories so one Flume config can land files in HDFS while the other publishes messages to Kafka. Spark reads the HDFS data with an explicit schema.

**Tech Stack:** Docker Compose, Hadoop HDFS, Apache Flume, Apache Kafka, PySpark, Python

---

### Task 1: Scaffold Repository

**Files:**
- Create: `docker-compose.yml`
- Create: `README.md`
- Create: `scripts/run-demo.ps1`
- Create: `artifacts/.gitkeep`

**Step 1: Create project directories**

Run: `New-Item -ItemType Directory -Force docs/plans, flume/conf, generator, spark, scripts, artifacts`
Expected: directories exist

**Step 2: Add compose-level service definitions**

Create a compose file for `hadoop`, `kafka`, `flume-hdfs`, `flume-kafka`, and `generator`.

**Step 3: Add a runnable demo script**

Create a PowerShell script that starts infra, creates the Kafka topic, starts Flume, runs the generator, and prints verification commands.

**Step 4: Verify YAML syntax**

Run: `docker compose config`
Expected: merged compose output without errors

### Task 2: Add Flume Containers And Configs

**Files:**
- Create: `flume/Dockerfile`
- Create: `flume/start-flume.sh`
- Create: `flume/conf/flume-hdfs.conf`
- Create: `flume/conf/flume-kafka.conf`

**Step 1: Build a Flume image**

Install Apache Flume in a small Java 8 image and expose an entrypoint script that selects the requested config file.

**Step 2: Write HDFS Flume config**

Use a `spooldir` source, durable channel, and HDFS sink with a path under `/flume/telecom/%Y/%m/%d/%H`.

**Step 3: Write Kafka Flume config**

Use a `spooldir` source, memory channel, and Kafka sink targeting topic `telecom-realtime`.

**Step 4: Verify container start command**

Run: `docker compose up -d flume-hdfs flume-kafka`
Expected: both agents stay healthy in logs

### Task 3: Add Data Generator And Spark Reader

**Files:**
- Create: `generator/Dockerfile`
- Create: `generator/generate_data.py`
- Create: `spark/read_hdfs.py`

**Step 1: Generate assignment-shaped sample data**

Produce records with columns `imsi|laccell|latitude|longitude|procedureStartTime`.

**Step 2: Duplicate output into both spool directories**

Write one batch file to `/data/spool/hdfs` and one batch file to `/data/spool/kafka`.

**Step 3: Read HDFS data in Spark**

Use an explicit schema and `option("delimiter", "|")` so the file loads as typed columns.

**Step 4: Verify output shape**

Run: `docker compose run --rm spark /opt/spark/bin/spark-submit /opt/spark/jobs/read_hdfs.py`
Expected: schema and sample rows print to stdout

### Task 4: Write Assignment Documentation

**Files:**
- Create: `assignment-answer.md`
- Modify: `README.md`

**Step 1: Paste Flume config contents**

Include full HDFS and Kafka config blocks in the assignment document.

**Step 2: Add screenshot placeholders**

Reserve sections for HDFS success, Kafka topic creation, and Kafka consumer output screenshots.

**Step 3: Add exact Kafka commands**

Document topic creation plus console producer/consumer commands that work inside the Docker Kafka container.

**Step 4: Add execution guide**

Explain how to start the demo and where to find verification output for screenshots.

### Task 5: End-To-End Verification

**Files:**
- Modify: `README.md` if verification reveals corrections

**Step 1: Start infrastructure**

Run: `docker compose up -d hadoop kafka`
Expected: containers become healthy or stay running

**Step 2: Create Kafka topic**

Run: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic telecom-realtime --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1`
Expected: topic creation succeeds or reports it already exists

**Step 3: Start Flume and generator**

Run: `docker compose up -d flume-hdfs flume-kafka`
Run: `docker compose run --rm generator`
Expected: batch files are consumed

**Step 4: Verify HDFS and Kafka**

Run: `docker compose exec hadoop hdfs dfs -ls -R /flume/telecom`
Run: `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic telecom-realtime --from-beginning --max-messages 8`
Expected: HDFS files exist and Kafka consumer prints generated lines

**Step 5: Commit**

```bash
git add .
git commit -m "feat: add dockerized hdfs kafka flume spark demo"
```


