# 作业二补充内容

## 案例一、数据开发-区域人员明细汇聚

### 1、准备工作

建表语句：

```hbase
create_namespace 'WZMF'
create 'WZMF:TO_MI_EVNT_NS_CELL_IMSI', 'crowds'
create 'WZMF:TO_H_EVNT_NS_AREA_IMSI', 'crowds'
```

在当前独立作业环境中，建表可直接执行：

```powershell
cd .\assignment2
$cmds = @(
  "create_namespace 'WZMF'",
  "create 'WZMF:TO_MI_EVNT_NS_CELL_IMSI','crowds'",
  "create 'WZMF:TO_H_EVNT_NS_AREA_IMSI','crowds'"
)
($cmds -join "`n") | docker compose exec -T hbase hbase shell -n
```

启动独立作业环境：

```powershell
cd .\assignment2
docker compose up -d hbase
```

插入数据：

- 题目要求中说明，源表模拟导入程序位于 `hbaseProcess` 代码包。
- 当前实际使用的模拟导入主类为 [imitateDataSource.scala](file:///d:/Document%20Files/Schoolwork/%E5%A4%A7%E4%B8%89%E4%B8%8B/%E6%95%B0%E7%A7%91%E5%AD%A6%E4%B8%8E%E6%99%BA%E8%83%BD%E8%BD%AF%E4%BB%B6%E9%A1%B9%E7%9B%AE%E5%AE%9E%E8%AE%AD/%E4%BD%9C%E4%B8%9A%E4%B8%80/docker-hadoop/assignment2/hbaseProcess/src/main/scala/org/example/dataProcess/imitateDataSource.scala)

执行模拟导数命令：

```powershell
cd .\assignment2
docker compose run --rm hbaseprocess sh -lc 'mvn -U -DskipTests compile exec:java -Dexec.mainClass=org.example.dataProcess.imitateDataSource'
```

建议保留运行过程截图：`artifacts/case1-seed-run.png`

源表查询截图命令如下：

```powershell
@("scan 'WZMF:TO_MI_EVNT_NS_CELL_IMSI', {LIMIT => 20}") | docker compose exec -T hbase hbase shell -n
```

建议截图文件：`artifacts/case1-source-scan.png`

### 2、题目介绍

请将 `WZMF:TO_MI_EVNT_NS_CELL_IMSI` 中的数据按区域、小时汇聚到 `WZMF:TO_H_EVNT_NS_AREA_IMSI` 中，区域与基站关系如下：

- 区域 1-id：`255027778846457856`
  - `310000_6254_140854179`
  - `310000_6254_140854180`
  - `310000_6254_140854181`
- 区域 2-id：`255027778846457857`
  - `310000_6234_140854182`
  - `310000_6234_140854183`

`WZMF:TO_H_EVNT_NS_AREA_IMSI` 表结构建议如下：

| 字段 | 含义 |
| --- | --- |
| `rowkey` | `md5(areaId前4位)_areaId_statHour_imsi` |
| `crowds:area_id` | 区域 id |
| `crowds:stat_hour` | 小时时间 |
| `crowds:imsi` | 用户 IMSI |
| `crowds:cell_ids` | 命中的基站列表 |
| `crowds:first_time` | 小时内首次出现时间 |
| `crowds:last_time` | 小时内最后出现时间 |
| `crowds:event_count` | 小时内事件条数 |

### 3、解答

#### 1）请将 scala 代码包传输在此处

本次实际提交的 Scala 代码包目录为：

- `assignment2/hbaseProcess`

案例一新增主类如下：

- [AreaHourlyDetailJob.scala](file:///d:/Document%20Files/Schoolwork/%E5%A4%A7%E4%B8%89%E4%B8%8B/%E6%95%B0%E7%A7%91%E5%AD%A6%E4%B8%8E%E6%99%BA%E8%83%BD%E8%BD%AF%E4%BB%B6%E9%A1%B9%E7%9B%AE%E5%AE%9E%E8%AE%AD/%E4%BD%9C%E4%B8%9A%E4%B8%80/docker-hadoop/assignment2/hbaseProcess/src/main/scala/org/example/dataProcess/AreaHourlyDetailJob.scala)

复用工具类如下：

- [HbaseUtil.scala](file:///d:/Document%20Files/Schoolwork/%E5%A4%A7%E4%B8%89%E4%B8%8B/%E6%95%B0%E7%A7%91%E5%AD%A6%E4%B8%8E%E6%99%BA%E8%83%BD%E8%BD%AF%E4%BB%B6%E9%A1%B9%E7%9B%AE%E5%AE%9E%E8%AE%AD/%E4%BD%9C%E4%B8%9A%E4%B8%80/docker-hadoop/assignment2/hbaseProcess/src/main/scala/org/example/utils/HbaseUtil.scala)
- [MD5Util.scala](file:///d:/Document%20Files/Schoolwork/%E5%A4%A7%E4%B8%89%E4%B8%8B/%E6%95%B0%E7%A7%91%E5%AD%A6%E4%B8%8E%E6%99%BA%E8%83%BD%E8%BD%AF%E4%BB%B6%E9%A1%B9%E7%9B%AE%E5%AE%9E%E8%AE%AD/%E4%BD%9C%E4%B8%9A%E4%B8%80/docker-hadoop/assignment2/hbaseProcess/src/main/scala/org/example/utils/MD5Util.scala)

运行命令如下：

```powershell
cd .\assignment2
docker compose run --rm hbaseprocess sh -lc 'mvn -U -DskipTests compile exec:java -Dexec.mainClass=org.example.dataProcess.AreaHourlyDetailJob'
```

建议保留运行过程截图：`artifacts/case1-job-run.png`

本次实际运行结果：

- 模拟导数已成功写入 `WZMF:TO_MI_EVNT_NS_CELL_IMSI`
- 案例一程序执行成功，控制台输出 `case1 done, rows=14400`
- 查询 `WZMF:TO_H_EVNT_NS_AREA_IMSI` 时，已能看到 `area_id`、`stat_hour`、`imsi`、`cell_ids`、`first_time`、`last_time`、`event_count` 等字段

#### 2）经代码运行后，查询 `WZMF:TO_H_EVNT_NS_AREA_IMSI` 的截图

请保留以下命令输出截图：

```powershell
@("scan 'WZMF:TO_H_EVNT_NS_AREA_IMSI', {LIMIT => 20}") | docker compose exec -T hbase hbase shell -n
```

建议截图文件：`artifacts/case1-target-scan.png`

---

## 案例二、数据开发-区域人流量汇聚统计

### 1、准备工作

建表语句：

```hbase
create 'WZMF:TA_H_REGION_CROWD_FLOW', 'crowds'
```

在当前独立作业环境中，建表可直接执行：

```powershell
cd .\assignment2
@("create 'WZMF:TA_H_REGION_CROWD_FLOW','crowds'") | docker compose exec -T hbase hbase shell -n
```

如果尚未启动环境，请先执行：

```powershell
cd .\assignment2
docker compose up -d hbase
```

说明：

- 案例二以上一题生成的 `WZMF:TO_H_EVNT_NS_AREA_IMSI` 为输入表。
- 当前实现程序会读取两个区域的小时人员明细，再计算总人数、流入人数、保持人数、流出人数。

### 2、题目介绍

在完成案例一程序代码后，将 `WZMF:TO_H_EVNT_NS_AREA_IMSI` 中两个区域：

- 区域 1-id：`255027778846457856`
- 区域 2-id：`255027778846457857`

按照以下统计口径写入 `WZMF:TA_H_REGION_CROWD_FLOW`：

- 总人数
- 流入人数
- 保持人数
- 流出人数

相关业务口径如下：

- 流入人群：上一时刻 A 不在该区域，此刻 B 在该区域的人
- 保持人群：上一时刻 A 在该区域，此刻 B 也在该区域的人
- 流出人群：上一时刻 A 在该区域，此刻 B 不在该区域的人
- 当前总人数：当前时刻 B 的总人数

### 3、解答

#### 1）设计的表结构可粘贴在此处

`WZMF:TA_H_REGION_CROWD_FLOW` 表结构建议如下：

| 字段 | 含义 |
| --- | --- |
| `rowkey` | `md5(areaId前4位)_areaId_statHour` |
| `crowds:area_id` | 区域 id |
| `crowds:stat_hour` | 统计小时 |
| `crowds:total_count` | 当前总人数 |
| `crowds:inflow_count` | 流入人数 |
| `crowds:stay_count` | 保持人数 |
| `crowds:outflow_count` | 流出人数 |

#### 2）scala 代码包传输在此处

本次实际提交的 Scala 代码包目录仍为：

- `assignment2/hbaseProcess`

案例二新增主类如下：

- [RegionCrowdFlowJob.scala](file:///d:/Document%20Files/Schoolwork/%E5%A4%A7%E4%B8%89%E4%B8%8B/%E6%95%B0%E7%A7%91%E5%AD%A6%E4%B8%8E%E6%99%BA%E8%83%BD%E8%BD%AF%E4%BB%B6%E9%A1%B9%E7%9B%AE%E5%AE%9E%E8%AE%AD/%E4%BD%9C%E4%B8%9A%E4%B8%80/docker-hadoop/assignment2/hbaseProcess/src/main/scala/org/example/dataProcess/RegionCrowdFlowJob.scala)

运行命令如下：

```powershell
cd .\assignment2
docker compose run --rm hbaseprocess sh -lc 'mvn -U -DskipTests compile exec:java -Dexec.mainClass=org.example.dataProcess.RegionCrowdFlowJob'
```

建议保留运行过程截图：`artifacts/case2-job-run.png`

本次实际运行结果：

- 案例二程序执行成功，控制台输出 `case2 done, rows=48`
- 查询 `WZMF:TA_H_REGION_CROWD_FLOW` 时，已能看到 `total_count`、`inflow_count`、`stay_count`、`outflow_count` 等指标字段
- 例如区域 `255027778846457857` 在 `2026060100` 时刻统计结果为：
  - `total_count=240`
  - `inflow_count=240`
  - `stay_count=0`
  - `outflow_count=0`

#### 3）经代码运行后，查询 `WZMF:TA_H_REGION_CROWD_FLOW` 的截图

请保留以下命令输出截图：

```powershell
@("scan 'WZMF:TA_H_REGION_CROWD_FLOW', {LIMIT => 20}") | docker compose exec -T hbase hbase shell -n
```

建议截图文件：`artifacts/case2-target-scan.png`

---

## 案例三、后端服务开发-读取某个时间段的人流数据

### 1、准备工作

下载并准备好 API 测试工具，例如：

- Postman
- Apifox
- 或直接使用命令行 `curl`

### 2、题目介绍

通过案例二已经得到了 `WZMF:TA_H_REGION_CROWD_FLOW` 表中 2026/6/1 至 2026/6/2 期间两个区域的人流数据情况。

本题需要完成两部分：

1. 设计 API 的入参、出参
2. 使用 Java 编写读取 `WZMF:TA_H_REGION_CROWD_FLOW` 的接口代码

本次实际设计的接口如下：

- 请求方式：`GET`
- 接口路径：`/api/region-flow`

入参如下：

| 参数 | 类型 | 是否必填 | 说明 |
| --- | --- | --- | --- |
| `areaId` | string | 是 | 区域 id |
| `startHour` | string | 是 | 开始小时，格式 `yyyyMMddHH` |
| `endHour` | string | 是 | 结束小时，格式 `yyyyMMddHH` |

出参示例如下：

```json
{
  "areaId": "255027778846457857",
  "startHour": "2026060100",
  "endHour": "2026060104",
  "items": [
    {
      "statHour": "2026060100",
      "totalCount": 240,
      "inflowCount": 240,
      "stayCount": 0,
      "outflowCount": 0
    }
  ]
}
```

### 3、解答

#### 1、java 代码包

本次实际提交的 Java 代码包目录为：

- `assignment2/region-flow-api`

实际主类如下：

- [RegionCrowdFlowApiServer.java](file:///d:/Document%20Files/Schoolwork/%E5%A4%A7%E4%B8%89%E4%B8%8B/%E6%95%B0%E7%A7%91%E5%AD%A6%E4%B8%8E%E6%99%BA%E8%83%BD%E8%BD%AF%E4%BB%B6%E9%A1%B9%E7%9B%AE%E5%AE%9E%E8%AE%AD/%E4%BD%9C%E4%B8%9A%E4%B8%80/docker-hadoop/assignment2/region-flow-api/src/main/java/org/example/assignment2/RegionCrowdFlowApiServer.java)
- [pom.xml](file:///d:/Document%20Files/Schoolwork/%E5%A4%A7%E4%B8%89%E4%B8%8B/%E6%95%B0%E7%A7%91%E5%AD%A6%E4%B8%8E%E6%99%BA%E8%83%BD%E8%BD%AF%E4%BB%B6%E9%A1%B9%E7%9B%AE%E5%AE%9E%E8%AE%AD/%E4%BD%9C%E4%B8%9A%E4%B8%80/docker-hadoop/assignment2/region-flow-api/pom.xml)

启动命令如下：

```powershell
cd .\assignment2
docker compose run --service-ports --rm region-flow-api sh -lc 'mvn -U -DskipTests compile exec:java -Dexec.mainClass=org.example.assignment2.RegionCrowdFlowApiServer'
```

建议保留运行过程截图：`artifacts/case3-api-run.png`

#### 2、测试截图，比如 postman 工具测试的截图

推荐测试 URL：

```text
http://localhost:8080/api/region-flow?areaId=255027778846457857&startHour=2026060100&endHour=2026060104
```

如果使用命令行测试，可执行：

```powershell
curl.exe "http://localhost:8080/api/region-flow?areaId=255027778846457857&startHour=2026060100&endHour=2026060104"
```

建议保留以下截图：

- `artifacts/case3-postman.png`
- `artifacts/case3-curl.png`

本次实际运行结果：

- Java API 已成功启动，控制台输出 `RegionCrowdFlowApiServer started on port 8080`
- 实际请求接口：
  - `GET /api/region-flow?areaId=255027778846457857&startHour=2026060100&endHour=2026060104`
- 返回状态：`200 OK`
- 返回结果中已包含 5 个小时的人流统计数据，且每小时都包含：
  - `statHour`
  - `totalCount`
  - `inflowCount`
  - `stayCount`
  - `outflowCount`
