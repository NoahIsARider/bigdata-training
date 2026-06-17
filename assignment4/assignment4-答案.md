# assignment4 最终提交版（PyFlink 实现）

## 一、需求理解

题目要求基于以下三类数据，统计 **1 分钟内行政区用户画像情况**，并将结果写入 HBase：

- 基站信令数据：`imsi、laccell、latitude、longitude、procedureStartTime`
- 用户基础数据：`imsi、gender、age`
- 行政区-基站关系数据：`region_id、laccell`

输出指标至少包括：

- 男性数量
- 女性数量
- 10-20 岁人数
- 20-40 岁人数
- 40 岁以上人数

本次提交采用 **PyFlink DataStream 作业** 实现，并使用独立的 `HBase + Flink JobManager + Flink TaskManager` 环境完成实际运行验证。

---

## 二、你的思路

### 1. 整体处理流程

本题的实现链路如下：

1. PyFlink 读取信令样例数据。
2. 通过 `laccell -> region_id` 关联行政区。
3. 通过 `imsi -> gender、age` 关联用户画像。
4. 将 `procedureStartTime` 向下取整到分钟，得到 `stat_minute`。
5. 在 Flink 中按 `region_id + stat_minute + imsi` 做 **有状态去重**，确保同一用户在同一分钟同一区域只统计一次。
6. 再按 `region_id + stat_minute` 汇总，得到分钟级画像统计结果。
7. 将统计结果写入 HBase 表 `TA_MI_REGION_USER_PORTRAIT`。

### 2. 年龄分段口径

- `10 <= age < 20` 记为 `age_10_20_count`
- `20 <= age < 40` 记为 `age_20_40_count`
- `age >= 40` 记为 `age_40_plus_count`

### 3. HBase 表设计

表名：

- `TA_MI_REGION_USER_PORTRAIT`

列族：

- `info`

字段设计如下：

| 字段 | 说明 |
| --- | --- |
| `rowkey` | `region_id#yyyyMMddHHmm` |
| `info:region_id` | 行政区编号 |
| `info:stat_minute` | 统计分钟 |
| `info:total_count` | 当前分钟去重总人数 |
| `info:male_count` | 男性人数 |
| `info:female_count` | 女性人数 |
| `info:age_10_20_count` | 10-20 岁人数 |
| `info:age_20_40_count` | 20-40 岁人数 |
| `info:age_40_plus_count` | 40 岁以上人数 |

---

## 三、代码

### 1. 代码正文

老师只收一个文档，因此本次作业的核心源码直接写入本文档。

#### `app/assignment4_flink_job.py`：PyFlink 作业入口与 HBase 写入实现

```python
import argparse
import json
from pathlib import Path

from region_portrait_job import (
    DEFAULT_HBASE_TABLE,
    build_hbase_row,
    build_minute_contribution_row,
    build_minute_user_record,
    load_profiles,
    load_region_cells,
    merge_minute_rows,
)


def merge_json_rows(left: str, right: str) -> str:
    merged = merge_minute_rows(json.loads(left), json.loads(right))
    return json.dumps(merged, ensure_ascii=False, sort_keys=True)


def reset_hbase_table(host: str, port: int, table_name: str) -> None:
    import happybase

    connection = happybase.Connection(host, port)
    connection.open()
    existing = {name.decode("utf-8") for name in connection.tables()}
    if table_name not in existing:
        connection.create_table(table_name, {"info": dict()})
    table = connection.table(table_name)
    batch = table.batch()
    for row_key, _ in table.scan():
        batch.delete(row_key)
    batch.send()
    connection.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assignment4 PyFlink region portrait job")
    parser.add_argument("--signal-path", required=True)
    parser.add_argument("--profile-path", required=True)
    parser.add_argument("--region-path", required=True)
    parser.add_argument("--hbase-host", default="hbase")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-table", default=DEFAULT_HBASE_TABLE)
    return parser


def main() -> None:
    import happybase
    from pyflink.common import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
    from pyflink.datastream.state import ValueStateDescriptor

    args = build_arg_parser().parse_args()
    profiles = load_profiles(Path(args.profile_path))
    region_cells = load_region_cells(Path(args.region_path))
    signal_lines = [
        line
        for line in Path(args.signal_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    reset_hbase_table(args.hbase_host, args.hbase_port, args.hbase_table)

    class LineToUserRecord(MapFunction):
        def map(self, value):
            record = build_minute_user_record(value, profiles, region_cells)
            if record is None:
                return ""
            return json.dumps(record, ensure_ascii=False, sort_keys=True)

    class UserRecordToContribution(MapFunction):
        def map(self, value):
            return json.dumps(
                build_minute_contribution_row(json.loads(value)),
                ensure_ascii=False,
                sort_keys=True,
            )

    class FirstSeenUserDeduplicator(KeyedProcessFunction):
        def open(self, runtime_context: RuntimeContext):
            self.seen_state = runtime_context.get_state(
                ValueStateDescriptor("seen_user", Types.BOOLEAN())
            )

        def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
            if self.seen_state.value() is None:
                self.seen_state.update(True)
                yield value

    class HBaseWriterMap(MapFunction):
        def open(self, runtime_context):
            self.connection = happybase.Connection(args.hbase_host, args.hbase_port)
            self.connection.open()
            self.table = self.connection.table(args.hbase_table)

        def close(self):
            if self.connection is not None:
                self.connection.close()

        def map(self, value):
            row = json.loads(value)
            row_key, payload = build_hbase_row(row)
            encoded_payload = {
                column.encode("utf-8"): cell_value.encode("utf-8")
                for column, cell_value in payload.items()
            }
            self.table.put(row_key.encode("utf-8"), encoded_payload)
            return json.dumps({"row_key": row_key, **row}, ensure_ascii=False, sort_keys=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_python_file(f"file://{Path(__file__).resolve().parent / 'region_portrait_job.py'}")

    stream = env.from_collection(signal_lines, type_info=Types.STRING())
    parsed = stream.map(LineToUserRecord(), output_type=Types.STRING()).filter(lambda value: value != "")
    deduped = parsed.key_by(
        lambda value: json.loads(value)["user_key"],
        key_type=Types.STRING(),
    ).process(FirstSeenUserDeduplicator(), output_type=Types.STRING())
    contributions = deduped.map(UserRecordToContribution(), output_type=Types.STRING())
    aggregated = contributions.key_by(
        lambda value: json.loads(value)["region_minute_key"],
        key_type=Types.STRING(),
    ).reduce(merge_json_rows)
    aggregated.map(HBaseWriterMap(), output_type=Types.STRING()).print()
    env.execute("assignment4-minute-region-portrait")


if __name__ == "__main__":
    main()
```

#### `app/region_portrait_job.py`：分钟画像统计与 HBase 行构造核心逻辑

```python
import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_PROFILE_PATH = Path("/workspace/data/user_profile.csv")
DEFAULT_REGION_PATH = Path("/workspace/data/region_cell.csv")
DEFAULT_SIGNAL_PATH = Path("/workspace/data/base_signal.txt")
DEFAULT_OUTPUT_PATH = Path("/workspace/artifacts/assignment4-minute-portrait.json")
DEFAULT_HBASE_TABLE = "TA_MI_REGION_USER_PORTRAIT"


def parse_signal_line(line: str) -> dict:
    imsi, laccell, latitude, longitude, procedure_start_time = line.strip().split("|")
    return {
        "imsi": imsi,
        "laccell": laccell,
        "latitude": latitude,
        "longitude": longitude,
        "procedureStartTime": int(procedure_start_time),
    }


def minute_bucket(timestamp_ms: int) -> str:
    event_dt = datetime.fromtimestamp(timestamp_ms / 1000, SHANGHAI_TZ)
    return event_dt.strftime("%Y%m%d%H%M")


def age_band(age: int) -> str | None:
    if 10 <= age < 20:
        return "age_10_20_count"
    if 20 <= age < 40:
        return "age_20_40_count"
    if age >= 40:
        return "age_40_plus_count"
    return None


def load_profiles(profile_path: Path) -> dict[str, dict]:
    with profile_path.open(encoding="utf-8") as handle:
        return {row["imsi"]: row for row in csv.DictReader(handle)}


def load_region_cells(region_path: Path) -> dict[str, str]:
    with region_path.open(encoding="utf-8") as handle:
        return {row["laccell"]: row["region_id"] for row in csv.DictReader(handle)}


def build_minute_user_record(line: str, profiles: dict[str, dict], region_cells: dict[str, str]) -> dict | None:
    signal = parse_signal_line(line)
    region_id = region_cells.get(signal["laccell"])
    if region_id is None:
        return None

    imsi = signal["imsi"]
    stat_minute = minute_bucket(signal["procedureStartTime"])
    profile = profiles.get(imsi, {})
    age_value = str(profile.get("age", "0")).strip()
    age = int(age_value) if age_value.isdigit() else 0
    return {
        "user_key": f"{region_id}|{stat_minute}|{imsi}",
        "region_minute_key": f"{region_id}|{stat_minute}",
        "region_id": region_id,
        "stat_minute": stat_minute,
        "imsi": imsi,
        "gender": str(profile.get("gender", "")).strip(),
        "age": age,
    }


def reduce_user_records_to_row(region_minute_key: str, user_records: list[dict]) -> dict:
    region_id, stat_minute = region_minute_key.split("|", 1)
    stats = {
        "row_key": f"{region_id}#{stat_minute}",
        "region_id": region_id,
        "stat_minute": stat_minute,
        "total_count": 0,
        "male_count": 0,
        "female_count": 0,
        "age_10_20_count": 0,
        "age_20_40_count": 0,
        "age_40_plus_count": 0,
    }
    for record in user_records:
        stats["total_count"] += 1
        if record["gender"] == "1":
            stats["male_count"] += 1
        elif record["gender"] == "0":
            stats["female_count"] += 1

        band = age_band(record["age"])
        if band is not None:
            stats[band] += 1
    return stats


def build_minute_contribution_row(record: dict) -> dict:
    row = {
        "region_minute_key": record["region_minute_key"],
        "row_key": f"{record['region_id']}#{record['stat_minute']}",
        "region_id": record["region_id"],
        "stat_minute": record["stat_minute"],
        "total_count": 1,
        "male_count": 1 if record["gender"] == "1" else 0,
        "female_count": 1 if record["gender"] == "0" else 0,
        "age_10_20_count": 0,
        "age_20_40_count": 0,
        "age_40_plus_count": 0,
    }
    band = age_band(record["age"])
    if band is not None:
        row[band] = 1
    return row


def merge_minute_rows(left: dict, right: dict) -> dict:
    return {
        "region_minute_key": left["region_minute_key"],
        "row_key": left["row_key"],
        "region_id": left["region_id"],
        "stat_minute": left["stat_minute"],
        "total_count": left["total_count"] + right["total_count"],
        "male_count": left["male_count"] + right["male_count"],
        "female_count": left["female_count"] + right["female_count"],
        "age_10_20_count": left["age_10_20_count"] + right["age_10_20_count"],
        "age_20_40_count": left["age_20_40_count"] + right["age_20_40_count"],
        "age_40_plus_count": left["age_40_plus_count"] + right["age_40_plus_count"],
    }


def aggregate_minute_portraits(
    signal_lines: list[str],
    profiles: dict[str, dict],
    region_cells: dict[str, str],
) -> list[dict]:
    unique_user_records = {}
    for line in signal_lines:
        if not line.strip():
            continue
        record = build_minute_user_record(line, profiles, region_cells)
        if record is None:
            continue
        unique_user_records.setdefault(record["user_key"], record)

    region_groups = defaultdict(list)
    for record in unique_user_records.values():
        region_groups[record["region_minute_key"]].append(record)

    rows = []
    for region_minute_key in sorted(
        region_groups.keys(),
        key=lambda item: (item.split("|", 1)[1], item.split("|", 1)[0]),
    ):
        contributions = [build_minute_contribution_row(record) for record in region_groups[region_minute_key]]
        current = contributions[0]
        for contribution in contributions[1:]:
            current = merge_minute_rows(current, contribution)
        current.pop("region_minute_key", None)
        rows.append(current)
    return rows


def build_hbase_payload(row: dict) -> dict[str, str]:
    return {
        "info:region_id": str(row["region_id"]),
        "info:stat_minute": str(row["stat_minute"]),
        "info:total_count": str(row["total_count"]),
        "info:male_count": str(row["male_count"]),
        "info:female_count": str(row["female_count"]),
        "info:age_10_20_count": str(row["age_10_20_count"]),
        "info:age_20_40_count": str(row["age_20_40_count"]),
        "info:age_40_plus_count": str(row["age_40_plus_count"]),
    }


def build_hbase_row(row: dict) -> tuple[str, dict[str, str]]:
    row_key = str(row.get("row_key") or f"{row['region_id']}#{row['stat_minute']}")
    return row_key, build_hbase_payload(row)


def write_rows_to_hbase(rows: list[dict], table_name: str, host: str, port: int) -> None:
    import happybase

    connection = happybase.Connection(host, port)
    connection.open()
    existing = {name.decode("utf-8") for name in connection.tables()}
    if table_name not in existing:
        connection.create_table(table_name, {"info": dict()})

    table = connection.table(table_name)
    batch = table.batch()
    for row_key, _ in table.scan():
        batch.delete(row_key)
    batch.send()

    batch = table.batch()
    for row in rows:
        row_key, payload = build_hbase_row(row)
        encoded_payload = {
            column.encode("utf-8"): value.encode("utf-8")
            for column, value in payload.items()
        }
        batch.put(row_key.encode("utf-8"), encoded_payload)
    batch.send()
    connection.close()


def run_job(
    signal_path: Path,
    profile_path: Path,
    region_path: Path,
    output_path: Path | None = None,
    hbase_host: str | None = None,
    hbase_port: int = 9090,
    hbase_table: str = DEFAULT_HBASE_TABLE,
) -> list[dict]:
    profiles = load_profiles(profile_path)
    region_cells = load_region_cells(region_path)
    signal_lines = signal_path.read_text(encoding="utf-8").splitlines()
    rows = aggregate_minute_portraits(signal_lines, profiles, region_cells)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    if hbase_host:
        write_rows_to_hbase(rows, hbase_table, hbase_host, hbase_port)

    return rows


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aggregate 1-minute region portraits and optionally write to HBase.")
    parser.add_argument("--signal-path", default=str(DEFAULT_SIGNAL_PATH))
    parser.add_argument("--profile-path", default=str(DEFAULT_PROFILE_PATH))
    parser.add_argument("--region-path", default=str(DEFAULT_REGION_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--hbase-host")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-table", default=DEFAULT_HBASE_TABLE)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    rows = run_job(
        signal_path=Path(args.signal_path),
        profile_path=Path(args.profile_path),
        region_path=Path(args.region_path),
        output_path=Path(args.output_path) if args.output_path else None,
        hbase_host=args.hbase_host,
        hbase_port=args.hbase_port,
        hbase_table=args.hbase_table,
    )
    print(f"assignment4 done, rows={len(rows)}")
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
```

#### `tests/test_region_portrait_job.py`：核心统计逻辑单元测试

```python
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.region_portrait_job import (
    aggregate_minute_portraits,
    build_hbase_row,
    build_hbase_payload,
    build_minute_contribution_row,
    build_minute_user_record,
    merge_minute_rows,
    minute_bucket,
    reduce_user_records_to_row,
)


class RegionPortraitJobTest(unittest.TestCase):
    def test_minute_bucket_rounds_down_to_minute(self) -> None:
        self.assertEqual(minute_bucket(1781661605123), "202606171000")
        self.assertEqual(minute_bucket(1781661659999), "202606171000")

    def test_aggregate_minute_portraits_deduplicates_users_within_same_minute(self) -> None:
        profiles = {
            "u1": {"imsi": "u1", "gender": "1", "age": "18"},
            "u2": {"imsi": "u2", "gender": "0", "age": "25"},
            "u3": {"imsi": "u3", "gender": "1", "age": "42"},
            "u4": {"imsi": "u4", "gender": "0", "age": "19"},
            "u5": {"imsi": "u5", "gender": "1", "age": "33"},
            "u6": {"imsi": "u6", "gender": "0", "age": "45"},
        }
        region_cells = {
            "10001_00001": "region_001",
            "10001_00002": "region_002",
        }
        signal_lines = [
            "u1|10001_00001|113.1|23.1|1781661605000",
            "u1|10001_00001|113.1|23.1|1781661625000",
            "u2|10001_00001|113.1|23.1|1781661610000",
            "u3|10001_00001|113.1|23.1|1781661620000",
            "u4|10001_00002|113.2|23.2|1781661640000",
            "u5|10001_00002|113.2|23.2|1781661650000",
            "u1|10001_00001|113.1|23.1|1781661665000",
            "u2|10001_00002|113.2|23.2|1781661675000",
            "u6|10001_00002|113.2|23.2|1781661690000",
            "u3|10001_00001|113.1|23.1|1781661705000",
            "u5|10001_00002|113.2|23.2|1781661710000",
        ]

        rows = aggregate_minute_portraits(signal_lines, profiles, region_cells)

        self.assertEqual(
            rows,
            [
                {
                    "row_key": "region_001#202606171000",
                    "region_id": "region_001",
                    "stat_minute": "202606171000",
                    "total_count": 3,
                    "male_count": 2,
                    "female_count": 1,
                    "age_10_20_count": 1,
                    "age_20_40_count": 1,
                    "age_40_plus_count": 1,
                },
                {
                    "row_key": "region_002#202606171000",
                    "region_id": "region_002",
                    "stat_minute": "202606171000",
                    "total_count": 2,
                    "male_count": 1,
                    "female_count": 1,
                    "age_10_20_count": 1,
                    "age_20_40_count": 1,
                    "age_40_plus_count": 0,
                },
                {
                    "row_key": "region_001#202606171001",
                    "region_id": "region_001",
                    "stat_minute": "202606171001",
                    "total_count": 2,
                    "male_count": 2,
                    "female_count": 0,
                    "age_10_20_count": 1,
                    "age_20_40_count": 0,
                    "age_40_plus_count": 1,
                },
                {
                    "row_key": "region_002#202606171001",
                    "region_id": "region_002",
                    "stat_minute": "202606171001",
                    "total_count": 3,
                    "male_count": 1,
                    "female_count": 2,
                    "age_10_20_count": 0,
                    "age_20_40_count": 2,
                    "age_40_plus_count": 1,
                },
            ],
        )

    def test_build_hbase_payload_serializes_all_required_columns(self) -> None:
        payload = build_hbase_payload(
            {
                "region_id": "region_001",
                "stat_minute": "202606171000",
                "total_count": 3,
                "male_count": 2,
                "female_count": 1,
                "age_10_20_count": 1,
                "age_20_40_count": 1,
                "age_40_plus_count": 1,
            }
        )

        self.assertEqual(payload["info:region_id"], "region_001")
        self.assertEqual(payload["info:stat_minute"], "202606171000")
        self.assertEqual(payload["info:total_count"], "3")
        self.assertEqual(payload["info:male_count"], "2")
        self.assertEqual(payload["info:female_count"], "1")
        self.assertEqual(payload["info:age_10_20_count"], "1")
        self.assertEqual(payload["info:age_20_40_count"], "1")
        self.assertEqual(payload["info:age_40_plus_count"], "1")

    def test_build_minute_user_record_creates_flink_dedup_record(self) -> None:
        profiles = {"u1": {"imsi": "u1", "gender": "1", "age": "18"}}
        region_cells = {"10001_00001": "region_001"}
        record = build_minute_user_record(
            "u1|10001_00001|113.1|23.1|1781661605000",
            profiles,
            region_cells,
        )

        self.assertEqual(
            record,
            {
                "user_key": "region_001|202606171000|u1",
                "region_minute_key": "region_001|202606171000",
                "region_id": "region_001",
                "stat_minute": "202606171000",
                "imsi": "u1",
                "gender": "1",
                "age": 18,
            },
        )

    def test_reduce_user_records_to_row_sums_profile_buckets(self) -> None:
        row = reduce_user_records_to_row(
            "region_002|202606171001",
            [
                {
                    "user_key": "region_002|202606171001|u2",
                    "region_minute_key": "region_002|202606171001",
                    "region_id": "region_002",
                    "stat_minute": "202606171001",
                    "imsi": "u2",
                    "gender": "0",
                    "age": 25,
                },
                {
                    "user_key": "region_002|202606171001|u5",
                    "region_minute_key": "region_002|202606171001",
                    "region_id": "region_002",
                    "stat_minute": "202606171001",
                    "imsi": "u5",
                    "gender": "1",
                    "age": 33,
                },
                {
                    "user_key": "region_002|202606171001|u6",
                    "region_minute_key": "region_002|202606171001",
                    "region_id": "region_002",
                    "stat_minute": "202606171001",
                    "imsi": "u6",
                    "gender": "0",
                    "age": 45,
                },
            ],
        )
        
        self.assertEqual(row["region_id"], "region_002")
        self.assertEqual(row["stat_minute"], "202606171001")
        self.assertEqual(row["total_count"], 3)
        self.assertEqual(row["male_count"], 1)
        self.assertEqual(row["female_count"], 2)
        self.assertEqual(row["age_10_20_count"], 0)
        self.assertEqual(row["age_20_40_count"], 2)
        self.assertEqual(row["age_40_plus_count"], 1)

    def test_build_hbase_row_returns_rowkey_and_payload(self) -> None:
        row_key, payload = build_hbase_row(
            {
                "region_id": "region_001",
                "stat_minute": "202606171000",
                "total_count": 3,
                "male_count": 2,
                "female_count": 1,
                "age_10_20_count": 1,
                "age_20_40_count": 1,
                "age_40_plus_count": 1,
            }
        )
        self.assertEqual(row_key, "region_001#202606171000")
        self.assertEqual(payload["info:total_count"], "3")
        self.assertEqual(payload["info:male_count"], "2")

    def test_build_minute_contribution_row_creates_single_user_counts(self) -> None:
        row = build_minute_contribution_row(
            {
                "user_key": "region_001|202606171000|u1",
                "region_minute_key": "region_001|202606171000",
                "region_id": "region_001",
                "stat_minute": "202606171000",
                "imsi": "u1",
                "gender": "1",
                "age": 18,
            }
        )
        self.assertEqual(row["region_minute_key"], "region_001|202606171000")
        self.assertEqual(row["total_count"], 1)
        self.assertEqual(row["male_count"], 1)
        self.assertEqual(row["female_count"], 0)
        self.assertEqual(row["age_10_20_count"], 1)
        self.assertEqual(row["age_20_40_count"], 0)
        self.assertEqual(row["age_40_plus_count"], 0)

    def test_merge_minute_rows_accumulates_counts(self) -> None:
        left = {
            "region_minute_key": "region_002|202606171001",
            "region_id": "region_002",
            "stat_minute": "202606171001",
            "row_key": "region_002#202606171001",
            "total_count": 1,
            "male_count": 0,
            "female_count": 1,
            "age_10_20_count": 0,
            "age_20_40_count": 1,
            "age_40_plus_count": 0,
        }
        right = {
            "region_minute_key": "region_002|202606171001",
            "region_id": "region_002",
            "stat_minute": "202606171001",
            "row_key": "region_002#202606171001",
            "total_count": 1,
            "male_count": 1,
            "female_count": 0,
            "age_10_20_count": 1,
            "age_20_40_count": 0,
            "age_40_plus_count": 0,
        }
        merged = merge_minute_rows(left, right)
        self.assertEqual(merged["total_count"], 2)
        self.assertEqual(merged["male_count"], 1)
        self.assertEqual(merged["female_count"], 1)
        self.assertEqual(merged["age_10_20_count"], 1)
        self.assertEqual(merged["age_20_40_count"], 1)
        self.assertEqual(merged["age_40_plus_count"], 0)


if __name__ == "__main__":
    unittest.main()
```

#### `docker-compose.yml`：Flink 与 HBase 集群编排配置

```yaml
services:
  hbase:
    build:
      context: ./hbase
    container_name: assignment4-hbase
    hostname: hbase
    ports:
      - "16011:16010"
      - "9091:9090"
    volumes:
      - hbase-data:/opt/hbase-data

  flink-jobmanager:
    build:
      context: ./flink
    container_name: assignment4-flink-jobmanager
    command: jobmanager
    depends_on:
      - hbase
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        rest.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 2
    ports:
      - "8081:8081"
    volumes:
      - ./app:/workspace/app
      - ./data:/workspace/data
      - ./artifacts:/workspace/artifacts

  flink-taskmanager:
    build:
      context: ./flink
    container_name: assignment4-flink-taskmanager
    command: taskmanager
    depends_on:
      - flink-jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        rest.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 2
    volumes:
      - ./app:/workspace/app
      - ./data:/workspace/data
      - ./artifacts:/workspace/artifacts

volumes:
  hbase-data:
```

#### `flink/Dockerfile`：PyFlink 运行环境镜像构建文件

```dockerfile
FROM eclipse-temurin:8-jre-jammy

ENV FLINK_VERSION=1.18.1
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
ENV PYFLINK_CLIENT_EXECUTABLE=python3

RUN sed -i 's|http://archive.ubuntu.com/ubuntu|http://mirrors.aliyun.com/ubuntu|g; s|http://security.ubuntu.com/ubuntu|http://mirrors.aliyun.com/ubuntu|g' /etc/apt/sources.list \
    && apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends bash curl ca-certificates python3 python3-pip python3-dev build-essential procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && curl --retry 5 --retry-delay 3 -fsSL "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" -o /tmp/flink.tgz \
    && tar -xzf /tmp/flink.tgz -C /opt \
    && mv "/opt/flink-${FLINK_VERSION}" "${FLINK_HOME}" \
    && rm /tmp/flink.tgz \
    && pip3 install --no-cache-dir apache-flink==1.18.1 happybase thriftpy2

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh \
    && chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
```

#### `flink/docker-entrypoint.sh`：Flink 容器启动入口脚本

```bash
#!/bin/bash
set -euo pipefail

FLINK_CONF_FILE="${FLINK_HOME}/conf/flink-conf.yaml"

append_conf_if_missing() {
  local line="$1"
  if ! grep -qxF "${line}" "${FLINK_CONF_FILE}"; then
    echo "${line}" >> "${FLINK_CONF_FILE}"
  fi
}

append_conf_if_missing "jobmanager.bind-host: 0.0.0.0"
append_conf_if_missing "rest.bind-address: 0.0.0.0"
append_conf_if_missing "taskmanager.bind-host: 0.0.0.0"

if [[ -n "${FLINK_PROPERTIES:-}" ]]; then
  while IFS= read -r line; do
    if [[ -z "${line// }" ]]; then
      continue
    fi
    append_conf_if_missing "${line}"
  done <<< "${FLINK_PROPERTIES}"
fi

case "${1:-help}" in
  jobmanager)
    exec "${FLINK_HOME}/bin/jobmanager.sh" start-foreground
    ;;
  taskmanager)
    exec "${FLINK_HOME}/bin/taskmanager.sh" start-foreground
    ;;
  help|--help|-h)
    exec "${FLINK_HOME}/bin/flink" --help
    ;;
  *)
    exec "$@"
    ;;
esac
```

#### `hbase/Dockerfile`：HBase 运行环境镜像构建文件

```dockerfile
FROM eclipse-temurin:8-jre-jammy

ENV HBASE_VERSION=2.4.17
ENV HBASE_HOME=/opt/hbase
ENV PATH=$HBASE_HOME/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl bash procps python3 python3-pip python3-dev build-essential tini net-tools \
    && rm -rf /var/lib/apt/lists/* \
    && bash -lc 'set -e; \
      primary="https://dlcdn.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"; \
      fallback="https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"; \
      if ! curl -fL "$primary" -o /tmp/hbase.tgz; then \
        for i in 1 2 3 4 5; do \
          if curl --retry 3 --retry-all-errors --retry-delay 5 -fL "$fallback" -o /tmp/hbase.tgz; then \
            exit 0; \
          fi; \
          sleep 3; \
        done; \
        exit 1; \
      fi' \
    && tar -xzf /tmp/hbase.tgz -C /opt \
    && mv /opt/hbase-${HBASE_VERSION} ${HBASE_HOME} \
    && rm /tmp/hbase.tgz \
    && pip3 install --no-cache-dir happybase thriftpy2

COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY start-hbase.sh /usr/local/bin/start-hbase.sh

RUN sed -i 's/\r$//' /usr/local/bin/start-hbase.sh \
    && chmod +x /usr/local/bin/start-hbase.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/start-hbase.sh"]
```

#### `hbase/hbase-site.xml`：HBase 单机模式关键参数配置

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///opt/hbase-data/rootdir</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>false</value>
  </property>
  <property>
    <name>hbase.master.hostname</name>
    <value>hbase</value>
  </property>
  <property>
    <name>hbase.regionserver.hostname.disable.master.reversedns</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.master.info.port</name>
    <value>16010</value>
  </property>
  <property>
    <name>hbase.regionserver.info.port</name>
    <value>16030</value>
  </property>
  <property>
    <name>hbase.rest.port</name>
    <value>8080</value>
  </property>
  <property>
    <name>hbase.thrift.port</name>
    <value>9090</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>127.0.0.1</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/opt/hbase-data/zookeeper</value>
  </property>
</configuration>
```

#### `hbase/start-hbase.sh`：HBase 服务启动与就绪检查脚本

```bash
#!/bin/bash
set -euo pipefail

mkdir -p /opt/hbase-data/rootdir /opt/hbase-data/logs
mkdir -p /opt/hbase-data/zookeeper

echo "[debug] hostname=$(hostname)"
echo "[debug] HBASE_HOME=${HBASE_HOME}"
echo "[debug] rootdir_contents_before_start"
find /opt/hbase-data/rootdir -maxdepth 2 -mindepth 1 | sort || true
echo "[debug] wal_dirs_before_start"
find /opt/hbase-data/rootdir/WALs -maxdepth 1 -mindepth 1 2>/dev/null | sort || true

${HBASE_HOME}/bin/hbase-daemon.sh start zookeeper
${HBASE_HOME}/bin/hbase-daemon.sh start master
${HBASE_HOME}/bin/hbase-daemon.sh start regionserver

deadline=$((SECONDS + 360))
while [ $SECONDS -lt $deadline ]; do
  if timeout 3 bash -c "echo >/dev/tcp/localhost/16010" 2>/dev/null; then
    echo "[check] Master port 16010 is listening"
    break
  fi
  echo "[check] Waiting for HBase master... (${SECONDS}s elapsed)"
  sleep 5
done

if [ $SECONDS -ge $deadline ]; then
  echo "HBase core services did not become ready in time" >&2
  echo "=== Master log ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-master-*.log 2>/dev/null || true
  echo "=== Master out ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-master-*.out 2>/dev/null || true
  echo "=== RegionServer log ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-regionserver-*.log 2>/dev/null || true
  echo "=== RegionServer out ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-regionserver-*.out 2>/dev/null || true
  echo "=== Zookeeper out ===" >&2
  cat ${HBASE_HOME}/logs/hbase-*-zookeeper-*.out 2>/dev/null || true
  ps aux 2>/dev/null || true
  exit 1
fi

${HBASE_HOME}/bin/hbase thrift start -p 9090 > /opt/hbase-data/logs/thrift.log 2>&1 &
${HBASE_HOME}/bin/hbase rest start -p 8080 --infoport 8085 > /opt/hbase-data/logs/rest.log 2>&1 &

echo "HBase services started"
tail -F ${HBASE_HOME}/logs/* /opt/hbase-data/logs/*.log
```

#### 代码说明

- 上面已经包含本次作业的核心业务代码、单元测试、Flink 集群配置和 HBase 环境配置。
- 这种写法更能证明不仅完成了统计逻辑，也掌握了 Flink 提交、容器化部署、HBase 服务启动与联调。

### 2. 测试命令

```powershell
python -m unittest assignment4/tests/test_region_portrait_job.py
```

本次实际验证结果：

- 共运行 `8` 个测试
- 输出为 `OK`

### 3. 启动 Flink + HBase 环境

```powershell
cd .\assignment4
docker compose up -d hbase flink-jobmanager flink-taskmanager
```

本次实际验证结果：

- `assignment4-hbase` 已成功运行
- `assignment4-flink-jobmanager` 已成功运行
- `assignment4-flink-taskmanager` 已成功运行

### 4. 真正的 Flink 作业提交命令

本次提交作业使用的是 **Flink CLI**，不是普通 Python 直接执行：

```powershell
cd .\assignment4
docker compose exec flink-jobmanager flink run -py /workspace/app/assignment4_flink_job.py --signal-path /workspace/data/base_signal.txt --profile-path /workspace/data/user_profile.csv --region-path /workspace/data/region_cell.csv --hbase-host hbase
```

本次最新实测结果：

- `Job has been submitted with JobID 750eaad436ad219de0384117e2b51877`
- `Program execution finished`
- `Job with JobID 750eaad436ad219de0384117e2b51877 has finished.`
- `Job Runtime: 1739 ms`

### 5. HBase 查询命令

```powershell
cd .\assignment4
@("scan 'TA_MI_REGION_USER_PORTRAIT', {LIMIT => 20}") | docker compose exec -T hbase hbase shell -n
```

本次实际 HBase 验证结果：

- 返回 `4 row(s)`
- 每条记录都包含：
  - `info:region_id`
  - `info:stat_minute`
  - `info:total_count`
  - `info:male_count`
  - `info:female_count`
  - `info:age_10_20_count`
  - `info:age_20_40_count`
  - `info:age_40_plus_count`

最终统计结果如下：

| region_id | stat_minute | total_count | male_count | female_count | age_10_20_count | age_20_40_count | age_40_plus_count |
| --- | --- | --- | --- | --- | --- | --- | --- |
| region_001 | 202606171000 | 3 | 2 | 1 | 1 | 1 | 1 |
| region_002 | 202606171000 | 2 | 1 | 1 | 1 | 1 | 0 |
| region_001 | 202606171001 | 2 | 2 | 0 | 1 | 0 | 1 |
| region_002 | 202606171001 | 3 | 1 | 2 | 0 | 2 | 1 |

---

## 四、截图

建议保留下面 4 张截图，这样最没有争议：

### 1. 单元测试通过截图

截图命令：

```powershell
python -m unittest assignment4/tests/test_region_portrait_job.py
```

截图位置要求：

- 清楚截到 `Ran 8 tests`
- 清楚截到 `OK`

建议文件名：

- `artifacts/a4-test-ok.png`

### 2. Flink 集群启动截图

截图命令：

```powershell
cd .\assignment4
docker compose up -d hbase flink-jobmanager flink-taskmanager
```

截图位置要求：

- 清楚截到 `assignment4-hbase`
- 清楚截到 `assignment4-flink-jobmanager`
- 清楚截到 `assignment4-flink-taskmanager`
- 清楚截到容器状态正常，例如 `Running` 或 `Started`

建议文件名：

- `artifacts/a4-flink-cluster-up.png`

### 3. Flink 作业提交截图

截图命令：

```powershell
cd .\assignment4
docker compose exec flink-jobmanager flink run -py /workspace/app/assignment4_flink_job.py --signal-path /workspace/data/base_signal.txt --profile-path /workspace/data/user_profile.csv --region-path /workspace/data/region_cell.csv --hbase-host hbase
```

截图位置要求：

- 必须清楚截到 `flink run -py`
- 必须清楚截到 `Job has been submitted with JobID`
- 必须清楚截到 `Program execution finished`
- 必须清楚截到 `has finished`

建议文件名：

- `artifacts/a4-flink-run.png`

### 4. HBase 查询结果截图

截图命令：

```powershell
cd .\assignment4
@("scan 'TA_MI_REGION_USER_PORTRAIT', {LIMIT => 20}") | docker compose exec -T hbase hbase shell -n
```

截图位置要求：

- 至少截到 1 条完整记录
- 最好截到：
  - `info:region_id`
  - `info:stat_minute`
  - `info:male_count`
  - `info:female_count`
  - `info:age_10_20_count`
  - `info:age_20_40_count`
  - `info:age_40_plus_count`
- 最后要截到 `4 row(s)`

建议文件名：

- `artifacts/a4-hbase-scan.png`

---

## 五、可直接写进作业里的结论

本次 assignment4 已采用 **PyFlink DataStream** 实现“1 分钟内行政区用户画像统计”功能。程序能够：

- 读取基站信令数据
- 关联行政区-基站关系数据
- 关联用户基础画像数据
- 按分钟进行时间归桶
- 对同一分钟同一区域重复出现的同一用户进行去重
- 统计男性、女性、10-20 岁、20-40 岁、40 岁以上人数
- 将结果写入 HBase 表 `TA_MI_REGION_USER_PORTRAIT`

本次作业已经通过以下实际验证：

- 单元测试 `8` 项全部通过
- Flink 集群实际启动成功
- 通过 `flink run -py` 成功提交并完成 PyFlink 作业
- HBase 查询返回 `4 row(s)`，且结果字段完整、统计值正确

因此，本次提交版本属于 **真实 Flink 作业实现 + HBase 落表验证完成版**。
