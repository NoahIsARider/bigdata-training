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
