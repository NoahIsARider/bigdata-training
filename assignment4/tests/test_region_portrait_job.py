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
