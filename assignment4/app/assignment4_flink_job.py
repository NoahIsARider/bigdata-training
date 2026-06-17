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
