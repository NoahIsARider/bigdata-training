import uuid
from pathlib import Path
import time

SAMPLE_LINES = [
    "748c35aa-854c-4efa-ab52-f90e09f6d8eb|20001 00001|23.7367148|40.3515728|1718175430523",
    "ddb32488-2980-4b87-93d5-a16b06a154f3|50001 00001|6.82555455|16.2644781|1718175431526",
    "0f62ea73-e7b1-4e5d-ab84-840680981ef4|70001 00001|59.0283434|33.9339193|1718175432541",
    "55a5c783-e4cd-41c4-b842-0bcb265a0aff|70001 00001|1.89313796|79.3784426|1718175433549",
    "c3b42737-8529-4e11-9de4-b4abcde24a90|50001 00001|1.02895263|33.5274876|1718175434563",
    "5f6f6859-c2ef-4d0d-b39e-17bc29bb669d|30001 00001|56.3833827|81.4646975|1718175435570",
    "db17f922-d66a-4402-9fcb-d55c74bd247f|30001 00001|31.5625781|19.0019309|1718175436585",
    "dd48baff-c9fb-40d0-8e5a-2b5f06b88f32|60001_00001|6.24584545|43.0772907|1718175437593",
]


def write_batch(target_dir: Path, lines: list[str]) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    batch_name = f"batch_{int(time.time())}_{uuid.uuid4().hex[:8]}.log"
    temp_path = target_dir / f".{batch_name}.tmp"
    final_path = target_dir / batch_name
    temp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temp_path.replace(final_path)
    return final_path


def main() -> None:
    hdfs_path = write_batch(Path("/data/spool/hdfs"), SAMPLE_LINES)
    kafka_path = write_batch(Path("/data/spool/kafka"), SAMPLE_LINES)
    print(f"Generated {len(SAMPLE_LINES)} records")
    print(f"HDFS spool file: {hdfs_path}")
    print(f"Kafka spool file: {kafka_path}")


if __name__ == "__main__":
    main()
