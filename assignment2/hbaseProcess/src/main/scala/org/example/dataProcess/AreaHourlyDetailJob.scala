package org.example.dataProcess

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Result, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.example.utils.{HbaseUtil, MD5Util}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import scala.collection.JavaConverters._
import scala.collection.mutable

object AreaHourlyDetailJob {

  private val SourceTable = "WZMF:TO_MI_EVNT_NS_CELL_IMSI"
  private val TargetTable = "WZMF:TO_H_EVNT_NS_AREA_IMSI"
  private val Family = Bytes.toBytes("crowds")
  private val HourFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneId.of("Asia/Shanghai"))

  private val CellToArea: Map[String, String] = Map(
    "310000_6254_140854179" -> "255027778846457856",
    "310000_6254_140854180" -> "255027778846457856",
    "310000_6254_140854181" -> "255027778846457856",
    "310000_6234_140854182" -> "255027778846457857",
    "310000_6234_140854183" -> "255027778846457857"
  )

  case class AreaHourlyDetail(
    areaId: String,
    statHour: String,
    imsi: String,
    cellIds: mutable.Set[String],
    var firstTime: Long,
    var lastTime: Long,
    var eventCount: Int
  )

  private def extractUsers(result: Result): Seq[(String, String, Long)] = {
    val rowKey = Bytes.toString(result.getRow)
    val eventMinute = rowKey.split("_").lastOption.getOrElse("")
    if (eventMinute.length != 12) {
      return Seq.empty
    }
    val eventHour = eventMinute.substring(0, 10)
    val eventTime = localMinuteToEpochMillis(eventMinute)

    result.rawCells().toSeq.flatMap { cell =>
      val cellId = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
      val areaIdOpt = CellToArea.get(cellId)
      val imsiValue = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      val imsis = imsiValue.split(",").map(_.trim).filter(_.nonEmpty)
      areaIdOpt.toSeq.flatMap(areaId => imsis.map(imsi => (areaId, cellId, eventTime, eventHour, imsi)))
    }.map { case (areaId, cellId, ts, statHour, imsi) => (s"$areaId|$statHour|$imsi", cellId, ts) }
  }

  private def localMinuteToEpochMillis(value: String): Long = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
    val localDateTime = java.time.LocalDateTime.parse(value, formatter)
    localDateTime.atZone(ZoneId.of("Asia/Shanghai")).toInstant.toEpochMilli
  }

  def main(args: Array[String]): Unit = {
    val connection: Connection = HbaseUtil.getConnection()
    val sourceTable: Table = connection.getTable(TableName.valueOf(SourceTable))
    val targetTable: Table = connection.getTable(TableName.valueOf(TargetTable))

    val scan = new Scan()
    scan.addFamily(Family)

    val aggregates = mutable.LinkedHashMap[String, AreaHourlyDetail]()

    try {
      val scanner = sourceTable.getScanner(scan)
      try {
        scanner.iterator().asScala.foreach { result =>
          extractUsers(result).foreach {
            case (aggregateKey, cellId, eventTime) =>
              val Array(areaId, statHour, imsi) = aggregateKey.split("\\|", 3)
              aggregates.get(aggregateKey) match {
                case Some(item) =>
                  item.cellIds += cellId
                  item.firstTime = math.min(item.firstTime, eventTime)
                  item.lastTime = math.max(item.lastTime, eventTime)
                  item.eventCount += 1
                case None =>
                  aggregates.put(
                    aggregateKey,
                    AreaHourlyDetail(
                      areaId = areaId,
                      statHour = statHour,
                      imsi = imsi,
                      cellIds = mutable.Set(cellId),
                      firstTime = eventTime,
                      lastTime = eventTime,
                      eventCount = 1
                    )
                  )
              }
          }
        }
      } finally {
        scanner.close()
      }

      val puts = aggregates.values.toSeq.sortBy(item => (item.areaId, item.statHour, item.imsi)).map { item =>
        val rowKey = MD5Util.md5(item.areaId).take(4) + "_" + item.areaId + "_" + item.statHour + "_" + item.imsi
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Family, Bytes.toBytes("area_id"), Bytes.toBytes(item.areaId))
        put.addColumn(Family, Bytes.toBytes("stat_hour"), Bytes.toBytes(item.statHour))
        put.addColumn(Family, Bytes.toBytes("imsi"), Bytes.toBytes(item.imsi))
        put.addColumn(Family, Bytes.toBytes("cell_ids"), Bytes.toBytes(item.cellIds.toSeq.sorted.mkString(",")))
        put.addColumn(Family, Bytes.toBytes("first_time"), Bytes.toBytes(item.firstTime.toString))
        put.addColumn(Family, Bytes.toBytes("last_time"), Bytes.toBytes(item.lastTime.toString))
        put.addColumn(Family, Bytes.toBytes("event_count"), Bytes.toBytes(item.eventCount.toString))
        put
      }

      if (puts.nonEmpty) {
        targetTable.put(puts.asJava)
      }
      println("case1 done, rows=" + puts.size)
    } finally {
      sourceTable.close()
      targetTable.close()
      connection.close()
    }
  }
}
