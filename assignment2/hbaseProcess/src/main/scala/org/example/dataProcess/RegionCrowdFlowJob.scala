package org.example.dataProcess

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Result, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.example.utils.{HbaseUtil, MD5Util}

import scala.collection.JavaConverters._
import scala.collection.mutable

object RegionCrowdFlowJob {

  private val SourceTable = "WZMF:TO_H_EVNT_NS_AREA_IMSI"
  private val TargetTable = "WZMF:TA_H_REGION_CROWD_FLOW"
  private val Family = Bytes.toBytes("crowds")

  private def readString(result: Result, qualifier: String): String = {
    Option(result.getValue(Family, Bytes.toBytes(qualifier))).map(Bytes.toString).getOrElse("")
  }

  def main(args: Array[String]): Unit = {
    val connection: Connection = HbaseUtil.getConnection()
    val sourceTable: Table = connection.getTable(TableName.valueOf(SourceTable))
    val targetTable: Table = connection.getTable(TableName.valueOf(TargetTable))

    val hourUsers = mutable.HashMap[(String, String), mutable.Set[String]]()

    try {
      val scan = new Scan()
      scan.addFamily(Family)
      val scanner = sourceTable.getScanner(scan)
      try {
        scanner.iterator().asScala.foreach { result =>
          val areaId = readString(result, "area_id")
          val statHour = readString(result, "stat_hour")
          val imsi = readString(result, "imsi")
          if (areaId.nonEmpty && statHour.nonEmpty && imsi.nonEmpty) {
            hourUsers.getOrElseUpdate((areaId, statHour), mutable.Set.empty[String]) += imsi
          }
        }
      } finally {
        scanner.close()
      }

      val groupedByArea = hourUsers.toSeq.groupBy(_._1._1)
      val puts = mutable.ArrayBuffer[Put]()

      groupedByArea.foreach {
        case (areaId, rows) =>
          var previousUsers = Set.empty[String]
          rows.sortBy(_._1._2).foreach {
            case ((_, statHour), userSet) =>
              val currentUsers = userSet.toSet
              val totalCount = currentUsers.size
              val inflowCount = (currentUsers -- previousUsers).size
              val stayCount = (currentUsers intersect previousUsers).size
              val outflowCount = (previousUsers -- currentUsers).size

              val rowKey = MD5Util.md5(areaId).take(4) + "_" + areaId + "_" + statHour
              val put = new Put(Bytes.toBytes(rowKey))
              put.addColumn(Family, Bytes.toBytes("area_id"), Bytes.toBytes(areaId))
              put.addColumn(Family, Bytes.toBytes("stat_hour"), Bytes.toBytes(statHour))
              put.addColumn(Family, Bytes.toBytes("total_count"), Bytes.toBytes(totalCount.toString))
              put.addColumn(Family, Bytes.toBytes("inflow_count"), Bytes.toBytes(inflowCount.toString))
              put.addColumn(Family, Bytes.toBytes("stay_count"), Bytes.toBytes(stayCount.toString))
              put.addColumn(Family, Bytes.toBytes("outflow_count"), Bytes.toBytes(outflowCount.toString))
              puts += put

              previousUsers = currentUsers
          }
      }

      if (puts.nonEmpty) {
        targetTable.put(puts.asJava)
      }
      println("case2 done, rows=" + puts.size)
    } finally {
      sourceTable.close()
      targetTable.close()
      connection.close()
    }
  }
}
