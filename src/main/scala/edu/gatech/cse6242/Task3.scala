package edu.gatech.cse6242

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object Task3 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, args(0))
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, "t w")

    val tBytes = Bytes.toBytes("t")
    val wBytes = Bytes.toBytes("w")
    val qualifier = Bytes.toBytes("")

    val rdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(x => x._2)
      .map(r => (
        Bytes.toString(r.getValue(tBytes, qualifier)),
        Bytes.toString(r.getValue(wBytes, qualifier)).toInt
        )
      )
      .reduceByKey(_ + _)
      .saveAsTextFile("hdfs://localhost:8020" + args(1));
  }
}
