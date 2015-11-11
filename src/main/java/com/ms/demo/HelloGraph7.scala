package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx.{Edge, Graph, VertexId}

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 * /opt/spark/bin/spark-submit --verbose --driver-class-path $(find /opt/hadoop/share/hadoop/mapreduce/lib/hadoop-lzo-* | head -n 1) --class com.ms.demo.HelloGraph7 ~/tmp/graph2-1.0-SNAPSHOT.jar hdfs:///user/jeremy/spark/
 */
object HelloGraph7 {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application")
    val sc   = new SparkContext(conf)

    val lines = sc.textFile(args(0))
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)

    println("NumRows:" + lines.count() + "," + lineLengths + "," + totalLength )

  }
}




