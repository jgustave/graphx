package com.ms.demo
import org.apache.spark._
//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 */
object Hello {

  def main( args: Array[String] ) = {
    println("Hello World")

    val conf = new SparkConf().setAppName("Simple Application")
    val sc   = new SparkContext(conf)

    val text = sc.textFile("/Users/jerdavis/devhome/spark/spark-1.5.0/README.md")

    println(text.count())

  }
}
