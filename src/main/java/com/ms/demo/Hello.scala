package com.ms.demo
import org.apache.spark._

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object Hello {

  def main( args: Array[String] ) = {
    println("Hello World")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)


    val text = sc.textFile("/Users/jerdavis/devhome/spark/spark-1.5.0/README.md")

    println("HelloWorld:" + text.count())

  }
}
