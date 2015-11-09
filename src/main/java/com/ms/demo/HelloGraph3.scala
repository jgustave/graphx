package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object HelloGraph3 {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)

    val preUsers: RDD[(String, String)] =
      sc.parallelize(Array(("cookie", "cookie:1"),
                           ("cookie", "cookie:2"),
                           ("cookie", "cookie:3"),
                           ("cookie", "cookie:4"),
                           ("cookie", "cookie:5"),
                           ("cookie", "cookie:6"),
                           ("email", "e:1"),
                           ("idfa", "idfa:1"),
                           ("idfa", "idfa:2"),
                           ("account", "a:1"),
                           ("account", "a:2"),
                           ("account", "a:3")
      ))

    //Example zip with unique and Map back out.
    val foo = preUsers.zipWithUniqueId().map(f => (f._1._1,f._2) )

    //Get RDD of WHATEVER.. distinct and zip with Unique
    val bar = preUsers.map(x => x._2 ).distinct().zipWithUniqueId()
    //bar.join(preUsers)
    //map...





  }
}




