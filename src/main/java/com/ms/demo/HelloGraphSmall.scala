package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import shapeless.syntax.std.tuple._

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object HelloGraphSmall {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)


    //UUID,source,time,type,val
    val rawData = sc.parallelize(Array(
                                    ("uuid1","click","1234","cookie","cookie:1"),
                                    ("uuid1","click","1234","orderid","order:1"),
                                    ("uuid2","click","1235","cookie","cookie:2"),
                                    ("uuid2","click","1235","orderid","order:2"),
                                    ("uuid3","click","1236","cookie","cookie:3"),
                                    ("uuid3","click","1236","orderid","order:2"),
                                    ("uuid3","click","1236","foo","foo:3")) )

    //I want to Group by UUID and then create the diagonal of all pairs along with their EdgeAttr
    val foobar = rawData.groupBy(_._1).map(x=>(x._1,x._2.toArray)).map(x=>{

      //Diagonal
      for (i <- 0 to x._2.length - 2; j <- i + 1 until x._2.length)
        yield (x._2(i), x._2(j))
    })
//    for (i <- 0 to x.verticies.length - 2; j <- i + 1 until x.verticies.length)
//      yield (x.verticies(i), x.verticies(j), x.edge)

    for( x <- foobar ) {
      println(x)
    }
  }
}




