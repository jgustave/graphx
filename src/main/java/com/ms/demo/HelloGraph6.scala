package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object HelloGraph6 {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)

    val dataRows: RDD[DataRow] =
      sc.parallelize( Array(DataRow( Array(VertexAttr("cookie", "cookie:1"),VertexAttr("orderid", "order:1")), EdgeAttr("click",1234)),
                            DataRow( Array(VertexAttr("cookie", "cookie:2"),VertexAttr("orderid", "order:2")), EdgeAttr("click",1235)),
                            DataRow( Array(VertexAttr("cookie", "cookie:2"),VertexAttr("orderid", "order:2"),VertexAttr("foo", "foo:2")), EdgeAttr("click",1236)) //can have > 2 vertecies in a line.
      ))

    val vertexes: RDD[(VertexId,VertexAttr)] = dataRows.flatMap(x=>x.verticies).distinct().zipWithUniqueId().map(x=>(x._2,x._1))


    for (x <- vertexes  ) {
      println(x)
    }

  }
}




