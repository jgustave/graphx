package com.ms.demo

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object HelloGraph5 {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)

    val preUsers: RDD[VertexAttr] =
      sc.parallelize(Array(new VertexAttr("cookie", "cookie:1"),
                           new VertexAttr("cookie", "cookie:2"),
                           new VertexAttr("cookie", "cookie:2"),
                           new VertexAttr("cookie", "cookie:2"),
                           new VertexAttr("cookie", "cookie:3"),
                           new VertexAttr("cookie", "cookie:4"),
                           new VertexAttr("cookie", "cookie:5"),
                           new VertexAttr("cookie", "cookie:5"),
                           new VertexAttr("cookie", "cookie:5"),
                           new VertexAttr("cookie", "cookie:6"),
                           new VertexAttr("email", "e:1"),
                           new VertexAttr("idfa", "idfa:1"),
                           new VertexAttr("idfa", "idfa:2"),
                           new VertexAttr("account", "a:1"),
                           new VertexAttr("account", "a:2"),
                           new VertexAttr("account", "a:3")
      ))

    //Example zip with unique and Map back out.
    val foo = preUsers.distinct().zipWithUniqueId()

    //preUsers joined with their Spark UID.
    //Note how we have to map to x,x to get a "PairRDD", so that we have a Key/Value and can reduce (in this case join)
    val joined = preUsers.map(x=>(x,x)).join(foo).map(x=>x._2)

    //foo.join(preUsers)


    //Get RDD of WHATEVER.. distinct and zip with Unique
    //val bar = preUsers.map(x => x._2 ).distinct().zipWithUniqueId()
    //bar.join(preUsers)
    //map...

    for (x <- joined  ) {
      println(x)
    }





  }
}




