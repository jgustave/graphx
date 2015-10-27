package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object HelloGraph {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((1L, ("cookie", "cookie:1")),
                           (2L, ("cookie", "cookie:2")),
                           (3L, ("cookie", "cookie:3")),
                           (4L, ("cookie", "cookie:4")),
                           (5L, ("cookie", "cookie:5")),
                           (6L, ("cookie", "cookie:6")),
                           (7L, ("email", "e:1")),
                           (8L, ("idfa", "idfa:1")),
                           (9L, ("idfa", "idfa:2")),
                           (10L, ("account", "a:1")),
                           (11L, ("account", "a:2")),
                           (12L, ("account", "a:3"))
      ))


    //Explicitly declaring the links.
    val links = Array(Edge(10L, 1L, ""), //a1 -> c1
                      Edge(10L, 2L, ""),
                      Edge(2L, 7L, ""),
                      Edge(7L, 3L, ""),  //e1 -> c3
                      Edge(3L, 8L, ""),
                      Edge(8L, 4L, ""),
                      Edge(8L, 5L, ""),
                      Edge(11L, 9L, ""),
                      Edge(12L, 9L, "")
                  )


    //Make the links bidirectional (undirected)
    val relationships: RDD[Edge[String]] =  sc.parallelize( links.flatMap(x => bidir(x) ) )


    val graph = Graph(users,relationships)

    val cc = graph.connectedComponents()

    /**
     * Sort by the component ID (The lowest vertexId in the component), and then the VertexID in the component.
     */
    for ((id, scc) <- cc.vertices.collect().sortBy(r => (r._2,r._1) )  ) {
      println(id,scc)
    }

    println("Hellooo " + cc)
  }

  /**
   * Make a Bidirectional Link
   */
  def bidir(v : Edge[String]) : Array[Edge[String]] = {
    Array(Edge(v.srcId ,v.dstId),Edge(v.dstId,v.srcId))
  }

}



