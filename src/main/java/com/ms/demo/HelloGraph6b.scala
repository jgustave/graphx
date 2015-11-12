package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import shapeless.syntax.std.tuple._

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object HelloGraph6b {

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

    //TODO: Exception handling...
    //Convert to Objects
    val packagedData = rawData.map(x=>(EdgeAttr(x._2, x._3.toLong,x._1),VertexAttr(x._4,x._5)))

    //Get Unique IDs for Vertexes
    val uniqueVertexes = packagedData.map(x=>x._2).distinct().zipWithUniqueId().cache()

    //Assign Unique IDS to All
    val allVertexes: RDD[(VertexId,VertexAttr)] = packagedData.map(x=>x._2).map(x=>(x,x)).join(uniqueVertexes).map(x=>x._2.reverse)

    //Join in the Vertex UID and drop the VertexAttr
    val edgeAndLink: RDD[(EdgeAttr,VertexId)] = packagedData.map(x=>(x._2,x)) //Vertex and Everything
                                                             .join(uniqueVertexes) //Join in VertexId
                                                             .map(x=>x._2) //Drop the Join Key
                                                             .map(x=>(x._1._1,x._2)) //Just Keep EdgeAttr and VertexId

//
    //Create the Edges
    val connections : RDD[Edge[EdgeAttr]] =
          edgeAndLink.groupBy(_._1.uuid) //group all Rows by the UID
                      .map(x=>x._2) //Drop the Extra Grouping UID Key
                      .map(x=> (x.unzip._1.last, x.unzip._2) ) //Split in to a single Edge, and List of VertexIds (may bemore than 2)
                      .map(x=>(x._1,halfcross(x._2,x._2) ) ) //Take the Lower Diagonal (~half cross product)of all pairs of vertexes
                      .flatMap(x=>{for{i <- x._2 } yield (i._1,i._2,x._1) } ) //Map to Triplet
                      .map(x=>Edge(x._1,x._2,x._3)) //Map to Edge

    //Create Graph
    val graph = Graph(allVertexes,connections)

    //Get Connected Components
    val cc = graph.connectedComponents()

    //Print out VertexID and it's CC ID.
    for ((id, scc) <- cc.vertices.collect().sortBy(r => (r._2,r._1) )  ) {
      println(id,scc)
    }


    for( x <- connections ) {
      println(x)
    }
  }

  /**
   * Lower half of the cartesian
   * @param xs
   * @param ys
   * @tparam L
   * @return
   */
  def halfcross[L <: Long](xs: Traversable[L], ys: Traversable[L]) = for { x <- xs; y <- ys if x < y  } yield (x, y)
}




