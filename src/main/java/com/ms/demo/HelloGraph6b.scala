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
    val packagedData : RDD[(EdgeAttr,VertexAttr)] = packageRawData(rawData)

    //Get Unique IDs for Vertexes
    val uniqueVertexes : RDD[(VertexId,VertexAttr)] = getUniqueVertexIds( packagedData )

    //Assign Unique IDS to All
    val allVertexes : RDD[(VertexId,VertexAttr)] = assignVertexIds(packagedData,uniqueVertexes)

    val connections : RDD[Edge[EdgeAttr]] = createEdges(packagedData,uniqueVertexes)

    //Create Graph
    val graph = Graph(allVertexes,connections)

    //Get Connected Components
    val cc = graph.connectedComponents()

    //(VertexId,CCId)
      //Select CC with more than one account.
      //

    val assignedGroups = cc.vertices.join(allVertexes).map(x=>x._2)


    for( x <- assignedGroups ) {
      println(x)
    }
  }


  /**
   * Convert Strings to Objects
   * @param rawInput
   * @return
   */
  def packageRawData( rawInput :RDD[(String,String,String,String,String)] ) : RDD[(EdgeAttr,VertexAttr)] = {
    rawInput.map(x=>(EdgeAttr(x._2, x._3.toLong,x._1), VertexAttr(x._4,x._5)))
  }

  def getUniqueVertexIds(packagedData : RDD[(EdgeAttr,VertexAttr)]  ) : RDD[(VertexId,VertexAttr)] = {
    packagedData.map(x=>x._2) //Vertex
                .distinct()
                .zipWithUniqueId()
                .map(x=>x.reverse)
  }

  /**
   * Create the Edge RDD (VertexId,VertexId,EdgeAttribute) That represents the set of tripplets
   * @param packagedData
   * @param uniqueVertexes
   * @return
   */
  def createEdges( packagedData : RDD[(EdgeAttr,VertexAttr)], uniqueVertexes : RDD[(VertexId,VertexAttr)]) : RDD[Edge[EdgeAttr]] = {

    //Sort of half Triplet.. An Edge, and a VertexId attched
    val edgeAndVertex: RDD[(EdgeAttr,VertexId)] = packagedData.map(x=>(x._2,x)) //Vertex and Everything
                                                             .join(uniqueVertexes.map(x=>x.reverse)) //Join in VertexId
                                                             .map(x=>x._2) //Drop the Join Key
                                                             .map(x=>(x._1._1,x._2)) //Just Keep EdgeAttr and VertexId

//
    //Create the Tripplet : (VertexId,VertexId,Edge)
    val connections : RDD[Edge[EdgeAttr]] =
          edgeAndVertex.groupBy(_._1.uuid) //group all Rows by the UID
                        .map(x=>x._2) //Drop the Extra Grouping UID Key
                        .map(x=> (x.unzip._1.last, x.unzip._2) ) //Split in to a single Edge, and List of VertexIds (may be more than 2)
                        .map(x=>(x._1,halfcross(x._2,x._2) ) ) //Take the Lower Diagonal (~half cross product)of all pairs of vertexes
                        .flatMap(x=>{for{i <- x._2 } yield (i._1,i._2,x._1) } ) //Map to Triplet
                        .map(x=>Edge(x._1,x._2,x._3)) //Map to Edge

    connections
  }

  def assignVertexIds(packagedData : RDD[(EdgeAttr,VertexAttr)], uniqueVertexes : RDD[(VertexId,VertexAttr)] ) : RDD[(VertexId,VertexAttr)] = {
    packagedData.map(x=>x._2) //vertex
                .map(x=>(x,x)) //make a K/V
                .join(uniqueVertexes.map(x=>x.reverse)) //Join in VertexID
                .map(x=>x._2.reverse) //Flip around
  }
  /**
   * Lower half (Excluding the diagonal) of the cartesian
   * @param xs
   * @param ys
   * @tparam L
   * @return
   */
  def halfcross[L <: Long](xs: Traversable[L], ys: Traversable[L]) = for { x <- xs; y <- ys if x < y  } yield (x, y)
}




