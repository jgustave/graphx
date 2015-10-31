package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object HelloGraph2 {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    //val conf = new SparkConf().setAppName("Simple Application")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, VertexAttr)] =
      sc.parallelize(Array((1L,  new VertexAttr("cookie", "cookie:1")),
                           (2L,  new VertexAttr("cookie", "cookie:2")),
                           (3L,  new VertexAttr("cookie", "cookie:3")),
                           (4L,  new VertexAttr("cookie", "cookie:4")),
                           (5L,  new VertexAttr("cookie", "cookie:5")),
                           (6L,  new VertexAttr("cookie", "cookie:6")),
                           (7L,  new VertexAttr("email", "e:1")),
                           (8L,  new VertexAttr("idfa", "idfa:1")),
                           (9L,  new VertexAttr("idfa", "idfa:2")),
                           (10L, new VertexAttr("account", "a:1")),
                           (11L, new VertexAttr("account", "a:2")),
                           (12L, new VertexAttr("account", "a:3"))
      ))


    //Explicitly declaring the links.
    val links = Array(Edge(10L, 1L, new EdgeAttr("A",1230)), //a1 -> c1
                      Edge(10L, 2L, new EdgeAttr("B",1231)),
                      Edge(2L, 7L, new EdgeAttr("C",1232)),
                      Edge(7L, 3L, new EdgeAttr("C",1233)),  //e1 -> c3
                      Edge(3L, 8L, new EdgeAttr("D",1234)),
                      Edge(8L, 4L, new EdgeAttr("D",1235)),
                      Edge(8L, 5L, new EdgeAttr("B",1236)),
                      Edge(11L, 9L, new EdgeAttr("B",1237)),
                      Edge(12L, 9L, new EdgeAttr("A",1238))
                  )

    val relationships: RDD[Edge[EdgeAttr]] =  sc.parallelize( links )
    //Make the links bidirectional (undirected)
//    val relationships: RDD[Edge[String]] =  sc.parallelize( links.flatMap(x => Array(Edge[String](x.srcId ,x.dstId),
//                                                                                     Edge[String](x.dstId,x.srcId)) ) )


    val graph = Graph(users,relationships)

    val cc = graph.connectedComponents()

    /**
     * Sort by the component ID (The lowest vertexId in the component), and then the VertexID in the component.
     */
    for ((id, scc) <- cc.vertices.collect().sortBy(r => (r._2,r._1) )  ) {
      println(id,scc)
    }
    println("...")

    for (x <- graph.inDegrees  ) {
          println(x)
        }
println("...")
    for (x <- graph.outDegrees  ) {
          println(x)
        }

    println("...")
  for (x <- graph.triplets  ) {
        println(x)
      }

    println("Hellooo " + cc)

    val maxIter = 3


//http://www.cakesolutions.net/teamblogs/graphx-pregel-api-an-example
    val initialMsg = "first"
    val pgraph     = graph.pregel(initialMsg, maxIter, EdgeDirection.Either)( vprog, sendMessage, mergeMsg )

  }
//
  /**
   * is the user defined function to determine the messages to send out for the next iteration and where to send it to.
   * Given an Edge Triplet, return an iterator of who to send a message to, and the message
   * ((Vertex,Msg),(Vertex,Msg))
   */
  def sendMessage(edge: EdgeTriplet[VertexAttr, EdgeAttr]): Iterator[(VertexId, String)] = {
    println("Send " + edge.attr )

        if( edge.srcAttr.vertexValue == "cookie" ) {
          Iterator.empty
        }else {
          Iterator((edge.srcId, "bar"))
        }
//        if (edge.srcAttr < edge.dstAttr) {
//          Iterator((edge.dstId, ""))
//        } else if (edge.srcAttr > edge.dstAttr) {
//          Iterator((edge.srcId, edge.dstAttr))
//        } else {
//          Iterator.empty
//        }
  }

  /**
   * Merge(Reduce) operation that combines all messages received.. Before applying the VertexProgram vprog
   * @param m1
   * @param m2
   * @return
   */
  def mergeMsg(m1: String, m2: String) : String = {
    println("merge: " + m1 + " " + m2 )
    m1 + " " + m2
  }

  /**
   * Function for receiving Messages
   * @param vertexId
   * @param vertexAttr
   * @param msg
   * @return
   */
  def vprog(vertexId: VertexId, vertexAttr: VertexAttr, msg: String ): VertexAttr = {
    println("vprog " + vertexAttr + " " + msg )
    vertexAttr
//    if (message == initialMsg)
//      value
//    else
//      (message min value._1, value._1)
  }

  def bidir( x : Edge[String] ) : Array[Edge[String]]  = {
    Array(Edge(x.srcId ,x.dstId), Edge(x.dstId,x.srcId))
  }

  /**
   * Properties
   *
   *
   * Vertex Properties:
   * - Type (cookie)
   * - Value
   *
   *
   *
   * Edge Properties:
   * - Source of the Edge
   * - DateTime of the link
   *
   *
   */
}




