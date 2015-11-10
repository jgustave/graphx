package com.ms.demo

import com.ms.util.Util.flatten
import org.apache.spark._
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import shapeless.syntax.std.tuple._

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

    //Assign a Unique ID to every distinct Vertex
    val uniqueVertexes: RDD[(VertexAttr,VertexId)] = dataRows.flatMap(x=>x.verticies).distinct().zipWithUniqueId().cache()

    //Join back in to the collection.
    val allVertexes: RDD[(VertexId,VertexAttr)] = dataRows.flatMap(x=>x.verticies).map(x=>(x,x)).join(uniqueVertexes).map(x=>(x._2._2,x._2._1))

    //Extract triplets from DataRows and assign proper UIDs to Vertex (V,V,E)
    val triplets = dataRows.flatMap( x =>
      for (i <- 0 to x.verticies.length - 2; j <- i + 1 until x.verticies.length)
        yield (x.verticies(i), x.verticies(j), x.edge)
    )

    //Get Edges, by joining UniqueIds to the Vertexes in the Tripplet
    val relationships: RDD[Edge[EdgeAttr]] = triplets.map(x=>(x._1,x.drop(1) )).join(uniqueVertexes).map(x=>x._2).map(x=>(x._1._1,(x._1._2,x._2))).join(uniqueVertexes).map(x=>x._2).map(x=> Edge(x._2,x._1._2,x._1._1) )

    val graph = Graph(allVertexes,relationships)

    val cc = graph.connectedComponents()


    for ((id, scc) <- cc.vertices.collect().sortBy(r => (r._2,r._1) )  ) {
      println(id,scc)
    }


  }
}




