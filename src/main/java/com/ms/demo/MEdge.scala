package com.ms.demo
import org.apache.spark.graphx._

/**
 * An Edge for the MS Id Graph
 * We ignore Edge direction.
 *
 * @param dataSource This is the ~file that the link came from
 * @param dateTime This is the dateTime that the Link was observed.
 */
class MEdge(srcId : VertexId,
            dstId : VertexId,
            val dataSource : String,
            val dateTime : Long )
  extends Edge(srcId,dstId) {
  //val dataSource = dataSource;
  override def toString = s"MEdge($srcId, $dstId, $dataSource, $dateTime)"
}
