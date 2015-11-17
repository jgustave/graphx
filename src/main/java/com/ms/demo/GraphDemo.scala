package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import shapeless.syntax.std.tuple._

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 * What next?
 *  - Extract test data.. Who? What types?
 *  -- Pixel: (V1:MM UUID) (V2 MM-Account), (V3 FP Cookie (PIXEL SessionID)), (V4 MM EmailID), (V5 OrderID), IDFA, IDFV
 *  -- Trans: Account, OrderId
 *
 *  - What output?
 *  -- CCID,VertexId
 *
 * /attribution/J140012/etl/input_staged/pixel
 *
 * select v1,v2,v3,v4,v5
 *  #U is for Page_URL sourced data
 hive -e "
     drop table $pixel_U;
     create table $pixel_U as
     select
         parse_url(DPAGE_URL, 'QUERY','email') as email_address,
         MM_UUID
     from $pixel_3
     where trim(parse_url(DPAGE_URL, 'QUERY','email')) != '';"
 checkError $HIVE_EXECUTION_ERROR "Unable to create $pixel_U Table"

 #R is for Referrer
 hive -e "
     drop table $pixel_R;
     create table $pixel_R as
     select
        parse_url(DREF_URL, 'QUERY','email') as email_address,
        MM_UUID
     from $pixel_3
     where trim(parse_url(DREF_URL, 'QUERY','email')) != /'/';"  NOTe.. was empty string
 checkError $HIVE_EXECUTION_ERROR "Unable to create $pixel_R Table"

drop table $orderid2;
create table $orderid2 as
select
    a.mm_uuid,
    a.orderid,
    b.subject
from $orderid a
join ${online_tran} b
    on a.orderid=b.order_number;"

 OrderID -> MM_UID  pick highest Sales... Look at Times...etc  (Should be 1:1, but if not..)

 create table pixel_small as
select reflect("java.util.UUID", "randomUUID") as row_uuid, unix_timestamp(timestamp_gmt) as utc_millis,mm_uuid,v2 as mm_account, v3 as pixel_session, v4 as emailid, v5 as orderid,
concat(V6,coalesce(V7,' '),coalesce(V8,' '),coalesce(V9,' '),coalesce(V10,' '),coalesce(S1,' '),coalesce(S2,' '),coalesce(S3,' '),coalesce(S4,' '),coalesce(S5,' '),coalesce(S6,' '),coalesce(S7,' '),coalesce(S8,' '),coalesce(S9,' '),coalesce(S10,' ')) as REF_URL,
REFERRER as PAGE_URL from pixel

 create table rawgraph as
 select row_uuid, 'click' as source, utc_millis, 'mm_uuid' as key_type, mm_uuid as key_value from pixel_small
UNION ALL
 select row_uuid, 'click' as source, utc_millis, 'mm_account' as key_type, mm_account as key_value from pixel_small
UNION ALL
 select row_uuid, 'click' as source, utc_millis, 'mm_cookie' as key_type, pixel_session as key_value from pixel_small
UNION ALL
 select row_uuid, 'click' as source, utc_millis, 'mm_emailid' as key_type, emailid as key_value from pixel_small
UNION ALL
 select row_uuid, 'click' as source, utc_millis, 'orderid' as key_type, orderid as key_value from pixel_small

 *
 *
 * --num-executors 3 --driver-memory 1g --executor-memory 2g --executor-cores 1
 *
 * spark.task.maxfailures
 * /opt/spark/bin/spark-submit --verbose --master yarn --num-executors 400 --executor-memory 4g --driver-memory 1g --executor-cores 2 --deploy-mode cluster --driver-class-path $(find /opt/hadoop/share/hadoop/mapreduce/lib/hadoop-lzo-* | head -n 1) --queue hive-delivery-high --class com.ms.demo.GraphDemo ~/tmp/graph2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/jeremy/graph/raw_pixel/rawgraph hdfs:///user/jeremy/graph/demoout2
 * /opt/spark/bin/spark-submit --verbose --conf spark.task.maxfailures=30 --master yarn --num-executors 400 --executor-memory 4g --driver-memory 1g --executor-cores 2 --deploy-mode cluster --driver-class-path $(find /opt/hadoop/share/hadoop/mapreduce/lib/hadoop-lzo-* | head -n 1) --queue hive-delivery-high --class com.ms.demo.GraphDemo ~/tmp/graph2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/jeremy/graph/raw_pixel/raw_graph_lzo hdfs:///user/jeremy/graph/demoout9
 */
object GraphDemo {

  def isNull (input: String ): Boolean = {
    if( input == null )
      true
    val clean = input.trim

    clean.length == 0  ||  clean.equalsIgnoreCase("null") || clean.equalsIgnoreCase("\\N")
  }


  def main( args: Array[String] ) = {
    println("Hello Graph Demo")

    //conf.set("textinputformat.record.delimiter", "\u0001")
    val conf = new SparkConf().setAppName("Graph Demo")
    val sc   = new SparkContext(conf)


    //UUID,source,time,type,val
//    val rawData = sc.parallelize(Array(
//                                    ("uuid1","click","1234","cookie","cookie:1"),
//                                    ("uuid1","click","1234","orderid","order:1"),
//                                    ("uuid2","click","1235","cookie","cookie:2"),
//                                    ("uuid2","click","1235","orderid","order:2"),
//                                    ("uuid3","click","1236","cookie","cookie:3"),
//                                    ("uuid3","click","1236","orderid","order:2"),
//                                    ("uuid3","click","1236","foo","foo:3")) )

    println("A")

    val rawData = sc.textFile(args(0)).repartition(128).map(x=>x.split('\1')).map(x=>(x(0),x(1),x(2),x(3),x(4))).filter(x=> !isNull(x._1) && !isNull(x._2) && !isNull(x._3) && !isNull(x._4) && !isNull(x._5) )

    //val lineLengths = lines.map(s => s.length)

    //TODO: Exception handling...
    //Convert to Objects
    println("B")
    val packagedData : RDD[(EdgeAttr,VertexAttr)] = packageRawData(rawData)

    println("C")
    //Get Unique IDs for Vertexes
    val uniqueVertexes : RDD[(VertexId,VertexAttr)] = getUniqueVertexIds( packagedData )

    //Assign Unique IDS to All
    println("D")
    val allVertexes : RDD[(VertexId,VertexAttr)] = assignVertexIds(packagedData,uniqueVertexes)

    println("E")
    val connections : RDD[Edge[EdgeAttr]] = createEdges(packagedData,uniqueVertexes)

    //Create Graph
    println("F")
    val graph = Graph(allVertexes,connections)

    //Get Connected Components
    println("G")
    val cc = graph.connectedComponents()

    //(VertexId,CCId)
      //Select CC with more than one account.
      //

    //Map back to CCID,VertexType,VertexValue
    println("H")
    val assignedGroups = cc.vertices.join(allVertexes)
                                    .map(x=>(x._2._1,x._2._2.vertexType,x._2._2.vertexValue) )
                                    .distinct()
                                    .map(x=>flatProduct(x).mkString(","))

    println("I")
    assignedGroups.saveAsTextFile(args(1))
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
                .map(x=>(x._2,x._1))
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
                                                             .join(uniqueVertexes.map(x=>(x._2,x._1))) //Join in VertexId
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
                .join(uniqueVertexes.map(x=>(x._2,x._1))) //Join in VertexID
                .map(x=>(x._2._2,x._2._1)) //Flip around
  }
  /**
   * Lower half (Excluding the diagonal) of the cartesian
   * @param xs
   * @param ys
   * @tparam L
   * @return
   */
  def halfcross[L <: Long](xs: Traversable[L], ys: Traversable[L]) = for { x <- xs; y <- ys if x < y  } yield (x, y)

  def flatProduct(t: Product): Iterator[Any] = t.productIterator.flatMap {
    case p: Product => flatProduct(p)
    case x => Iterator(x)
  }
}




