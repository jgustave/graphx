package com.ms.demo

import org.apache.spark._
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 *
 *
 *  spark.rdd.compress true
       spark.storage.memoryFraction 1
       spark.core.connection.ack.wait.timeout 600
       spark.akka.frameSize 50
 *
 * spark.network.timeout
 *
 * --num-executors 3 --driver-memory 1g --executor-memory 2g --executor-cores 1
 *
 * spark.task.maxfailures
 * /opt/spark/bin/spark-submit --verbose --conf spark.task.maxFailures=40 --master yarn --num-executors 400 --executor-memory 4g --driver-memory 1g --executor-cores 2 --deploy-mode cluster --driver-class-path $(find /opt/hadoop/share/hadoop/mapreduce/lib/hadoop-lzo-* | head -n 1) --queue hive-delivery-high --class com.ms.demo.GraphDemo ~/tmp/graph2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/jeremy/graph/raw_pixel/rawgraph hdfs:///user/jeremy/graph/demoout2
 * /opt/spark/bin/spark-submit --verbose --conf spark.task.maxFailures=40 --master yarn --num-executors 200 --executor-memory 8g --driver-memory 1g --executor-cores 2 --deploy-mode cluster --driver-class-path $(find /opt/hadoop/share/hadoop/mapreduce/lib/hadoop-lzo-* | head -n 1) --queue hive-delivery-high --class com.ms.demo.GraphDemo ~/tmp/graph2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/jeremy/graph/raw_pixel/raw_graph_lzo hdfs:///user/jeremy/graph/demoout1 200
 *
 * /opt/spark/bin/spark-submit --verbose --conf spark.task.maxFailures=100 --conf spark.rdd.compress=true --master yarn --num-executors 100 --executor-memory 12g --driver-memory 2g --executor-cores 1 --deploy-mode cluster --driver-class-path $(find /opt/hadoop/share/hadoop/mapreduce/lib/hadoop-lzo-* | head -n 1) --queue hive-delivery-high --class com.ms.demo.GraphDemo ~/tmp/lcp2.jar hdfs:///user/jeremy/graph/raw_pixel/raw_graph_lzo hdfs:///user/jeremy/graph/demoout111 200
 *
 * /opt/spark/bin/spark-submit --verbose --conf spark.task.maxFailures=100 --conf spark.rdd.compress=true --master yarn --num-executors 200 --executor-memory 12g --driver-memory 2g --executor-cores 1 --deploy-mode cluster --driver-class-path $(find /opt/hadoop/share/hadoop/mapreduce/lib/hadoop-lzo-* | head -n 1) --queue hive-delivery-high --class com.ms.demo.GraphDemo ~/tmp/lcp4.jar hdfs:///user/jeremy/graph/raw_pixel/raw_graph_lzo hdfs:///user/jeremy/graph/demoout333 200
 * 6g works.. 4g doesn't
 *
 * This takes additional steps..
 * There is a vertex_type hierarchy, and it will pick a final vertex to represent the cluster.
 * It uses the row_uuid as a simple way to join the result back on to the source data.
 *
 *
 *
 */
object GraphDemo2 {

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

    //Enable Kryo Serialization and Register some classes.
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[EdgeAttr], classOf[VertexAttr]))

    val sc   = new SparkContext(conf)
    val inputPath     = args(0)
    val outputPath    = args(1)
    val numPartitions = Integer.parseInt(args(2))

    sc.setCheckpointDir(outputPath+"/tmp/")

    println("A")

    val rawParsedData : RDD[(String,String,String,String,String)] = parseRawData(sc.textFile(inputPath).repartition(numPartitions))


    //TODO: Exception handling...
    //Convert to Objects
    println("B")
    val packagedData : RDD[(EdgeAttr,VertexAttr)] = packageRawData(rawParsedData)

    println("C")
    //Get Unique IDs for Vertexes
    val uniqueVertexes : RDD[(VertexId,VertexAttr)] = getUniqueVertexIds( packagedData )

    //It's important to persist here for two reasons.
    // 1 .. the longer the chain of unpersisted computations, the more likely Spark seems to start thrashing / recomputing
    // 2 .. Since we are zipping with a random unique ID.. If it gets recalculated, then we will have inconsistent/non deterministic results. So must persist.
    uniqueVertexes.persist(StorageLevel.MEMORY_AND_DISK_SER)
    packagedData.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //Assign Unique IDS to All
    println("D")
    val allVertexes : RDD[(VertexId,VertexAttr)] = assignVertexIds(packagedData,uniqueVertexes)
    allVertexes.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("E")
    val connections : RDD[Edge[EdgeAttr]] = createEdges(packagedData,uniqueVertexes)
    connections.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //Create Graph
    println("F")
    val graph = Graph(allVertexes,connections)
    //graph.persist(StorageLevel.MEMORY_AND_DISK)

    //Get Connected Components
    println("G")
    val cc = graph.connectedComponents()

    //(VertexId,CCId)
      //Select CC with more than one account.
      //

    //Map back to CCID,VertexType,VertexValue
    println("H")

//    val assignedGroups : RDD[(VertexId,VertexAttr)] = cc.vertices.join(allVertexes)

//                                    .map(x=>(x._2._1,x._2._2.vertexType,x._2._2.vertexValue) )
//                                    .distinct();


//    val assignedGroups = cc.vertices.join(allVertexes)
//                                    .map(x=>(x._2._1,x._2._2.vertexType,x._2._2.vertexValue) )
//                                    .distinct()
//                                    .map(x=>flatProduct(x).mkString(","))

    println("I")
//    assignedGroups.saveAsTextFile(outputPath +"/out/")
  }


  /**
    * Get delimited columns.
    * @param rawInput
    * @return
    */
  def parseRawData( rawInput :RDD[String] ) : RDD[(String,String,String,String,String)] = {

    rawInput.map(x=>x.split('\1'))
            //.filter(_.length!=5)
            .map(x=>(x(0),x(1),x(2),x(3),x(4)))
            .filter(x=> !isNull(x._1) && !isNull(x._2) && !isNull(x._3) && !isNull(x._4) && !isNull(x._5) )
  }

  /**
   * Convert Strings to Objects
   * The input format is row_uuid,source,utcMillis,keyType,keyValue
   * Which represents a row I saw in a file, that may show 2 or more vertexes (keyType,keyValue), which are implicitly connected by an edge
   *
   * 800ed948-2203-411d-847c-d02339091b02,click,1437857236,mm_account,\N
   * 800ed948-2203-411d-847c-d02339091b02,click,1437857236,mm_cookie,2riJxPFgjuFnTpG3RVIHjF7pSCw-Qxm82YE
   * 800ed948-2203-411d-847c-d02339091b02,click,1437857236,mm_uuid,b3f45579-7f9d-4b00-98af-afe9c570903a
   * 800ed948-2203-411d-847c-d02339091b02,click,1437857236,mm_emailid,\N
   * 800ed948-2203-411d-847c-d02339091b02,click,1437857236,orderid,\N
   *
   * The above shows that two nodes: {mm_uuid,b3f45579-7f9d-4b00-98af-afe9c570903a} and {mm_cookie,2riJxPFgjuFnTpG3RVIHjF7pSCw-Qxm82YE}
   * that are implicitly linked together.
   *
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




