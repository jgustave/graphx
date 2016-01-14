package com.ms.demo

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.{After, Before, Test }
import org.junit.Assert._
import GraphDemo._


/**
  *
  */
class TestGraph extends Serializable {

  @transient
  var sc : SparkContext = null

  @Before
  def before(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    sc = createSparkContext()
  }

  @After
  def after(): Unit = {
    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }


  @Test
  def testParsing(): Unit = {

    val lines = Array("row1,click,1234,account,1",
                      "row2,click,1235,account,2" )
    val inputData :RDD[String] = sc.parallelize(lines)
    val rawParsedData : RDD[(String,String,String,String,String)] = GraphDemo.parseRawData(',',inputData)

    assertEquals(2,rawParsedData.count())
    assertTrue(rawParsedData.collect().contains(("row1","click","1234","account","1") ))
    assertTrue(rawParsedData.collect().contains(("row2","click","1235","account","2") ))
  }

  @Test
  def testPackage(): Unit = {

    val lines = Array("row1,click,1234,account,1",
                      "row2,click,1235,account,2" )
    val inputData :RDD[String] = sc.parallelize(lines)
    val rawParsedData : RDD[(String,String,String,String,String)] = parseRawData(',',inputData)
    val packagedData : RDD[(EdgeAttr,VertexAttr)] = packageRawData(rawParsedData)


    assertEquals(2,packagedData.count())
    assertTrue(packagedData.collect().contains((EdgeAttr("click",1234,"row1"),VertexAttr("account","1")) ))
    assertTrue(packagedData.collect().contains((EdgeAttr("click",1235,"row2"),VertexAttr("account","2")) ))
  }

  @Test
  def testUniqueVertexes(): Unit = {

    val lines = Array("row1,click,1234,account,1",
                      "row2,click,1235,account,2",
                      "row3,click,1236,account,2")
    val inputData :RDD[String] = sc.parallelize(lines)
    val rawParsedData : RDD[(String,String,String,String,String)] = parseRawData(',',inputData)
    val packagedData : RDD[(EdgeAttr,VertexAttr)] = packageRawData(rawParsedData)
    val uniqueVertexes : RDD[(VertexId,VertexAttr)] = getUniqueVertexIds( packagedData )


    assertEquals(2,uniqueVertexes.count())
    assertTrue(uniqueVertexes.map(x=>x._2).collect().contains(VertexAttr("account","1") ))
    assertTrue(uniqueVertexes.map(x=>x._2).collect().contains(VertexAttr("account","2") ))
  }


  @Test
  def testCC(): Unit = {

    val edges = Array(Edge(1L,2L,EdgeAttr("src",1,"row1")),   //link
                      Edge(1L,2L,EdgeAttr("src",1,"row2")),   //dupe on diff row
                      Edge(1L,3L,EdgeAttr("src",1,"row3")), //bridge in
                      Edge(1L,4L,EdgeAttr("src",1,"row3")), //bridge in
                      Edge(5L,6L,EdgeAttr("src",1,"row4")) //island
    )
    val vertexes = Array((1L,VertexAttr("a","1")),
                         (2L,VertexAttr("a","2")),
                         (3L,VertexAttr("b","3")),
                         (4L,VertexAttr("c","4")),
                         (5L,VertexAttr("b","5")),
                         (6L,VertexAttr("c","6"))
    )

    val connections : RDD[Edge[EdgeAttr]] = sc.parallelize(edges)
    val uniqueVertexes : RDD[(VertexId,VertexAttr)] = sc.parallelize(vertexes)

    val graph = Graph(uniqueVertexes,connections)
    val cc =  graph.connectedComponents()

    //(VertexId,ComponentId)

    //(ComponentId,VertexAttr)
    val assignedGroups : RDD[(Long,VertexAttr)]= cc.vertices.join(uniqueVertexes).map(x=>x._2 )

    //val bar = assignedGroups.collect()
    //assignedGroups.gr

    //Get to CCID,VertexAttr
    //Resolve to CCID,BestVertexAttr

    //row_uuid,CCID,BestVertexAttr

    //Join from Packaged VertexAttr,row_uuid

    //Get the VertexAttr of the VertexId

//    assertEquals(2,uniqueVertexes.count())
//    assertTrue(uniqueVertexes.map(x=>x._2).collect().contains(VertexAttr("account","1") ))
//    assertTrue(uniqueVertexes.map(x=>x._2).collect().contains(VertexAttr("account","2") ))
    println("")
  }


  @Test
  def testFold(): Unit = {

    val data = Array((1,("a",2)),
                     (1,("b",1)),
                     (1,("a",3)),
                     (2,("a",4))
    )

    val dataRdd  = sc.parallelize(data)

    implicit val fooOrdering = new Ordering[(String,Int)] {
         override def compare(a: (String,Int), b: (String,Int)) = -1 * a._2.compare(b._2)
    }



//    var sorted = dataRdd.sortByKey()
//    var bar = sorted.collect()


//    //dataRdd.foldByKey( ("",1) )(folder)
    //var folded = dataRdd.foldByKey( null ){(a,b)=> if (a==null) b else if (b==null) a else if( a._1.compare(b._1) >0) a else b }

    var folded = dataRdd.foldByKey( null ){(a,b)=> {
      if (a == null) b
      else if (b == null) a

      val comp = foo(a._1).compare(foo(b._1))
      if (comp > 0) a
      else if (comp < 0) b
      else b

    }
    }


    val bar = folded.collect()

    println("")
  }

  def folder(a : (String,Int), b : (String,Int) ) : (String,Int) = {
    b
  }

  def foo( input : String ) : Int = {
    input match {
      case "b" => 3
      case "subject" => 2
      case "mm_uuid" => 1
      case _ => 0
    }
  }

  /**
    * TODO: Figure out DependencyInjection
    * @return
    */
  private def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc   = new SparkContext(conf)

    sc
  }


}
