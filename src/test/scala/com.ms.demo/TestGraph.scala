package com.ms.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.{After, Before, Test, Assert}
//import org.junit.Assert


/**
  *
  */
class TestGraph {

  var sc : SparkContext = null

  @Before
  def before(): Unit = {
    println("Before")
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    sc = createSparkContext()
  }

  @After
  def after(): Unit = {
    println("After")

    sc.stop()
    sc = null
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }


  @Test
  def testNothing(): Unit = {

    println("Hello Worldsss")



    val lines = Array("row1,click,1234,account,1",
                      "row2,click,1235,account,2" )
    val inputData :RDD[String] = sc.parallelize(lines)
    val parsed : RDD[(String,String,String,String,String)] = GraphDemo.parseRawData(',',inputData)

    Assert.assertEquals(2,parsed.count())
    Assert.assertTrue(parsed.collect().contains(("row1","click","1234","account","1") ))
    Assert.assertTrue(parsed.collect().contains(("row2","click","1235","account","2") ))
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
