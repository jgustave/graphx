package com.ms.demo

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object SDemo {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    val foo = Array(1L,2L,3L)

    println("Hellooo " + foo.length )

    //def bidir(v:Long) = Array(v, 100+v)

    val result = foo.flatMap(x => Array(x,1000+x) )
    result.foreach(println(_))
  }

//  def bidir(x : Long) : Array[Long] = {
//    Array(x,100+x)
//  }

}



