package com.ms.demo

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object SDemo {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    val foo = Array(1L,2L,3L)
    val result = foo.flatMap(x => Array(x,1000+x) )
    result.foreach(println(_))

    val foo2 = new MEdge(1,2,"Email",4)

    println(foo2 )

  }

//  def bidir(x : Long) : Array[Long] = {
//    Array(x,100+x)
//  }

}



