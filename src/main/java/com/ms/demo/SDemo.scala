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

    def bidir(v:Long) = Array(v, 100+v)

    val result = foo.flatMap(x => bidir(x) )
    result.foreach(println(_))
  }

  def bidir(vals : Array[Long]) : Array[Long] = {
    vals
  }

}



