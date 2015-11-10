package com.ms.demo
import shapeless.syntax.std.tuple._

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object SDemo {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    val foobar = (1,2,"Three")
    val zz = foobar.reverse

    println(zz)
  }

//  def bidir(x : Long) : Array[Long] = {
//    Array(x,100+x)
//  }

}



