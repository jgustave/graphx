package com.ms.demo

import com.ms.util.Util._
import shapeless.syntax.std.tuple._

/**
 * http://spark.apache.org/docs/latest/quick-start.html
 *
 */
object SDemo {

  def main( args: Array[String] ) = {
    println("Hello World Graph")

    val sfoo = Array("A","B","C","D")

    for (i <- 0 to sfoo.length-2; j <- i+1 until sfoo.length) {
      println(sfoo(i),sfoo(j))
    }
        //yield (i, j)

    val zz = ("foo","bar","other").drop(1)

    println(zz)

//
//
//
//    val foobar = (1,2,"Three")
//    val zz = foobar.reverse
//
//    val tup = (1,"foo",("bar",3.4),"ender")
//    println(tup)
//
//    val flat = flatten(tup)
//    println(flatten(tup))
//
//    var t1 = ("a","b",2)
//    val t2 = t1 :+ "foo"
//    val t3 = "bar" +: t1
//
//    println(t1)
//    println(t2)
//    println(t3)
//    println(zz)
  }

//  def bidir(x : Long) : Array[Long] = {
//    Array(x,100+x)
//  }

}



