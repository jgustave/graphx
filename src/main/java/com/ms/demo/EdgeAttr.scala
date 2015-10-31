package com.ms.demo

/**
 *
 */
class EdgeAttr(val dataSource : String,
            val dateTime : Long) extends Serializable {


  override def toString = s"EInfo(dataSource=$dataSource, dateTime=$dateTime)"
}
