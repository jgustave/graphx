package com.ms.demo

/**
 *
 */
case class EdgeAttr(dataSource : String,
                    dateTime : Long,
                    uuid: String = "") extends Serializable {


  override def toString = s"EInfo(dataSource=$dataSource, dateTime=$dateTime)"


  def canEqual(other: Any): Boolean = other.isInstanceOf[EdgeAttr]

  override def equals(other: Any): Boolean = other match {
    case that: EdgeAttr =>
      (that canEqual this) &&
      dataSource == that.dataSource &&
      dateTime == that.dateTime
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(dataSource, dateTime)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
