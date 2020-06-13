import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * value方法：获取累加器中的值
  *
  * merge方法：该方法特别重要，一定要写对，这个方法是各个task的累加器进行合并的方法（下面介绍执行流程中将要用到）
  *
  * iszero方法：判断是否为初始值
  *
  * reset方法：重置累加器中的值
  *
  * copy方法：拷贝累加器
  */
class SessionAccumlator extends AccumulatorV2[String, mutable.HashMap[String, Int]]  {
  val countMap = new mutable.HashMap[String, Int]()


  override def isZero: Boolean = {
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionAccumlator
    acc.countMap++=this.countMap
    acc
  }

  override def reset(): Unit = {
    countMap.clear()
  }

  override def add(v: String): Unit = {
    if (!countMap.contains(v)){
      this.countMap+=(v->0)
    }
    this.countMap.update(v,countMap(v)+1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: SessionAccumlator => acc.countMap.foldLeft(this.countMap) {
        case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
