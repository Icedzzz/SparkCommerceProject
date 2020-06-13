/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 11/1/17 2:46 PM.
 * Author: wuyufei.
 */

package com.atguigu.mock

import com.atguigu.commons.conf.ConfigurationManager
import scala.util.Random


/**
  * 模拟的数据
  * 时间点: 当前时间毫秒
  * userId: 0 - 99
  * 省份、城市 ID相同 ： 1 - 9
  * adid: 0 - 19
  * ((0L,"北京","北京"),(1L,"上海","上海"),(2L,"南京","江苏省"),(3L,"广州","广东省"),(4L,"三亚","海南省"),(5L,"武汉","湖北省"),(6L,"长沙","湖南省"),(7L,"西安","陕西省"),(8L,"成都","四川省"),(9L,"哈尔滨","东北省"))
  * 格式 ：timestamp province city userid adid
  * 某个时间点 某个省份 某个城市 某个用户 某个广告
  */
object MockRealtimeDataGenerate {

  private def mockBatchRealtimeData(): Array[(Long,Int,Int,Int,Int)] = {

    val random = new Random()

    val batchClick = for (i <- 0 to 50) yield {
      val userid = random.nextInt(50)
      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(10)
      val city = province
      val adid = random.nextInt(20)

      (timestamp,province,city,userid,adid)
    }
    batchClick.toArray
  }

  def main(args: Array[String]): Unit = {

    val broker_list = ConfigurationManager.config.getString("kafka.broker.list")
    val topics = ConfigurationManager.config.getString("kafka.topics")

    val kafkaProxy = KafkaProducerProxy(broker_list, Some(topics))

    while (true){

      val msgs = mockBatchRealtimeData()
      msgs.foreach{ case (timestamp,province,city,userid,adid) =>
        kafkaProxy.send(timestamp + " " + province + " " + city + " " + userid + " " + adid)
        println(timestamp + " " + province + " " + city + " " + userid + " " + adid)
      }
      Thread.sleep(500)
    }

    kafkaProxy.shutdown()
  }

}
