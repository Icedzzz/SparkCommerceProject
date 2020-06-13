/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 11/3/17 3:22 PM.
 * Author: wuyufei.
 */

package com.atguigu.mock

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by wuyufei on 06/09/2017.
  */
/**
  * Kafka 连接工具类
  *
  * @param brokerList   kafka地址
  * @param defaultTopic kafka默认的主题
  */
case class KafkaProducerProxy(brokerList: String, defaultTopic: Option[String] = None) {

  type Key = String
  type Val = String

  require(brokerList == null || !brokerList.isEmpty, "Must set broker list")

  private val producer = {

    var props: Properties = new Properties()
    props.put("bootstrap.servers", brokerList)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)

  }

  /**
    * 将消息转换为ProducerRecord
    *
    * @param value
    * @param key
    * @param topic
    * @return
    */
  private def toMessage(value: Val, key: Option[Key] = None, topic: Option[String] = None): ProducerRecord[Key, Val] = {
    val t = topic.getOrElse(defaultTopic.getOrElse(throw new IllegalArgumentException("Must provide topic or default topic")))
    require(!t.isEmpty, "Topic must not be empty")
    key match {
      case Some(k) => new ProducerRecord(t, k, value)
      case _ => new ProducerRecord(t, value)
    }
  }

  def send(key: Key, value: Val, topic: Option[String] = None) {
    //调用KafkaProducer他的send方法发送消息
    producer.send(toMessage(value, Option(key), topic))
  }

  def send(value: Val, topic: Option[String]) {
    send(null, value, topic)
  }

  def send(value: Val, topic: String) {
    send(null, value, Option(topic))
  }

  def send(value: Val) {
    send(null, value, None)
  }

  /**
    * 关闭客户端
    */
  def shutdown(): Unit = producer.close()

}
