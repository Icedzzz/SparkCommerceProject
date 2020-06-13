/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 10/31/17 8:29 PM.
 * Author: wuyufei.
 */

package com.atguigu.mock

import java.util.UUID

import com.atguigu.commons.model.{ProductInfo, UserInfo, UserVisitAction}
import com.atguigu.commons.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.runtime.Nothing$
import scala.util.Random


/**
  * 模拟的数据
  * date：是当前日期
  * age: 0 - 59
  * professionals: professional[0 - 59]
  * cities: 0 - 9
  * sex: 0 - 1
  * keywords: ("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
  * categoryIds: 0 - 99
  * ProductId: 0 - 99
  */
object MockDataGenerate {

  /**
    * 模拟用户行为信息
    *
    * @return
    */
  private def mockUserVisitActionData(): Array[UserVisitAction] = {

    val searchKeywords = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val date = DateUtils.getTodayDate()
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()


    for (i <- 0 to 100) {
      val userid = random.nextInt(100)
      for (j <- 0 to 10) {
        val sessionid = UUID.randomUUID().toString().replace("-", "")
        val baseActionTime = date + " " + random.nextInt(23)
//        var clickCategoryId: Long = -1L

        for (k <- 0 to random.nextInt(100)) {
          val pageid = random.nextInt(10)
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickProductId: Long = -1L
          var clickCategoryId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong
          val action = actions(random.nextInt(4))

          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => if (clickCategoryId == -1L)
              clickCategoryId = String.valueOf(random.nextInt(100)).toLong
              println("DEBUG clickCategoryId= " + clickCategoryId)
              clickProductId = String.valueOf(random.nextInt(100)).toLong
              println("DEBUG clickProductId= " + clickProductId)
            case "order" => orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityid)

        }

      }

    }
    rows.toArray
  }

  /**
    * 模拟用户信息表
    *
    * @return
    */
  private def mockUserInfo(): Array[UserInfo] = {

    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    for (i <- 0 to 100) {
      val userid = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))
      rows += UserInfo(userid, username, name, age,
        professional, city, sex)
    }
    rows.toArray
  }

  /**
    * 模拟产品数据表
    *
    * @return
    */
  private def mockProductInfo(): Array[ProductInfo] = {

    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)

    for (i <- 0 to 100) {
      val productId = i
      val productName = "product" + i
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      rows += ProductInfo(productId, productName, extendInfo)
    }

    rows.toArray
  }

  /**
    * 将DataFrame插入到Hive表中
    *
    * @param spark     SparkSQL客户端
    * @param tableName 表名
    * @param dataDF    DataFrame
    */
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

  /**
    * 主入口方法
    *
    * @param args 启动参数
    */
  def main(args: Array[String]): Unit = {

    // 创建Spark配置
    val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    // 创建Spark SQL 客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 模拟数据
    val userVisitActionData = this.mockUserVisitActionData()
    val userInfoData = this.mockUserInfo()
    val productInfoData = this.mockProductInfo()

    // 将模拟数据装换为RDD
    val userVisitActionRdd = spark.sparkContext.makeRDD(userVisitActionData)
    val userInfoRdd = spark.sparkContext.makeRDD(userInfoData)
    val productInfoRdd = spark.sparkContext.makeRDD(productInfoData)

    // 加载SparkSQL的隐式转换支持
    import spark.implicits._

    // 将用户访问数据装换为DF保存到Hive表中
    val userVisitActionDF = userVisitActionRdd.toDF()
    insertHive(spark, USER_VISIT_ACTION_TABLE, userVisitActionDF)

    // 将用户信息数据转换为DF保存到Hive表中
    val userInfoDF = userInfoRdd.toDF()
    insertHive(spark, USER_INFO_TABLE, userInfoDF)

    // 将产品信息数据转换为DF保存到Hive表中
    val productInfoDF = productInfoRdd.toDF()
    insertHive(spark, PRODUCT_INFO_TABLE, productInfoDF)

    spark.close
  }

}
