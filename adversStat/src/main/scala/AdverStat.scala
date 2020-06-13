import java.lang
import java.util.Date

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @author zwt
  * @date 2020/6/10 22:03
  * @version 1.0
  */
object AdverStat {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("adverStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()
    val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))


    val kafka_brokers=ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics=ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest : 先去zk中获取offset,如果有,直接使用,如果没有.从最新的数据开始消费
      // earlist: 先去zk中获取offset,如果有,直接获取,如果没有,从最开始的数据开始消费
      // none: 先去zk中获取offset,如果有,直接使用,如果没有,直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    /**LocationStrategies
      * PreferBrokers:Use this only if your executors are on the same nodes as your Kafka brokers.
      * PreferConsistent:Use this in most cases, it will consistently distribute partitions across all executors.
      * PreferFixed:Use this to place particular TopicPartitions on particular hosts if your load is uneven.
      * * Any TopicPartition not specified in the map will use a consistent location.
      */
    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )
    //adRealTimeDStream=>(k,v) (null,info)
    //String: timestamp   province    city   userid    adid
    val adRealTimeValueDStream: DStream[String] = adRealTimeDStream.map(item=>item.value())
    val filterBlackListRDD: DStream[String] = adRealTimeValueDStream.transform(
      logRDD => {
        val blacklists: Array[AdBlacklist] = AdBlacklistDAO.findAll()
        val blackListUserID: Array[Long] = blacklists.map(item => item.userid)
        val filterUserIdLogRDD: RDD[String] = logRDD.filter {
          case log =>
            val userid = log.split(" ")(3)
            !blackListUserID.contains(userid)
        }
        filterUserIdLogRDD
      }
    )
    streamingContext.checkpoint("./spark-streaming")
    filterBlackListRDD.checkpoint(Duration(10000))
    // 需求一：实时维护黑名单
    generateBlackList(filterBlackListRDD)
    // 需求二：各省各城市一天中的广告点击量（累积统计）
     val ClickCountAggDStream: DStream[(String, Long)] = provinceCityClickStat(filterBlackListRDD)
    // 需求三：统计各省Top3热门广告
    proveinceTope3Adver(sparkSession, ClickCountAggDStream)
    // 需求四：最近一个小时广告点击量统计
    getRecentHourClickCount(filterBlackListRDD)



    streamingContext.start()
    streamingContext.awaitTermination()
  }
  /**
   *  需求一：实时维护黑名单
   */

  def generateBlackList(filterBlackListRDD: DStream[String]) = {
    //String: timestamp   province    city   userid    adid
    val LogAndOneDStream: DStream[(String, Long)] = filterBlackListRDD.map {
      item =>
        val splitLog = item.split(" ")
        val timestamp: Long = splitLog(0).toLong
        val Date = DateUtils.formatDateKey(new Date(timestamp))
        val userId = splitLog(3).toLong
        val adid = splitLog(4).toLong
        val str = Date + "_" + userId + "_" + adid
        (str, 1L)
    }
    val ReducedLogData: DStream[(String, Long)] = LogAndOneDStream.reduceByKey(_+_)
    ReducedLogData.foreachRDD(
      rdd=>
        rdd.foreachPartition{
          item=>
            //Array和ArrayBuffer的区别：Array是固定长度的，ArrayBuffer类似于ArrayList是变长的
            val arrayAdUser = new ArrayBuffer[AdUserClickCount]()
            for (elem <- item) {
              val splitLogData = elem._1.split("_")
              val date = splitLogData(0)
              val userId=splitLogData(1).toLong
              val adid=splitLogData(2).toLong
              val count=elem._2.toLong
              arrayAdUser += AdUserClickCount(date, userId, adid, count)
            }
            AdUserClickCountDAO.updateBatch(arrayAdUser.toArray)
        }
    )

    val newBlackUser: DStream[(String, Long)] = ReducedLogData.filter {
      item =>
        val splitLogData = item._1.split("_")
        val date = splitLogData(0)
        val userId = splitLogData(1).toLong
        val adid = splitLogData(2).toLong
        val currentCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)
        if (currentCount >= 100) {
          true
        } else {
          false
        }
    }

    val DistinctUserID: DStream[Long] = newBlackUser.map(item=>item._1.split("_")(1).toLong).transform(rdd=>rdd.distinct())

    DistinctUserID.foreachRDD{
      RDD=>
       RDD.foreachPartition{
         item=>
         //insertBatch(adBlacklists: Array[AdBlacklist])
         val blacklists = new ArrayBuffer[AdBlacklist]()
           for (elem <- item) {
             blacklists+=AdBlacklist(elem)
           }
           AdBlacklistDAO.insertBatch(blacklists.toArray)
       }
    }



    }
  /**
    *  需求二：各省各城市一天中的广告点击量（累积统计）
    */
  def provinceCityClickStat(filterBlackListRDD: DStream[String]) = {
    //String: timestamp   province    city   userid    adid
    val LogStringValueRDD: DStream[(String, Long)] = filterBlackListRDD.map {
      item =>
        val splitLog = item.split(" ")
        val timeLong = splitLog(0).toLong
        val date = DateUtils.formatDateKey(new Date(timeLong))
        val province = splitLog(1)
        val city = splitLog(2)
        val adId = splitLog(4).toLong
        val str = date + "_" + province + "_" + city + "_" + adId
        (str,1L)
    }
    //date + "_" + province + "_" + city + "_" + adId,count
    val key2StateDStream: DStream[(String, Long)] = LogStringValueRDD.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get
        for (elem <- values) {
          newValue += elem
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray = new ArrayBuffer[AdStat]()
          // key: date province city adid
          for((key, count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong

            adStatArray += AdStat(date, province, city, adid, count)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }
    key2StateDStream

  }

  /**
    * 需求三：统计各省Top3热门广告
    */
  def proveinceTope3Adver(sparkSession: SparkSession, ClickCountAggDStream: DStream[(String, Long)]) ={
    val newKeyClickCountDStream: DStream[(String, Long)] = ClickCountAggDStream.map {
      item =>
        //timestamp   province    city   userid    adid
        val SplitValue: Array[String] = item._1.split("_")
        val timeStamp = SplitValue(0)
        val province = SplitValue(1)
        val adId = SplitValue(4).toLong
        val str = timeStamp + "_" + province + "_" + adId
        (str, item._2)
    }
    //timestamp  province  adid,count
    val reducedDStream = newKeyClickCountDStream.reduceByKey(_+_)
    val Top3RDD: DStream[Row] = reducedDStream.transform {
      rdd =>
        val LogRDD: RDD[(String, String, Long, Long)] = rdd.map {
          case (item, count) =>
            val keySplit = item.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong
            (date, province, adid, count)
        }
        import sparkSession.implicits._
        LogRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")
        val sql = "select date,province,adid,count " +
          "from (select date,provice,adid,count,row_number() over(partition by date,province order by count) rank from tmp_basic_info ) t " +
          "where rank<=3"
        sparkSession.sql(sql).rdd
    }
    Top3RDD.foreachRDD{
      RDD=>
        RDD.foreachPartition{
          rows=>
            val adProvinceTops = new ArrayBuffer[AdProvinceTop3]()
            for (row<-rows){
              val date = row.getAs[String]("date")
              val province=row.getAs[String]("province")
              val adid=row.getAs[Long]("adid")
              val count=row.getAs[Long]("count")
              adProvinceTops+= AdProvinceTop3(date,province,adid,count)
            }
            AdProvinceTop3DAO.updateBatch(adProvinceTops.toArray)
        }
    }
  }

  /**
    * 需求四：最近一个小时广告点击量统计
    */

  def getRecentHourClickCount(filterBlackListRDD: DStream[String]) = {
    //timestamp   province    city   userid    adid
    val newKeyAndLongDStream = filterBlackListRDD.map {
      item =>
        val splitLog = item.split(" ")
        val timeStampLong = splitLog(0).toLong
        //yyyyMMddHHmm
        val DateMinute = DateUtils.formatTimeMinute(new Date(timeStampLong))
        val adid = splitLog(4).toLong
        val str = DateMinute + "_" + adid
        (str, 1L)
    }
    val ReduceByWindow: DStream[(String, Long)] = newKeyAndLongDStream.reduceByKeyAndWindow((a:Long, b:Long)=>a+b,Minutes(60), Minutes(1))
    ReduceByWindow.foreachRDD{
      RDD=>
        RDD.foreachPartition{
          item=>
            //  def updateBatch(adClickTrends: Array[AdClickTrend])
            val AdClickTdBuffer = new ArrayBuffer[AdClickTrend]()
            for ((key,count)<-item){
              val splitKey = key.split("_")
              val date = splitKey(0).substring(0,8)
              val hour=splitKey(0).substring(8,10)
              val min=splitKey(0).substring(10,12)
              val adid=splitKey(1).toLong
              AdClickTdBuffer+=AdClickTrend(date,hour,min,adid,count)
            }
            AdClickTrendDAO.updateBatch(AdClickTdBuffer.toArray)
        }
    }
  }

}
