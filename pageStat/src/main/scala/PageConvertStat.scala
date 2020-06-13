import java.util.UUID

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model.UserVisitAction
import com.atguigu.commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable



/**
  * @author zwt
  * @date 2020/5/28 15:03
  * @version 1.0
  */
object PageConvertStat {



  def main(args: Array[String]): Unit = {


    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)
    val taskUUID = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("PageConvertStat").setMaster("local[*]").set("spark.io.compression.codec","snappy")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val sessionIdActionRdd = getUserVisitAction(sparkSession,taskParam)
    sessionIdActionRdd.foreach(println(_))
    val sessionIdGroupAction: RDD[(String, Iterable[UserVisitAction])] = sessionIdActionRdd.groupByKey()

    val pageFlowStr: String = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    val pageFlowArr: Array[String] = pageFlowStr.split(",")
    val targetPageSplit: Array[String] = pageFlowArr.slice(0, pageFlowArr.length - 1).zip(pageFlowArr.tail).map {
      case (item1, item2) => item1 + "_" + item2
    }

    val pageSplitNumRDD: RDD[(String, Long)] = sessionIdGroupAction.flatMap {
      case (session, iterableAction) =>
        val sortList: List[UserVisitAction] = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })
        val pageList: List[Long] = sortList.map {
          case action => action.page_id
        }
        val pageSplit: List[String] = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }
        val pageSplitFilter = pageSplit.filter(
          item => targetPageSplit.contains(item)
        )
        pageSplitFilter.map(item => (item, 1L))
    }
    val pageSplitCountMap: collection.Map[String, Long] = pageSplitNumRDD.countByKey()

    val startPage = pageFlowArr(0).toLong

    val startPageCount = sessionIdActionRdd.filter {
      case (sessionIdTableSchema, action) => action.page_id == startPage
    }.count()

    getPageConvert(sparkSession,taskUUID,targetPageSplit,startPageCount,pageSplitCountMap)



  }
  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long]) = {
    val pageSplitRatio = new mutable.HashMap[String, Double]()
    var lastPageCount=startPageCount.toDouble
    for (elem <- targetPageSplit) {
      val currentPageSplitCount = pageSplitCountMap.get(elem).get.toDouble
      val ratio = currentPageSplitCount/lastPageCount
      pageSplitRatio.put(elem,ratio)
      lastPageCount=currentPageSplitCount
    }
    val convertStr: String = pageSplitRatio.map {
      case (pageSplt, ratio) => pageSplt + "=" + ratio
    }.mkString("|")
    val pageSplitConvertRate = PageSplitConvertRate(taskUUID,convertStr)
    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplitConvertRate))
    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()


  }

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val sql="select * from user_visit_action where date>='"+startDate+"'and date<='"+endDate+"'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item=>(item.session_id,item))
  }
}
