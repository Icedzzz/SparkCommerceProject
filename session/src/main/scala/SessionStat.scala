import java.util.{Date, Random, UUID}

import com.atguigu.commons.conf.ConfigurationManager
import com.atguigu.commons.constant.Constants
import com.atguigu.commons.model.{UserInfo, UserVisitAction}
import com.atguigu.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @author zwt
  * @date 2020/5/17 11:32
  * @version 1.0
  */
object SessionStat {




  def main(args: Array[String]): Unit = {
    // 获取筛选条件
    val JsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val jSONObject: JSONObject = JSONObject.fromObject(JsonStr)
    // 创建全局唯一主键
    val taskUUID: String = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]").set("spark.io.compression.codec","snappy")
    val session = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 获取原始的动作表信息 -- 筛选数据
    var actionRDD: RDD[UserVisitAction] = getOrActionRDD(session, jSONObject)
    //(k,v)=>(sessionid,item)
    val mapItem: RDD[(String, UserVisitAction)] = actionRDD.map(item=>(item.session_id,item))
    //聚合
    //UserVisitAction(2020-05-16,91,7f324f9accb6409fa42f41a7add49324,9,2020-05-16 15:47:58,null,-1,-1,64,80,null,null,4)
    val groupBySessionId: RDD[(String, Iterable[UserVisitAction])] = mapItem.groupByKey()
    groupBySessionId.cache()

    val fullAggInfo: RDD[(String, String)] = getSessionFullInfo(session,groupBySessionId)
    //(56b7a09cd41f4963be19fcce1bdd22ca,sessionid=56b7a09cd41f4963be19fcce1bdd22ca|searchKeywords=火锅,重庆小面,国贸大厦,重庆辣子鸡,呷哺呷哺,温泉,日本料理,蛋糕,太古商场|clickCategoryIds=52,57,32,83,31,17,99,10,87,15,58,8,19,64,95,89|visitLength=3430|stepLength=71|startTime=2020-05-16 09:00:08|age=2|professional=professional80|sex=female|city=city48|)

    // 创建累加器对象
    val sessionAccumlator: SessionAccumlator = new SessionAccumlator

    // 注册累加器
    session.sparkContext.register(sessionAccumlator)

    //过滤数据
    val filteredAggData: RDD[(String, String)] = filterAggInfo(jSONObject,fullAggInfo,sessionAccumlator)
    filteredAggData.foreach(println(_))
    // 需求一：求session步长和统计结果 写入数据库
    getSessionRatio(session, taskUUID, sessionAccumlator.value)
    // 需求二：session随机抽取
    // sessionId2FilterRDD： RDD[(sid, fullInfo)] 一个session对应一条数据，也就是一个fullInfo
    sessionRandomExtract(session, taskUUID, filteredAggData)

    //需求三：Top10热门品类的Top10热门session
    val FilterActionRdd: RDD[(String, UserVisitAction)] = mapItem.join(filteredAggData).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }
    val top10CategoryArray: Array[(SortKey, String)] = top10PopularCategories(session,taskUUID,FilterActionRdd)

    // 需求四：Top10热门商品的Top10活跃session统计
    top10ActiveSession(session, taskUUID, FilterActionRdd, top10CategoryArray)
  }


  /**
   *需求一：用户session各范围访问步长、访问时长统计占比
   *
   */

  def getSessionRatio(session: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {

      val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

      val visitLength_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
      val visitLength_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
      val visitLength_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
      val visitLength_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
      val visitLength_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
      val visitLength_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
      val visitLength_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
      val visitLength_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
      val visitLength_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

      val stepLength_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
      val stepLength_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
      val stepLength_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
      val stepLength_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
      val stepLength_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
      val stepLength_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

      val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visitLength_1s_3s / session_count, 2)
      val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visitLength_4s_6s / session_count, 2)
      val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visitLength_7s_9s / session_count, 2)
      val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visitLength_10s_30s / session_count, 2)
      val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visitLength_30s_60s / session_count, 2)
      val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visitLength_1m_3m / session_count, 2)
      val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visitLength_3m_10m / session_count, 2)
      val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visitLength_10m_30m / session_count, 2)
      val visit_length_30m_ratio = NumberUtils.formatDouble(visitLength_30m / session_count, 2)

      val step_length_1_3_ratio = NumberUtils.formatDouble(stepLength_1_3 / session_count, 2)
      val step_length_4_6_ratio = NumberUtils.formatDouble(stepLength_4_6 / session_count, 2)
      val step_length_7_9_ratio = NumberUtils.formatDouble(stepLength_7_9 / session_count, 2)
      val step_length_10_30_ratio = NumberUtils.formatDouble(stepLength_10_30 / session_count, 2)
      val step_length_30_60_ratio = NumberUtils.formatDouble(stepLength_30_60 / session_count, 2)
      val step_length_60_ratio = NumberUtils.formatDouble(stepLength_60 / session_count, 2)

    val sessionAggrStat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio, visit_length_10s_30s_ratio,
      visit_length_30s_60s_ratio, visit_length_1m_3m_ratio, visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio, step_length_1_3_ratio,
      step_length_4_6_ratio, step_length_7_9_ratio, step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)
    import session.implicits._
    val sessionRatioRDD: RDD[SessionAggrStat] = session.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionRatioRDD.toDF().write.format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio")
      .mode(SaveMode.Append)
      .save()
  }


  def calculateStepLength(stepLength: Long, sessionAccumlator: SessionAccumlator) = {
    if (stepLength >= 1 && stepLength <= 3)
      sessionAccumlator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4 && stepLength <= 6)
      sessionAccumlator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7 && stepLength <= 9)
      sessionAccumlator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10 && stepLength <= 30)
      sessionAccumlator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength > 30 && stepLength <= 60)
      sessionAccumlator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength > 60)
      sessionAccumlator.add(Constants.STEP_PERIOD_60)
  }

  def calculateVisitLength(visitLength: Long, sessionAccumlator: SessionAccumlator) = {
    if (visitLength >= 1 && visitLength <= 3)
      sessionAccumlator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6)
      sessionAccumlator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9)
      sessionAccumlator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength <= 30)
      sessionAccumlator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength > 30 && visitLength <= 60)
      sessionAccumlator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength > 60 && visitLength <= 180)
      sessionAccumlator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength > 180 && visitLength <= 600)
      sessionAccumlator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength > 600 && visitLength <= 1800)
      sessionAccumlator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength > 1800)
      sessionAccumlator.add(Constants.TIME_PERIOD_30m)
  }
  //过滤数据
  def filterAggInfo(jSONObject: JSONObject, fullAggInfo: RDD[(String, String)],sessionAccumlator:SessionAccumlator) = {
      //限制条件
    val startAge = ParamUtils.getParam(jSONObject, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(jSONObject, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(jSONObject, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(jSONObject, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(jSONObject, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(jSONObject, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(jSONObject, Constants.PARAM_CATEGORY_IDS)
    var filterInfo=(if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else "")
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }
    val filteredRdd: RDD[(String, String)] = fullAggInfo.filter {
      case (session, fullInfo) => {
        var success = true
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionAccumlator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength, sessionAccumlator)
          calculateStepLength(stepLength, sessionAccumlator)
        }
        success
      }
    }
    filteredRdd


  }
  def getSessionFullInfo(session: SparkSession, groupBySessionId: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = groupBySessionId.map {
      //String, Iterable[UserVisitAction]
      //2020-05-16,91,7f324f9accb6409fa42f41a7add49324,9,2020-05-16 15:47:58,null,-1,-1,64,80,null,null,4
      case (sessionId, iterableAction) => {
        //session_id | search_keywords | click_category_id | visit_length | Step_length | start_time | age | profess | sex |city
        var userId = -1L
        var startTime: Date = null
        var endTime: Date = null
        var stepLength: Int = 0
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")
        for (elem <- iterableAction) {
          if (userId == -1L) userId = elem.user_id

          val actionTime = DateUtils.parseTime(elem.action_time)
          if (startTime == null || startTime.after(actionTime)) startTime = actionTime
          if (endTime == null || endTime.before(actionTime)) endTime = actionTime

          val search_keyword: String = elem.search_keyword
          if (StringUtils.isNotEmpty(search_keyword) && !searchKeywords.toString.contains(search_keyword)) searchKeywords.append(search_keyword + ",")
          val click_category_id: Long = elem.click_category_id
          if (click_category_id != -1L && !clickCategories.toString.contains(click_category_id)) clickCategories.append(click_category_id + ",")
          stepLength += 1;
        }
        val searchKeywordBuffer = StringUtils.trimComma(searchKeywords.toString)
        val clickCategoryBuffer = StringUtils.trimComma(clickCategories.toString)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        val fullInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywordBuffer + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryBuffer + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, fullInfo)
      }
    }
    import session.implicits._
    val sql="select * from user_info"
    val userInfo: RDD[(Long, UserInfo)] = session.sql(sql).as[UserInfo].rdd.map(item=>(item.user_id,item))
    val fullInfoRdd: RDD[(String, String)] =userId2AggrInfoRDD.join(userInfo).map{
      case (userId, (aggrInfo, userInfo)) =>
        val age: Int = userInfo.age
        val professional: String = userInfo.professional
        val sex: String = userInfo.sex
        val city: String = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city + "|"

        val sessionID: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionID, fullInfo)
    }
    fullInfoRdd
  }

  def getOrActionRDD(session: SparkSession, jSONObject: JSONObject): RDD[UserVisitAction] = {
    val startDate: String = ParamUtils.getParam(jSONObject, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(jSONObject, Constants.PARAM_END_DATE)
    import session.implicits._
    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"
    session.sql(sql).as[UserVisitAction].rdd
  }


  /**
    *需求二：session随机抽取
    *
    */

  def sessionRandomExtract(session: SparkSession,
                           taskUUID: String,
                           filteredAggData: RDD[(String, String)]): Unit = {
    val dateHourAndFullInfoRDD: RDD[(String, String)] = filteredAggData.map {
      case (sid, fullInfo) => {
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // dateHour: yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
      }
    }
    //.Map[dateHour, count]
    val hourCountMap: collection.Map[String, Long] = dateHourAndFullInfoRDD.countByKey()
    val DateAndHourAndCountMap=new mutable.HashMap[String,mutable.HashMap[String,Long]]()

    for ((dateHour,count) <- hourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      DateAndHourAndCountMap.get(date) match {
        case None =>DateAndHourAndCountMap(date)=new mutable.HashMap[String,Long]()
          DateAndHourAndCountMap(date)+=(hour->count)
        case Some(map)=> DateAndHourAndCountMap(date)+=(hour->count)
      }
    }
    // 解决问题一： 一共有多少天： DateAndHourAndCountMap.size
    //              一天抽取多少条：100 / DateAndHourAndCountMap.size
    val extractPerDay = 100/DateAndHourAndCountMap.size

    // 解决问题二： 一天有多少session：dateHourCountMap(date).values.sum
    // 解决问题三： 一个小时有多少session：dateHourCountMap(date)(hour)

    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    for ((date,hourCountMap) <- DateAndHourAndCountMap) {
      val dateSessionCount = hourCountMap.values.sum
      dateHourExtractIndexListMap.get(date) match {
        case None=> dateHourExtractIndexListMap(date)=new mutable.HashMap[String,ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap,  dateHourExtractIndexListMap(date))
        case Some(map)=>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap,  dateHourExtractIndexListMap(date))
      }
    }
    // 到目前为止，我们获得了每个小时要抽取的session的index
    // 广播大变量，提升任务性能
    val dateExtractIndexListMapBd = session.sparkContext.broadcast(dateHourExtractIndexListMap)
    val dateHourGroupRdd: RDD[(String, Iterable[String])] = dateHourAndFullInfoRDD.groupByKey()
    val extractSessionBuffer: RDD[SessionRandomExtract] = dateHourGroupRdd.flatMap {
      case (dateHour, iterableFullInfo) => {
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        val extractList = dateExtractIndexListMapBd.value.get(date).get(hour)
        val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()
        var index = 0
        for (fullInfo <- iterableFullInfo) {
          if (extractList.contains(index)) {
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            val sessionExtract = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
            extractSessionArrayBuffer += sessionExtract
          }
          index += 1
        }
        extractSessionArrayBuffer
      }
    }
    import session.implicits._
    extractSessionBuffer.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_extract")
      .mode(SaveMode.Append)
      .save()
  }
  def generateRandomIndexList(extractPerDay: Int,
                              dateSessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    for ((hour,count) <- hourCountMap) {
      // 获取一个小时要抽取多少条数据
      var hourExrCount = ((count/dateSessionCount.toDouble)*extractPerDay).toInt
      // 避免一个小时要抽取的数量超过这个小时的总数
      if(hourExrCount>count){
        hourExrCount=count.toInt
      }
      val random = new Random()

      hourListMap.get(hour) match {
        case None => hourListMap(hour)=new ListBuffer[Int]
          for (elem <- 0 until hourExrCount) {
            var index=random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)){
              index=random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list)=>
          for (elem <- 0 until hourExrCount) {
            var index=random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)){
              index=random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }

    }

  }

  /**
    *需求三：热门商品统计
    *
    */

  //最受欢迎的商品Top10
  def top10PopularCategories(session: SparkSession, taskUUID: String, FilterActionRdd: RDD[(String, UserVisitAction)]) = {
    // 第一步：获取所有发生过点击、下单、付款的品类
    val cidCategoryRdd: RDD[(Long, Long)] = FilterActionRdd.flatMap {
      case (sid, action) => {
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(",")) {
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
          }
        } else if (action.pay_category_ids != null) {
          for (payCid <- action.pay_category_ids.split(",")) {
            categoryBuffer += ((payCid.toLong, payCid.toLong))
          }
        }
        categoryBuffer
      }
    }

    val distinctCategoryIdRdd = cidCategoryRdd.distinct()

    //第二步：统计品类的点击次数、下单次数、付款次数
    val cidClickCountRdd = getClickCount(FilterActionRdd)
    val cidOrederCountRdd = getOrdeCount(FilterActionRdd)
    val cidPayCountRdd = getPayCount(FilterActionRdd)

    val cidFullCountRDD = getFullCount(distinctCategoryIdRdd,cidClickCountRdd,cidOrederCountRdd,cidPayCountRdd)
    // 实现自定义二次排序key
    val sortKeyFullCountRDD: RDD[(SortKey, String)] = cidFullCountRDD.map {
      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, countInfo)
    }
    val top10CategoryArray: Array[(SortKey, String)] = sortKeyFullCountRDD.sortByKey(false).take(10)
    val top10CategoryRdd = session.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }
    import session.implicits._
    top10CategoryRdd.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save
    top10CategoryArray
  }

  def getFullCount(distinctCategoryIdRdd: RDD[(Long, Long)],
                   cidClickCountRdd: RDD[(Long, Long)],
                   cidOrederCountRdd: RDD[(Long, Long)],
                   cidPayCountRdd: RDD[(Long, Long)]): RDD[(Long, String)] = {
    val cidClickInfoRdd = distinctCategoryIdRdd.leftOuterJoin(cidClickCountRdd).map {
      case (cid, (category, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid, aggrCount)
    }
    val cidOrderInfoRdd = cidClickInfoRdd.leftOuterJoin(cidOrederCountRdd).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrCount = clickInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrCount)
    }
    val cidPayInfoRdd = cidOrderInfoRdd.leftOuterJoin(cidPayCountRdd).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrCount = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid, aggrCount)
    }
    cidPayInfoRdd

  }

  //统计品次的点击次数
  def getClickCount(FilterActionRdd: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    val clickFilterRdd = FilterActionRdd.filter(item=> item._2.click_category_id!=null)
    val clickFilterMapRdd: RDD[(Long, Long)] = clickFilterRdd.map {
      case (sid, action) => (action.click_category_id, 1L)
    }
    clickFilterMapRdd.reduceByKey(_+_)
  }

  //统计下单次数
  def getOrdeCount(FilterActionRdd: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] ={
    val orderFilterRdd: RDD[(String, UserVisitAction)] = FilterActionRdd.filter(item=>item._2.order_category_ids!=null)
    val orderNumRdd: RDD[(Long, Long)] = orderFilterRdd.flatMap {
      case (sessionId,action)=>action.order_category_ids.split(",").map(item=>(item.toLong,1L))
    }
    orderNumRdd.reduceByKey(_+_)
  }

  //统计付款次数
  def getPayCount(FilterActionRdd: RDD[(String, UserVisitAction)]) ={
    val payFilterRdd = FilterActionRdd.filter(item=>item._2.pay_category_ids!=null)
    val payNumRDD = payFilterRdd.flatMap{
      case (sid, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    payNumRDD.reduceByKey(_+_)
  }

  /**
    *需求四：Top10热门商品的Top10活跃session统计
    *
    */

  def top10ActiveSession(session: SparkSession,
                         taskUUID: String,
                         FilterActionRdd: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]): Unit = {
    // 第一步：过滤出所有点击过Top10品类的action
    val cidArray: Array[Long] = top10CategoryArray.map {
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }
    // 所有符合过滤条件的，并且点击过Top10热门品类的action
    val sessionIdActionRDD: RDD[(String, UserVisitAction)] = FilterActionRdd.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }
    val sessionIdGroupRdd: RDD[(String, Iterable[UserVisitAction])] = sessionIdActionRDD.groupByKey()
    val cidSessionCountRDD: RDD[(Long, String)] = sessionIdGroupRdd.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()
        for (elem <- iterableAction) {
          val cid = elem.click_category_id
          if (!categoryCountMap.contains(cid)) {
            categoryCountMap += (cid -> 0)
          }
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)

        }
        // 记录了一个session对于它所有点击过的品类的点击次数
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)

    }
    //一个categoryid和它对应的所有点击过它的session对它的点击次数
    val cidGroupRDD: RDD[(Long, Iterable[String])] = cidSessionCountRDD.groupByKey()

    val top10SessionsRDD = cidGroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        val sortList: List[String] = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)
        val top10Sessions = sortList.map {
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }
        top10Sessions
    }

    import session.implicits._
    top10SessionsRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session")
      .mode(SaveMode.Append)
      .save()

  }

}
