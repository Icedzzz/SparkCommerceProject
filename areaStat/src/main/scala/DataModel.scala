/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 10/28/17 12:38 PM.
 * Author: wuyufei.
 */

case class CityClickProduct(city_id: Long, click_product_id: Long)

case class CityAreaInfo(city_id: Long, city_name: String, area: String)


//***************** 输出表 *********************

case class AreaTop3Product(taskid: String,
                           area: String,
                           areaLevel: String,
                           productid: Long,
                           cityInfos: String,
                           clickCount: Long,
                           productName: String,
                           productStatus: String)