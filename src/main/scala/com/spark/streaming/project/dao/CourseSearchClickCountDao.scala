package com.spark.streaming.project.dao
import com.spark.streaming.project.domain.CourseSearchClickCount
import com.spark.streaming.project.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
/**
 *
 * @author 夏龙
 * @date 2020-08-27
 */


object CourseSearchClickCountDao {
  var tableName = "ns1:courses_search_clickcount"
  var cf = "info" // 列族
  var qualifer = "click_count" // 列

  /*
  * 保存到Hbase
  * */
  def save(list:ListBuffer[CourseSearchClickCount]):Unit = {
    val table = HbaseUtils.getInstance().getTable(tableName)
    for(item <- list){
      // 调用hbase的一个自增加方法
      table.incrementColumnValue(Bytes.toBytes(item.day_search_course), Bytes.toBytes(cf), Bytes.toBytes(qualifer), item.click_count)

    }
  }

  /*
  * 根据rowkey查询值
  * */
  def count(day_course:String):Long = {
    val table = HbaseUtils.getInstance().getTable(tableName)
    var get = new Get(Bytes.toBytes(day_course))
    var value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }


  def main(args: Array[String]): Unit = {
    var list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("spark实战",8))
    list.append(CourseSearchClickCount("scala",10))


    save(list)
    println(count("spark实战") + " : " + count("scala"))
  }


}
