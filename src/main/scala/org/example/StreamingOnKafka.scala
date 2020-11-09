package org.example


import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Date

import com.spark.streaming.project.domain.{ClickLog, CourseClickCount}
import com.spark.streaming.project.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

/**
 *
 * @author 夏龙
 * @date 2020-08-27
 */
object StreamingOnKafka {

  def main(args: Array[String]): Unit = {
    val checkpointPath = "D:\\C盘桌面文件\\2020-08-25Scala开始\\jar"
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("StreamingOnKafka")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(checkpointPath)

    val topicName = Set[String]("test")
    val kafkaParams = Map[String,Object](
      "bootstrap.servers"->"192.168.31.140:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id"->"kafka-test-group",
      "auto.offset.reset"->"latest",
      "enable.auto.commit"->(true:java.lang.Boolean)
    )

    val Dstream=KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topicName,kafkaParams)
    )
    val logResourcesDS = Dstream.map(_.value)


    //对数据进行处理
    val wordCountData=logResourcesDS.map(line=>{
      val splits=line.split("\t")
      if(splits.length!=5){
        ClickLog("", "", 0, 0, "")
      }else{
        val ip=splits(0)
        val time = splits(1)
        val status = splits(3).toInt
        val referer = splits(4).replace("-","null")
        val url = splits(2).split(" ")(1)
        var courseId = 0
        if (url.startsWith("/class")) {
          val courseIdHtml = url.split("/")(2)
          courseId = courseIdHtml.substring(0, courseIdHtml.lastIndexOf(".")).toInt
        }
        ClickLog(ip,time,courseId,status,referer)
      }
    }).filter(x=>x.courseId!=0)

    //按搜索引擎分类统计总数
    val dataSearch=wordCountData.filter(_.referer!="null").map(mp=>{
      val http=mp.referer.replaceAll("//","/")
        .split("/")(1)
      val date=mp.time.substring(0,10)
      (http+"_"+date,1)
    }).reduceByKey(_+_)
    //实时统计总数
    val  count= dataSearch.updateStateByKey(sumCount )
    count.print()


    //存入mysql
    count.foreachRDD(rdd => rdd.foreachPartition(line => {
      Class.forName("com.mysql.jdbc.Driver")
      //获取mysql连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "145112")
      //把数据写入mysql
      try {
        for (row <- line) {
          val sql = "insert into wordcount(http_id,count)values('" + row._1 + "','" + row._2 + "') ON DUPLICATE KEY UPDATE count= " + row._2 + ""
            val code = conn.prepareStatement(sql).executeUpdate()
            //返回值
            if (code < 0) {
              println("更新失败")
            }
            else {
              println("更新成功 ")
            }
        }
      } finally {
        conn.close()
      }
    }))




    //sparkStreaming启动
    ssc.start()
    ssc.awaitTermination()




  }


  var tableName = "ns1:courses_clickcount"
  var cf = "info" // 列族
  var qualifer = "click_count" // 列

  /**
  * 保存到Hbase
  **/
  def save(list:ListBuffer[CourseClickCount]):Unit = {
    val table = HbaseUtils.getInstance().getTable(tableName)
    for(item <- list){
      // 调用hbase的一个自增加方法
      table.incrementColumnValue(Bytes.toBytes(item.day_course), Bytes.toBytes(cf), Bytes.toBytes(qualifer), item.click_count)

    }
  }
  /**
   * 统计总数
   **/
  def sumCount(currenValue: Seq[Int], runningcount: Option[Int]): Option[Int] = {
    val total = currenValue.sum + runningcount.getOrElse(0)
    Some(total)
  }

}
