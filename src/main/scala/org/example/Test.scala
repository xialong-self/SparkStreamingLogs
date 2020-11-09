package org.example

import com.spark.streaming.project.domain.ClickLog
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author 夏龙
 * @date 2020-09-14
 */
object Test {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Test").setMaster("local")

    val sparkContext=new SparkContext(conf)
    val data=sparkContext.textFile("D:\\C盘桌面文件\\2020-08-25Scala开始\\Log.txt")


    val data_yx=data.map(line=>{
      val sp=line.split("\t",-1)
      if(sp.length!=5){
        ClickLog("", "", 0, 0, "")
      }else {
        val id=sp(0)
        val time=sp(1)
        val url=sp(2).split(" ")(1)
        val status=sp(3).toInt
        val referer=sp(4).replace("-","null")
        var courseId = 0
        if(url.startsWith("/class")){
              val courseIdHtml=url.split("/")(2)
              courseId=courseIdHtml.substring(0,courseIdHtml.lastIndexOf(".")).toInt
        }
        ClickLog(id,time,courseId,status,referer)
      }
    })
    val data_x=data_yx.filter(_.courseId!=0)
    data_x.collect().foreach(println)
//
//    val dataSearch=data_x.filter(_.referer!="null").map(mp=>{
//      val http=mp.referer.replaceAll("//","/")
//        .split("/")(1)
//      val date=mp.time.substring(0,10)
//      (http+"_"+date,1)
//    }).reduceByKey(_+_)
//
//    dataSearch.collect().foreach(println)



  }




}
