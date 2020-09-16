package org.example



import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
 *
 * @author 夏龙
 * @date 2020-09-11
 */


object KafkaConsumerStreamingDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[1]").setAppName("kks")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setLogLevel("error")

    val topic = List("test")
    val map = Map("bootstrap.servers" -> "192.168.255.140:9092",
      "group.id" -> "george",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    ssc.checkpoint("C:\\Users\\xialong\\Desktop\\2020-08-25Scala开始\\jar")
    val ds = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, map))
    val mapDs = ds.map(_.value())

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val userDs: DStream[(String, Int)] = mapDs.transform(x => x.map(line => {
      val arr = line.split(" ")
      val date = sdf.format(new Date(arr(0).toLong))
      val userId = arr(1)
      val adId = arr(2)
      (date + "," + userId + "," + adId, 1)
    }))
    val reduceDs = userDs.reduceByKey(_+_)
    val resDs: DStream[(String, Int)] = reduceDs.updateStateByKey((currentValues: Seq[Int], preValue: Option[Int]) => {
      val now = currentValues.sum
      val pre = preValue.getOrElse(0)
      Option(now + pre)
    })
    resDs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
