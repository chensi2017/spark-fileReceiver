import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object HTLogAnalysisContext {
  /*def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test")
      .set("spark.executor.instances","5")
      .set("spark.broadcast.compress","true")
      .set("spark.io.compression.codec","snappy")
    //    val spark = SparkSession
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Duration(3000))
//    ssc.socketStream()
    val line = ssc.textFileStream("D:/textFile")
    //转换流
    val itemStream = line.mapPartitions(iter=>{
      val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
      var arr=Array[Item]()
      iter.map(s=>{
        mapper.readValue(s.substring(s.indexOf("{"), s.length), classOf[Item])
      })
    })
    //统计1.各个接口累计访问次数; 2.每个接口响应总时长
    itemStream.foreachRDD(rdd=>rdd.foreachPartition(it=>{
      val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
      //HashMap[reqUrl,Array[访问次数,访问时长]]
      var resultMap=new mutable.HashMap[String,Array[Long]]()
      it.foreach(item=>{
            val arr = resultMap.getOrElse(item.reqUrl, Array[Long](0, 0))
            arr(0) = arr(0) + 1
            arr(1) = arr(1) + item.useTime
            resultMap.put(item.reqUrl, arr)
        })
      resultMap.foreach(a=>{
        println(s"api:${a._1};;;count:${a._2(0)};;;times:${a._2(1)}")
      })
    }))
    //统计每个接口每分钟访问最大次数
    val windowStream = itemStream.map(x=>(x.reqUrl,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(60),Seconds(60)).print()

    ssc.start()
    ssc.awaitTermination()
  }*/
  def main(args: Array[String]): Unit = {
//    val source = Source.fromFile("D:/textFile/f.txt").getLines()
//    source.foreach(s=>{
//      val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
//      val item = mapper.readValue(s.substring(s.indexOf("{"),s.length),classOf[Item])
//    })
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test")
      .set("spark.executor.instances","5")
      .set("spark.broadcast.compress","true")
      .set("spark.io.compression.codec","snappy")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val lines = ssc.receiverStream(new FileReceiver("D:/textFile/a.txt"))
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
