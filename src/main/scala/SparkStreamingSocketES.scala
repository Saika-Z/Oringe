/**
 * @Author Wenqing Zhao
 * @Date 2021/4/5 19:39
 * @Version 1.0
 */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketES {
 def main(args: Array[String]): Unit = {
   val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingSocketES")
// 设置es的节点
   conf.set("es.nodes", "s1.hadoop")
   // 设置es通信端口
   conf.set("es.port", "9200")
   // 创建StreamingContext
   // 批次间隔时间5秒，也就是说每5秒攒一批数据并处理
    val ssc = new StreamingContext(conf,Durations.seconds(5))
   // socket流的缓存级别： storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
   val inputDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6666)
   // DStream 转换操作
   // 【重要】通过隐式转换实现写入es
   import org.elasticsearch.spark._
  // "java teacher-liu"  -->  "java" -> teacher-liu
   val mapDS: DStream[Map[String, String]] = inputDS.map(line => {
     val arr: Array[String] = line.split(" ")
     val course: String = arr(0)
     val tname: String = arr(1)
     Map[String, String]("course" -> course, "tname" -> tname)
   })
     mapDS.foreachRDD((rdd, time) => {
       // 写入es
       rdd.saveToEs("course_index/course_type")
       println(s"time:${time}, data:${rdd.collect().toBuffer}")
     })
     // 启动流式计算
     ssc.start()
     // 阻塞一直运行下去，除非异常退出或关闭
     ssc.awaitTermination()

 }
}

