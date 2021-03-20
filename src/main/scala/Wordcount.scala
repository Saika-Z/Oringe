import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @program: test1
 * @description: ${description}
 * @author: Wenqing Zhao
 * @create: 2021-03-10 22:59
 * */
object Wordcount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("E:\\hn\\scala\\测试数据\\input\\word1.txt")
    // val groupbykey: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.size)
    val reduceByKey: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    reduceByKey.foreach(f => {
      println(f)
    })
  }


}
