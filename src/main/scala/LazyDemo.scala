/**
 * @program: test1
 * @description: ${description}
 * @author: Wenqing Zhao
 * @create: 2021-03-10 22:58
 * */
object LazyDemo {

  def init(): Unit = {
    print("init")
  }
  def main(args: Array[String]): Unit = {
    val p = init() //没有lazy关键字
    lazy val q = init()
    println("init after")
    println(p)
    println(p)
    println("----------")
    println(q)
    println(q)
  }


}
