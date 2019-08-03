package com.sev7e0.spark.scala

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Random, Success}

object UseFuture {
  type CoffeeBeans = String
  type GroundCoffee = String
  type Milk = String
  type FrothedMilk = String
  type Espresso = String
  type Cappuccino = String

  /**
   * 对future的理解，不同于java中的future，在java中future执行的计算结果需要等异步线程执行完后才可以获取到，获取结果时是阻塞的
   * 在scala中使用了执行上下文（ExecutiveContext）的概念，这样在future执行时也可以对其进行转换，只不过每一次
   * 都是产生新的future，直到最终结果生成，不同点在与两种语言对结果的粒度控制。
   */
  def main(args: Array[String]): Unit = {

    // 语句引入了一个全局的执行上下文，确保了隐式值的存在
    import scala.concurrent.ExecutionContext.Implicits.global
    //成功或者失败都可以使用回调函数，一个偏函数 不推荐使用,推荐使用onComplete 代替
    grind("ara beans").onSuccess { case ground =>
      Thread.sleep(Random.nextInt(2000))
      println("okay, got my ground coffee")
    }

    //在 onComplete 方法上注册回调，回调的输入是一个 Try。
    grind("java beans").onComplete {
      case Success(message) => println(s"okay, got my ground coffee $message")
      case Failure(exception) => println(exception.getMessage)
    }

    val eventualBoolean: Future[Boolean] = heatWater(Water(50)) flatMap {
      water => temperatureOkay(water)
    }
    eventualBoolean.foreach(println)


    //for语句之前三个Future并行运行，for语句之内顺序执行，注意for语句内部其实是flat map的语法糖
    val eventualCoffee = grind("java beans")
    val eventualWater = heatWater(Water(20))
    val eventualMilk = frothMilk("milk")

    val coffee = for {
      ground <- eventualCoffee
      water <- eventualWater
      milk <- eventualMilk
      okay <- brew(ground, water)
    } yield combine(okay, milk)
    coffee.foreach(println)

    Thread.sleep(10000)
  }

  //使用异步线程检查水的温度
  def temperatureOkay(water: Water): Future[Boolean] = Future {
    (80 to 85) contains water.temperature
  }

  def grind(coffeeBeans: CoffeeBeans): Future[GroundCoffee] = Future {
    println("start grinding...")
    Thread.sleep(Random.nextInt(2000))
    if (coffeeBeans == "baked beans") throw GrindingException("are you joking?")
    println("finished grinding...")
    s"ground coffee of $coffeeBeans"
  }

  def heatWater(water: Water): Future[Water] = Future {
    println("heating the water now")
    Thread.sleep(Random.nextInt(2000))
    println("hot, it's hot!")
    water.copy(temperature = 85)
  }

  def frothMilk(milk: Milk): Future[FrothedMilk] = Future {
    println("milk frothing system engaged!")
    Thread.sleep(Random.nextInt(2000))
    println("shutting down milk frothing system")
    s"frothed $milk"
  }

  def brew(coffeeBeans: CoffeeBeans, water: Water): Future[Espresso] = Future {
    println("happy brewing :)")
    Thread.sleep(Random.nextInt(2000))
    println("it's brewed!")
    "espresso"
  }

  def combine(espresso: Espresso, frothedMilk: FrothedMilk): Cappuccino = "cappuccino"

  case class Water(temperature: Int)

  case class GrindingException(msg: String) extends Exception(msg)

  case class FrothingException(msg: String) extends Exception(msg)

  case class WaterBoilingException(msg: String) extends Exception(msg)

  case class BrewingException(msg: String) extends Exception(msg)
}
