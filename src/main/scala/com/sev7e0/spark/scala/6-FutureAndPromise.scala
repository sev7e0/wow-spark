package com.sev7e0.spark.scala

import concurrent.{Await, Future, Promise}
import concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


object FutureAndPromise {

  case class TaxCut(money: Int)

  case class exception(msg:String) extends Exception(msg)

  def main(args: Array[String]): Unit = {
    val promisedCut = Promise[TaxCut]

    val promise: Promise[TaxCut] = Promise()

    //在同一个promise上调用future总是返回同一个对象，这里promise和future是一对一的关系
    val future: Future[TaxCut] = promisedCut.future

    //要结束一个promise我们可以调用success方法，此时Promise属性不可在变
    val cut = promisedCut.success(TaxCut(20))

    func1()

    /**
      * 在scala中创建线程池
      */
    import java.util.concurrent.Executors
    import concurrent.ExecutionContext
    val service = Executors.newFixedThreadPool(4)
    val ec = ExecutionContext.fromExecutorService(service)

    /**
      * 其他创建Future的方式
      **/
    val eventualUnit = Future.successful()
    val eventualFuture = Future.failed(exception("error"))
    val eventualInt = Future.fromTry(Success({1+1}))

    import concurrent.duration._
    //设置阻塞方式运行，此处表示阻塞十五秒
    Await.result(future,15.seconds)

    Thread.sleep(5000)
  }

  def func1(): Unit ={
    val eventualCut = reduceTaxFailure()
    println("Now that they're elected, let's see if they remember their promises...")
    eventualCut.onComplete{
      case Success(TaxCut(reduce)) => println(s"A miracle! They really cut our taxes by $reduce percentage points!")
      case Failure(exception) => println(s"They broke their promises! Again! Because of a ${exception.getMessage}")
    }
  }

  /**
    * 一旦用 failure 结束这个 Promise，也无法再次写入了，正如 success 方法一样。 相关联的 Future 也会以 Failure 收场
    * @return
    */
  def reduceTaxFailure():Future[TaxCut]={
    val p = Promise[TaxCut]
    Future{
      println("Starting the new legislative period.")
      Thread.sleep(2000)
      p.failure(exception("error"))
      println("We didn't fulfill our promises, but surely they'll understand.")
    }
    p.future
  }

  /**
    * 这个例子中使用了 Future 伴生对象，不过不要被它搞混淆了，这个例子的重点是：Promise 并不是在调用者的线程里完成的。
    * @return
    */
  def reduceTax():Future[TaxCut]={
    val p = Promise[TaxCut]
    Future{
      println("Starting the new legislative period.")
      Thread.sleep(2000)
      p.success(TaxCut(20))
      println("reduce is okay")
    }
    p.future
  }

}
