package com.sev7e0.spark.scala

object PartialApplicationFunctions {

  /**
    * 调用一个函数时，不是把函数需要的所有参数都传递给它，而是仅仅传递一部分，其他参数留空； 这样会生成一个新的函数，其参数列表由那些被留空的参数组成
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val mails = Email(
      subject = "It's me again, your stalker friend!",
      text = "Hello my friend! How are you?",
      sender = "johndoe@example.com",
      recipient = "me@example.com")
    println(sizeConstraintFn(ge)(50)(mails))

    val emailToBoolean = minSize(50)(mails)



  }


  val gt: IntPairPred = _ > _
  val ge: IntPairPred = _ >= _
  val lt: IntPairPred = _ < _
  val le: IntPairPred = _ <= _
  val eq: IntPairPred = _ == _

  type IntPairPred = (Int, Int) => Boolean

  //对于没有传入值的参数没必须使用占位符_，还需要指定这些参数的类型，这也是的函数的部分应用有些复杂
  val minimumSize :(Int, Email) => Boolean = sizeConstraint1(ge, _:Int, _:Email)

  /**
    *
    * @param pairPred
    * @param n
    * @param email
    * @return 不返回新的filter，只用使用参数进行计算
    */
  def sizeConstraint1 (pairPred: IntPairPred, n: Int, email: Email): Boolean ={
    pairPred(email.text.length, n)
  }
  //在scala中可以顶多多个参数列表
  def sizeConstraint(pred: IntPairPred)(n: Int)(email: Email): Boolean =
    pred(email.text.size, n)

  /**
    * 也可以把它定义成一个可赋值 可传递的函数对象
    * sizeConstraintFn 接受一个 IntPairPred ，返回一个函数，这个函数又接受 Int 类型的参数，返回另一个函数，最终的这个函数接受一个 Email ，返回布尔值。
    *
    * 柯里化函数
    */
  val sizeConstraintFn: IntPairPred => Int => Email => Boolean = sizeConstraint _

  val min20:Email => Boolean = sizeConstraint(ge)(20)

  val minSize: Int => Email => Boolean = sizeConstraint(ge)
  val maxSize: Int => Email => Boolean = sizeConstraint(le)


  case class Email(
                    subject: String,
                    text: String,
                    sender: String,
                    recipient: String)

  //过滤邮件的条件用谓词 Email => Boolean 表示， EmailFilter 是其别名
  type EmailFilter = Email => Boolean
}
