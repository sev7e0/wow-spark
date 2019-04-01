package com.sev7e0.spark.scala

object Functional {
  /**
    * 高阶函数，有几种表现方式
    *   一个或多个参数是函数，并返回一个值。
    *   返回一个函数，但没有参数是函数。
    *   上述两者叠加：一个或多个参数是函数，并返回一个函数。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val emailFilter: EmailFilter = sentByOneOf(Set("johndoe@example.com"))
    val mails = Email(
      subject = "It's me again, your stalker friend!",
      text = "Hello my friend! How are you?",
      sender = "johndoe@example.com",
      recipient = "me@example.com") :: Nil
    newMailsForUser(mails, emailFilter)

    val filter: EmailFilter = maximumSize1(5)
    newMailsForUser(mails, filter)
  }
  val maximumSize1: Int => EmailFilter ={
    n => size(_ >= n)
  }
  val minimumSize1: Int =>EmailFilter={
    n => size(_ <= n)
  }

  /**
    * Type是一个谓词函数，可以作为为一个参数类型，也可以放在
    */
  type SizeChecker = Int => Boolean
  val size: SizeChecker => EmailFilter={
    f=> {
      email => {
        f(email.text.length)
      }
    }
  }

  val sentByOneOf: Set[String] => Email => Boolean = {
    senders => email => senders.contains(email.sender)
  }

  val notSentByAnyOf: Set[String] => EmailFilter = {
    senders => email => !senders.contains(email.sender)
  }

  val minimumSize: Int => EmailFilter = {
    n => email => email.text.length >= n
  }

  val maximumSize: Int => EmailFilter = {
    n => email => email.text.length <= n
  }


  def newMailsForUser(mails: Seq[Email], f: EmailFilter): Seq[Email] = {
    mails.filter(f)
  }

  /**
    * 类型为 Email => Boolean 的谓词函数， 这个谓词函数决定某个邮件是否该被屏蔽：如果谓词成真，那这个邮件被接受，否则就被屏蔽掉
    */
  type EmailFilter = Email => Boolean

  case class Email(
                    subject: String,
                    text: String,
                    sender: String,
                    recipient: String
                  )
}
