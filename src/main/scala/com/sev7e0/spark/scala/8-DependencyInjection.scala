package com.sev7e0.spark.scala

object DependencyInjection {

  /**
    * curry化最大的意义在于把多个参数的function等价转化成多个单参数function的级联，这样所有的函数就都统一了，方便做lambda演算。
    * 在scala里，curry化对类型推演也有帮助，scala的类型推演是局部的，在同一个参数列表中后面的参数不能借助前面的参数类型进行推演，
    * curry化以后，放在两个参数列表里，后面一个参数列表里的参数可以借助前面一个参数列表里的参数类型进行推演。
    * 这就是为什么 foldLeft这种函数的定义都是curry的形式
    */

  object EmailRepoImpl extends EmailRepo{
    override def getEmailRepo(user: User, unread: Boolean): Seq[Email] = {
      Nil
    }
  }
  object FilterRepoImpl extends FilterRepo{
    override def getFilterRepo(user: User): EmailFilter = _ => true
  }
  //MailBox的实现类，实例对象在继承特质时，特质中未实现的方法可以直接同名实现，若已经实现则一定要使用override关键字
  object MailBoxImpl extends MailBox{
    //将两个实现类注入进去。函数的柯里化是指在函数定义时，可以定义多个参数列表，每次调用传递的参数是一个列表中的，返回下一次调用的函数值
    override val newEmail: (User) => Seq[Email] = getEmail(EmailRepoImpl)(FilterRepoImpl) _
  }


  type EmailFilter = Email => Boolean

  case class Email(
                    subject: String,
                    text: String,
                    sender: String,
                    recipient: String
                  )

  //样例类
  case class User(name:String)

  trait EmailRepo{
    def getEmailRepo(user:User, unread:Boolean):Seq[Email]
  }

  trait FilterRepo{
    def getFilterRepo(user: User):EmailFilter
  }

  trait MailBox{
    def getEmail(emailRepo: EmailRepo)(filterRepo: FilterRepo)(user: User)={
      emailRepo.getEmailRepo(user, true).filter(filterRepo.getFilterRepo(user))
    }
    //空字段，字段的类型为一个函数
    val newEmail:User => Seq[Email]
  }


}

/**
  * 关于柯里化函数类型推演的例子：
  */

object CurryTest {
  //定义为柯里化函数时没有问题的，b的类型可以由a参与得出
  def method(a: String)(b: a.type) {

  }

  //参数列表的第二个参数，a没有定义，不能参与b的类型推断
  //  def method2(a: String, b: a.type /*a is not defined in a.B*/) {
  //
  //  }

  def main(args: Array[String]) {

  }
}