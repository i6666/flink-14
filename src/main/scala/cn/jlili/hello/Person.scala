package cn.jlili.hello

class Person {

  var name = "zhangsan"
  var nickname: String = _
  var age = 10
  val num = 30

  private var hobby: String = "chi"
  private[this] val cardInfo = "123343"

  def hello(msg:String):Unit = {
    println(s"$msg,$cardInfo")
  }



}
