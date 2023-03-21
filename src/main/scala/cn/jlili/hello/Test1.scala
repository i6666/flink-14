package cn.jlili.hello

import scala.language.postfixOps

object Test1 extends App {

  val person = new Person
  person.hello("nihao")

  person.age_=(100)

  println(person.age)



}
