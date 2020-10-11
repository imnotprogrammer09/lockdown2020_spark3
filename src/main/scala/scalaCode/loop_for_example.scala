package scalaCode

object loop_for_example {
  def main(args: Array[String]): Unit = {
    println("this is a test program for use of loops in Scala!")

    println("For loops can define ranges using 'to' and 'until' keywords")
    for(a <- 1 to 5){
      println("value of a is : " + a)
    }

    for(b <- 1 until 6; if b<4) {
      println("value of b until : " + b)
    }

    // use of for loop to return a value
    val lst = List(1,2,3,4,5)
    val slst = for{c <- lst} yield{c*c}
    println(slst)
  }

}
