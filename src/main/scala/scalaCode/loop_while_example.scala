package scalaCode

object loop_while_example {
  def main(args: Array[String]): Unit = {
    println("this is a while loop example program!")

    var a = args(0)
    var b = args(1)

    println(a + " , " + b)

    while (a > b){

      println(a + " , " + b)
    }
  }

}
