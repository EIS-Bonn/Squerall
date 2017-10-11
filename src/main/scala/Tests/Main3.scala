package Tests

import scala.collection.mutable

/**
  * Created by mmami on 07.03.17.
  */

object Main3 extends App {

    override def main(args: Array[String]) = {

        var q = mutable.Queue[String]()

        q.enqueue("1")
        q.enqueue("3")
        q.enqueue("5")
        q.enqueue("2")
        q.enqueue("4")
        q.enqueue("6")

        println("pendingJoins! " + q)

        q.dequeue()
        q.dequeue()

        println("pendingJoins " + q)

        var i = 0
        while (q.nonEmpty) {
            val e = q.head
            println(e)

            if (i < 2){
                i += 1
                q.enqueue(e)
            }

            q = q.tail
        }

        println("pendingJoins " + q)
    }
}

