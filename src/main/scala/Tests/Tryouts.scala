package Tests

import com.google.common.collect.ArrayListMultimap
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

object Tryouts extends App {

    val jMap : ArrayListMultimap[String, (String,String)] = ArrayListMultimap.create[String,(String,String)]()

    jMap.put("b",("a","ba"))
    jMap.put("b",("l","bl"))
    jMap.put("a",("in","ain"))
    jMap.put("a",("im","aim"))
    jMap.put("im",("d","imd"))
    jMap.put("im",("f","imf"))


    for (key <- jMap.keySet) {
        val value = jMap.get(key)
        println(key + ": " + value)
    }

    println("***")

    val it = jMap.entries.iterator
    var firstTime = true

    val seenDF : ListBuffer[String] = ListBuffer()

    val join = " x "
    var joinSeries = ""

    while ({it.hasNext}) {
        val entry = it.next

        val op1 = entry.getKey
        val op2 = entry.getValue._1
        val jVal = entry.getValue._2

        println("-> (" + op1 + join + op2 + ")")

        it.remove

        if (firstTime) {
            joinSeries += op1 + join + op2

            seenDF.add(op1)
            seenDF.add(op2)
            // var joinDF = df(op1).join(op2, "jVal=ID")

            val pairsHavingAsValue = jMap.entries().filter(entry => entry.getValue()._1 == op1)
            for (i <- pairsHavingAsValue) {
                println(i.getKey + " join " + op1)
                //jMap.remove(i.getKey,i.getValue)

                seenDF.add(i.getKey)
            }

            val pairsHavingAsKey = jMap.entries().filter(entry => entry.getKey == op2)
            for (j <- pairsHavingAsKey) {
                println(op2 + " join " + j.getValue._1)
                //jMap.remove(j.getKey,j.getValue)

                seenDF.add(j.getValue._1)
            }


            firstTime = false

        } else {
            if(seenDF.contains(op1) && !seenDF.contains(op2)) {
                seenDF.add(op2)
            } else if(seenDF.contains(op2) && !seenDF.contains(op1)) {
                seenDF.add(op1)
            }
        }
    }

    println("--Join series: " + seenDF)

    val str = seenDF mkString(join)
    println("--Join series string: " + str)

}
