import com.google.common.collect.ArrayListMultimap

import scala.collection.mutable.{HashMap, MultiMap, Set}

/**
  * Created by mmami on 06.07.17.
  */
class Planner(stars: HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]) {

    def generateJoinPlan(): (ArrayListMultimap[String, (String,String)], Set[String]) = {

        var keys = stars.keySet.toSeq
        var joins : ArrayListMultimap[String, (String,String)] = ArrayListMultimap.create[String,(String,String)]()

        var joinedToFlag : Set[String] = Set()

        //println("----" + stars)

        for(i <- keys.indices) {
            var currentSubject = keys(i)
            //println("sub: " + currentSubject) //bbcxvf
            var valueSet = stars(currentSubject)
            //println("vals: " + valueSet.toString())
            for(p_o <- valueSet) {
                var o = p_o._2
                //print("o=" + o)
                if (keys.contains(o)) { // A previous star of o
                    var p = p_o._1
                    //println(currentSubject + "-" + o)
                    joins.put(currentSubject, (o, p))
                    joinedToFlag.add(o)
                }
            }
        }
        //println("FLAAAG: " + joinedToFlag)

        return (joins, joinedToFlag)
    }
}
 /*
 * Map(
 *  ?b -> Set(
 *      (<http://semweb.mmlab.be/ns/rml#logicalSource3>,?c),
 *      (<http://semweb.mmlab.be/ns/rml#logicalSource5>,?c),
 *      (<http://semweb.mmlab.be/ns/rml#logicalSource4>,?c),
 *      (<http://semweb.mmlab.be/ns/rml#logicalSource2>,?c)
 *  ),
 *  ?a -> Set((<http://semweb.mmlab.be/ns/rml#logicalSource1>,?b))
 * )
 */