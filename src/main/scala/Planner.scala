import java.util

import com.google.common.collect.ArrayListMultimap

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

/**
  * Created by mmami on 06.07.17.
  */
class Planner(stars: HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]) {

    def getNeededPredicates(star_pred_var: mutable.HashMap[(String, String), String], join_vars: util.Set[String], select_vars: util.List[String]) : Set[String] = {

        var preds : Set[String] = Set.empty

        for (t <- star_pred_var) {
            val s_p = t._1
            val o = t._2

            //println("select_vars (" + select_vars + ") contains " + o  + "? " + select_vars.contains(o))
            //println("join_vars (" + join_vars + ") contains " + o  + "? " + join_vars.contains(o))

            val occurrences = star_pred_var groupBy ( _._2 ) mapValues ( _.size )

            if(select_vars.contains(o.replace("?","")) || join_vars.contains(o) || occurrences(o) > 1)
                preds.add(o)
        }

        preds
    }

    def generateJoinPlan(): (ArrayListMultimap[String, (String,String)], Set[String]) = {

        var keys = stars.keySet.toSeq
        println("Stars: " + keys)
        var joins : ArrayListMultimap[String, (String,String)] = ArrayListMultimap.create[String,(String,String)]()

        var joinedToFlag : Set[String] = Set()

        for(i <- keys.indices) {
            var currentSubject = keys(i)
            println("Star subject: " + currentSubject) //bbcxvf
            var valueSet = stars(currentSubject)
            //println("vals: " + valueSet.toString())
            for(p_o <- valueSet) {
                var o = p_o._2
                //print("o=" + o)
                if (keys.contains(o)) { // A previous star of o
                    var p = p_o._1
                    println(currentSubject + "---(" + o + ", " + p + ")")
                    joins.put(currentSubject, (o, p))
                    joinedToFlag.add(o)
                }
            }
        }

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