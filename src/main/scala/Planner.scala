import java.util

import com.google.common.collect.ArrayListMultimap

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

import collection.JavaConverters._

/**
  * Created by mmami on 06.07.17.
  */
class Planner(stars: HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]) {


    def getNeededPredicates(star_predicate_var: mutable.HashMap[(String, String), String], sources: ArrayListMultimap[String, (String, String)], select_vars: util.List[String]) : (Set[String],Set[(String,String)]) = {

        println("star_predicate_var: "+ star_predicate_var)
        val predicates : Set[String] = Set.empty
        val predicatesForSelect : Set[(String,String)] = Set.empty

        val join_left_vars = sources.keySet()
        val join_right_vars = sources.values().asScala.map(x => x._1).toSet // asScala, converts Java Collection to Scala Collection

        val join_left_right_vars = join_right_vars.union(join_left_vars.asScala)

        println("-->Left & right join operands: " + join_left_right_vars)

        for (t <- star_predicate_var) {
            val s_p = t._1
            val o = t._2
            //val star = s_p._1

            //println("select_vars (" + select_vars + ") contains " + o  + "? " + select_vars.contains(o))
            //println("join_left_vars (" + join_left_vars + ") contains " + o  + "? " + join_left_vars.contains(o))

            val occurrences = star_predicate_var groupBy ( _._2 ) mapValues ( _.size ) // To capture variables (objects) used in more than one predicate

            if(select_vars.contains(o.replace("?","")) || join_left_vars.contains(o) || join_right_vars.contains(o) || occurrences(o) > 1)
                predicates.add(s_p._2)

            if(select_vars.contains(o.replace("?","")))
                predicatesForSelect.add(s_p)
        }

        (predicates,predicatesForSelect)
    }

    def generateJoinPlan: (ArrayListMultimap[String, (String,String)], Set[String], Set[String]) = {

        var keys = stars.keySet.toSeq
        println("Stars: " + keys.toString())
        var joins : ArrayListMultimap[String, (String,String)] = ArrayListMultimap.create[String,(String,String)]()

        var joinedToFlag : Set[String] = Set()
        var joinedFromFlag : Set[String] = Set()

        for(i <- keys.indices) {
            var currentSubject = keys(i)
            //println("Star subject: " + currentSubject)
            var valueSet = stars(currentSubject)
            //println("values: " + valueSet.toString())
            for(p_o <- valueSet) {
                var o = p_o._2
                //print("o=" + o)
                if (keys.contains(o)) { // A previous star of o
                    var p = p_o._1
                    //println(currentSubject + "---(" + o + ", " + p + ")")
                    joins.put(currentSubject, (o, p))
                    joinedToFlag.add(o)
                    joinedFromFlag.add(currentSubject)
                }
            }
        }

        (joins, joinedToFlag, joinedFromFlag)
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