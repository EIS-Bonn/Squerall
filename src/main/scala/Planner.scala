package org.sparkall

import java.util

import com.google.common.collect.ArrayListMultimap
import org.sparkall.Helpers._
import play.api.libs.functional.syntax._
import play.api.libs.json.{Json, Reads, __}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, MultiMap, Set}

/**
  * Created by mmami on 06.07.17.
  */
class Planner(stars: HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]) {

    def getNeededPredicates(star_predicate_var: mutable.HashMap[(String, String), String], joins: ArrayListMultimap[String, (String, String)], select_vars: util.List[String]) : (Set[String],Set[(String,String)]) = {

        println("star_predicate_var: "+ star_predicate_var)
        val predicates : Set[String] = Set.empty
        val predicatesForSelect : Set[(String,String)] = Set.empty

        val join_left_vars = joins.keySet()
        val join_right_vars = joins.values().asScala.map(x => x._1).toSet // asScala, converts Java Collection to Scala Collection

        val join_left_right_vars = join_right_vars.union(join_left_vars.asScala)

        println("--> Left & right join operands: " + join_left_right_vars)

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

    def generateJoinPlan: (ArrayListMultimap[String, (String,String)], Set[String], Set[String], Map[(String, String), String]) = {

        var keys = stars.keySet.toSeq
        println("Stars: " + keys.toString())
        var joins : ArrayListMultimap[String, (String,String)] = ArrayListMultimap.create[String,(String,String)]()
        var joinPairs : Map[(String,String), String] = Map.empty

        val joinedToFlag : Set[String] = Set()
        val joinedFromFlag : Set[String] = Set()

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
                    joinPairs += (omitQuestionMark(currentSubject), omitQuestionMark(o)) -> p
                    joinedToFlag.add(o)
                    joinedFromFlag.add(currentSubject)
                }
            }
        }

        (joins, joinedToFlag, joinedFromFlag, joinPairs)
    }

    def reorder(joins: ArrayListMultimap[String, (String, String)], starDataTypesMap: Map[String, mutable.Set[String]], starNbrFilters: Map[String, Integer], starWeights: Map[String, Double], configFile: String) = {

        //var configFile = Config.get("datasets.weights")

        println("************REORDERING JOINS**************")

        var joinsToReorder : ListBuffer[(String, String)] = ListBuffer()

        for (j <- joins.entries) {
            joinsToReorder += ((j.getKey, j.getValue._1))
        }

        val scoredJoins = getScoredJoins(joins, starWeights)

        val sortedScoredJoins  = ListMap(scoredJoins.toSeq.sortWith(_._2 > _._2):_*)

        sortedScoredJoins
    }

    def getScoredJoins(joins : ArrayListMultimap[String, (String, String)], scores: Map[String, Double]) = {
        var scoredJoins : Map[(String, String), Double] = Map()

        for (j <- joins.entries)
            scoredJoins += (j.getKey, j.getValue._1) -> (scores(j.getKey) + scores(j.getValue._1))

        scoredJoins
    }

    def sortStarsByWeight(starDataTypesMap: Map[String, mutable.Set[String]], filters: Map[String, Integer], configFile: String) = {
        //var configFile = Config.get("datasets.weights")

        //println("************REORDERING STARS**************")

        val queryString = scala.io.Source.fromFile(configFile)
        val configJSON = try queryString.mkString finally queryString.close()

        case class ConfigObject(datasource: String, weight: Double)

        implicit val userReads: Reads[ConfigObject] = (
            (__ \ 'datasource).read[String] and
                (__ \ 'weight).read[Double]
            )(ConfigObject)

        val weights = (Json.parse(configJSON) \ "weights").as[Seq[ConfigObject]]

        var scoresByDatasource : Map[String, Double] = Map()
        for (w <- weights) {
            scoresByDatasource += w.datasource -> w.weight
        }

        println(s"- We use the following scores of the datasource types: $scoresByDatasource \n")

        val scores = starScores(starDataTypesMap, scoresByDatasource, filters)

        scores
    }

    def starScores(starDataTypesMap: Map[String, mutable.Set[String]], weightsByDatasource: Map[String, Double], filters: Map[String, Integer]) = {
        var scores : Map[String, Double] = Map()

        var datasourceTypeWeight = 0.0 // Coucou!

        for (s <- starDataTypesMap) {
            val star = s._1 // eg. ?r
            val datasourceTypeURI_s = s._2 // eg. http://purl.org/db/nosql#cassandra

            val nbrFilters = filters(star).toInt

            if (datasourceTypeURI_s.size == 1) { // only one relevant datasource
                val datasourceType = datasourceTypeURI_s.head.split("#")(1) // eg. cassandra

                datasourceTypeWeight = weightsByDatasource(datasourceType) + nbrFilters
                // Add up the number of filters to the score of the star
            }
            // else, we keep 0, as we are assuming if there are more than 1 data sources, queryig & union-ing them would be expensive
            scores += (star -> datasourceTypeWeight)
        }

        scores
    }
}