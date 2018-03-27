package org.sparkall

import java.util

import com.google.common.collect.ArrayListMultimap
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.syntax.{ElementFilter, ElementVisitorBase, ElementWalker}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

import Helpers._

/**
  * Created by mmami on 05.07.17.
  */

class QueryAnalyser(query: String) {


    def getPrefixes : Map[String, String] = {
        val q = QueryFactory.create(query)
        val prolog = q.getPrologue().getPrefixMapping.getNsPrefixMap

        val prefix: Map[String, String] = invertMap(prolog)

        println("\n- Prefixes: " + prefix)

        prefix
    }

    def getProject : util.List[String] = {
        val q = QueryFactory.create(query)
        val project = q.getResultVars

        println(s"\n- Projected vars: $project")
        project
    }

    def getFilters : ArrayListMultimap[String, (String, String)] = {
        val q = QueryFactory.create(query)
        val filters : ArrayListMultimap[String, (String,String)] = ArrayListMultimap.create[String,(String,String)]()

        ElementWalker.walk(q.getQueryPattern, new ElementVisitorBase() { // ...when it's a block of triples...
            override def visit(ef: ElementFilter): Unit = { // ...go through all the triples...
                val bits = ef.getExpr.toString.replace("(","").replace(")","").split(" ")
                val operation = bits(1)
                val leftOperand = bits(0)
                val rightOperand = bits(2)

                filters.put(operation,(leftOperand,rightOperand))
            }
        })

        filters
    }

    def getOrderBy = {
        val q = QueryFactory.create(query)

        val orderBy = q.getOrderBy.iterator()
        var orderBys : Set[(String,String)] = Set()
        while(orderBy.hasNext) {
            val it = orderBy.next()

            orderBys += ((it.direction.toString,it.expression.toString))
        }

        orderBys
    }

    def getStars : (mutable.HashMap[String, mutable.Set[(String, String)]] with mutable.MultiMap[String, (String, String)], mutable.HashMap[(String,String), String]) = {

        val q = QueryFactory.create(query)
        val originalBGP = q.getQueryPattern.toString

        val bgp = originalBGP.replaceAll("\n", "").replaceAll("\\s+", " ").replace("{"," ").replace("}"," ") // See example below + replace breaklines + remove extra white spaces
        val tps = bgp.split("\\.(?![^\\<\\[]*[\\]\\>])")

        val orderBy = q.getOrderBy.toArray

        println("\n- The BGP of the input query:  " + originalBGP)
        println("\n- Number of triple-stars detected: " + tps.length)

        val stars = new HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]
        // Multi-map to add/append elements to the value

        // Save [star]_[predicate]
        val star_pred_var : HashMap[(String,String), String] = HashMap()

        for (i <- tps.indices) { //i <- 0 until tps.length
            val triple = tps(i).trim

            println(s"triple: $triple")

            if(!triple.contains(';')) { // only one predicate attached to the subject
                val trplBits = triple.split(" ")
                stars.addBinding(trplBits(0), (trplBits(1), trplBits(2)))
                // addBinding` because standard methods like `+` will overwrite the complete key-value pair instead of adding the value to the existing key

                star_pred_var.put((trplBits(0), trplBits(1)), trplBits(2))
            } else {
                val triples = triple.split(";")
                val firsTriple = triples(0)
                val firsTripleBits = firsTriple.split(" ")
                val sbj = firsTripleBits(0) // get the first triple which has s p o - rest will be only p o ;
                stars.addBinding(sbj, (firsTripleBits(1), firsTripleBits(2))) // add that first triple
                star_pred_var.put((sbj, firsTripleBits(1)), firsTripleBits(2))

                for(i <- 1 until triples.length) {
                    val t = triples(i).trim.split(" ")
                    stars.addBinding(sbj, (t(0), t(1)))

                    star_pred_var.put((sbj, t(0)), t(1))
                }
            }
        }

        (stars, star_pred_var)
        // TODO: Support OPTIONAL later
    }

    def getTransformations (trans: String) = {
        // Transformations
        val transformations = trans.trim().substring(1).split("&&") // [?k?a.l.+60, ?a?l.r.toInt]
        var transmap_left : Map[String,(String, Array[String])] = Map.empty
        var transmap_right : Map[String,Array[String]] = Map.empty
        for (t <- transformations) { // E.g. ?a?l.r.toInt.scl[61]
            val tbits = t.trim.split("\\.", 2) // E.g.[?a?l, r.toInt.scl(_+61)]
            val vars = tbits(0).substring(1).split("\\?") // [a, l]
            val operation = tbits(1) // E.g. r.toInt.scl(_+60)
            val temp = operation.split("\\.", 2) // E.g. [r, toInt.scl(_+61)]
            val lORr = temp(0) // E.g. r
            val functions = temp(1).split("\\.") // E.g. [toInt, scl(_+61)]
            if (lORr == "l")
                transmap_left += (vars(0) -> (vars(1), functions))
            else
                transmap_right += (vars(1) -> functions)
        }


        (transmap_left, transmap_right)
    }

}