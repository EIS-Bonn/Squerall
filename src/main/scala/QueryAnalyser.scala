import java.util

import com.google.common.collect.ArrayListMultimap
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.syntax.{ElementFilter, ElementVisitorBase, ElementWalker}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

/**
  * Created by mmami on 05.07.17.
  */

class QueryAnalyser(query: String) {

    def getPrefixes : Map[String, String] = {
        val q = QueryFactory.create(query)
        val prolog = q.getPrologue().getPrefixMapping.getNsPrefixMap

        val prefix: Map[String, String] = Helpers.invertMap(prolog)

        println("\n- Prefixes: " + prefix)

        prefix
    }

    def getProject : util.List[String] = {
        val q = QueryFactory.create(query)
        val project = q.getResultVars

        println("\n- Projected vars: " + project)
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

    def getStars : (mutable.HashMap[String, mutable.Set[(String, String)]] with mutable.MultiMap[String, (String, String)], mutable.HashMap[(String,String), String]) = {

        val q = QueryFactory.create(query)
        val originalBGP = q.getQueryPattern.toString

        val bgp = originalBGP.replaceAll("\n", "").replaceAll("\\s+", " ").replace("{"," ").replace("}"," ") // See example below + replace breaklines + remove extra white spaces
        val tps = bgp.split("\\.(?![^\\<\\[]*[\\]\\>])")

        println("\n- The BGP of the input query:  " + originalBGP)
        println("\n- Number of triple-stars detected: " + tps.length)

        val stars = new HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]
        // Multi-map to add/append elements to the value

        // Save [star]_[predicate]
        var star_pred_var : HashMap[(String,String), String] = HashMap()

        for(i <- tps.indices) { //i <- 0 until tps.length
            val triple = tps(i).trim

            println("triple: " + triple)

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
}