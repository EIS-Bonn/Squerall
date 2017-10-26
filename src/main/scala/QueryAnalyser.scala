import java.util

import org.apache.jena.query.QueryFactory

import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap, Set}

/**
  * Created by mmami on 05.07.17.
  */

class QueryAnalyser(query: String) {

    def getProfixes() : Map[String, String] = {
        val q = QueryFactory.create(query)
        val prolog = q.getPrologue().getPrefixMapping.getNsPrefixMap

        var prefix = Helpers.invertMap(prolog)

        println("\n- Prefixesss: " + prefix)

        return prefix
    }

    def getProject() : util.List[String] = {
        val q = QueryFactory.create(query)
        val project = q.getResultVars

        println("\n- Projected vars: " + project)

        return project
    }


    def getStars() : (mutable.HashMap[String, mutable.Set[(String, String)]] with mutable.MultiMap[String, (String, String)], mutable.HashMap[String, String]) = {
        val q = QueryFactory.create(query)
        val originalBGP = q.getQueryPattern.toString
        val bgp = originalBGP.replaceAll("\n", "").replaceAll("\\s+", " ").replace("{"," ").replace("}"," ") // See example below + replace breaklines + remove extra white spaces
        val tps = bgp.split("\\.(?![^\\<\\[]*[\\]\\>])")

        println("Projected vars are: " + q.getProjectVars)
        println("Projected vars are: " + q.getResultVars)

        println("\n- The BGP of the input query:  " + originalBGP)
        println("\n- Number of triple-stars detected: " + tps.length)

        val stars = new HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]
        // Multi-map to add/append elements to the value

        // Save [star]_[pred]
        var star_pred_var : HashMap[String, String] = HashMap()

        for(i <- tps.indices) { //i <- 0 until tps.length
            val trpl = tps(i).trim

            if(!trpl.contains(';')) { // only one predicate attached to the subject
                val trplBits = trpl.split(" ")
                stars.addBinding(trplBits(0), (trplBits(1), trplBits(2)))
                // addBinding` because standard methods like `+` will overwrite the complete key-value pair instead of adding the value to the existing key

                star_pred_var.put(trplBits(0) + "_" + trplBits(1), trplBits(2))
            } else {
                val triples = trpl.split(";")
                val firsTrpl = triples(0)
                val firsTrplBits = firsTrpl.split(" ")
                val sbj = firsTrplBits(0) // get the first triple which has s p o - rest will be only p o ;
                stars.addBinding(sbj, (firsTrplBits(1), firsTrplBits(2))) // add that first triple

                for(i <- 1 until triples.length) {
                    val t = triples(i).trim.split(" ")
                    stars.addBinding(sbj, (t(0), t(1)))

                    star_pred_var.put(sbj + "_" + t(0), t(1))
                }
            }
        }

        return (stars, star_pred_var)

        // Support optional later
    }
}