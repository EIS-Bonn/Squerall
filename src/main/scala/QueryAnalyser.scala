import org.apache.jena.query.QueryFactory

import scala.collection.mutable.{HashMap, MultiMap, Set}

/**
  * Created by mmami on 05.07.17.
  */

class QueryAnalyser(query: String) {

    def getProlog() : Map[String, String] = {
        val q = QueryFactory.create(query)
        val prolog = q.getPrologue().getPrefixMapping.getNsPrefixMap

        println("\n- Prefixes: " + prolog)

        return null
    }


    def getStars() : HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]] = {
        val q = QueryFactory.create(query)
        val originalBGP = q.getQueryPattern.toString
        val bgp = originalBGP.replaceAll("\n", "").replaceAll("\\s+", " ").replace("{"," ").replace("}"," ") // See example below + replace breaklines + remove extra white spaces
        val tps = bgp.split("\\.(?![^\\<\\[]*[\\]\\>])")

        println("\n- The BGP of the input query:  " + originalBGP)
        println("\n- Number of triple-stars detected: " + tps.length)

        val stars = new HashMap[String, Set[Tuple2[String,String]]] with MultiMap[String, Tuple2[String,String]]
        // Multi-map to add/append elements to the value

        for(i <- tps.indices) { //i <- 0 until tps.length
            val trpl = tps(i).trim

            if(!trpl.contains(';')) { // only one predicate attached to the subject
                val trplBits = trpl.split(" ")
                stars.addBinding(trplBits(0), (trplBits(1), trplBits(2)))
            } else {
                val triples = trpl.split(";")
                val firsTrpl = triples(0)
                val firsTrplBits = firsTrpl.split(" ")
                val sbj = firsTrplBits(0) // get the first triple which has s p o - rest will be only p o ;
                stars.addBinding(sbj, (firsTrplBits(1), firsTrplBits(2))) // add that first triple

                for(i <- 1 until triples.length) {
                    val t = triples(i).trim.split(" ")
                    stars.addBinding(sbj, (t(0), t(1)))
                    // addBinding` because standard methods like `+` will overwrite the complete key-value pair instead of adding the value to the existing key
                }
            }
        }

        return stars

        // Support optional later
    }
}

/* Example:
{ <file:///home/mmami/Documents/IntelliJ_Projects/SeBiFly/#InsuranceMapping>
            <http://semweb.mmlab.be/ns/rml#logicalSource>  ?s .
  ?s        <http://semweb.mmlab.be/ns/rml#source>  ?data_location
  OPTIONAL
    { ?s  ?o  ?p .
      ?g  ?o  ?d .
      ?a  ?b  "dd"
    }
}

*/