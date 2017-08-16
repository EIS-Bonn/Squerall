/**
  * Created by mmami on 30.01.17.
  */
//import com.typesafe.scalalogging._

import org.apache.jena.query.{QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.util.FileManager

//import org.json4s._
//import org.json4s.native.JsonMethods._

import play.api.libs.functional.syntax._
import play.api.libs.json._

import scala.collection.mutable.{HashMap, MultiMap, Set}

class Mapper (mappingFile: String) {

    //val logger = Logger("name")

    def findDataSources(stars: HashMap[String, Set[(String, String)]] with MultiMap[String, (String, String)]) : Set[(String,Set[(HashMap[String, String], String, String)],HashMap[String, Map[String, String]])] = {

        //logger.info(queryString)
        var starSources :
            Set[(
                String, // Star core
                Set[(HashMap[String, String], String, String)], // A set of data sources relevant to the Star (pred_attr, src, srcType)
                HashMap[String, Map[String, String]] // A set of options of each relevant data source
            )] = Set()

        var count = 0
        
        for(s <-stars) {
            val subject = s._1 // core of the star
            val predicates_objects = s._2

            println("\n- Going to find DataSources related to " + subject + "...")
            val ds = findDataSource(predicates_objects) // One or more relevant data sources
            count = count + 1

            // Options of relevant sources of one star
            var optionsPerStar : HashMap[String, Map[String,String]] = new HashMap()

            // Iterate through the relevant data sources to get options
            // One star can have many relevant sources (containing its predicates)
            for(d <- ds) {
                //val pre_attr = d._1
                val src = d._2
                //val srcType = d._3

                var configFile = Config.get("datasets.descr")
                val queryString = scala.io.Source.fromFile(configFile)
                val configJSON = try queryString.mkString finally queryString.close()

                case class ConfigObject(source: String, options: Map[String,String])

                implicit val userReads = (
                    (__ \ 'source).read[String] and
                    (__ \ 'options).read[Map[String,String]]
                )(ConfigObject)

                val sources = (Json.parse(configJSON) \ "sources").as[Seq[ConfigObject]]

                for (s <- sources) {
                    if (s.source == src) {
                        val source = s.source
                        val options = s.options

                        optionsPerStar.put(source, options)
                    }
                }
            }

            starSources.add(subject,ds,optionsPerStar)
        }

        // return: subject (star core), list of (data source, options)
        return starSources
    }

    private def findDataSource(predicates_objects: Set[(String, String)]) : Set[(HashMap[String, String], String, String)] = {
                            // Set((<http://xmlns.com/foaf/spec/firstName>,?fn), (<http://xmlns.com/foaf/spec/firstName>,?ln))
        var listOfPredicatesForQuery = ""
        val listOfPredicates : Set[String] = Set()
        val returnedSources : Set[(HashMap[String, String], String, String)] = Set()

        var temp = 0

        println("...with the (Predicate,Object) pairs: " + predicates_objects)

        for(v <- predicates_objects) {
            val predicate = v._1

            if(predicate == "rdf:type" || predicate == "a") {
                listOfPredicatesForQuery += "?mp rr:subjectMap ?sm . ?sm rr:class " + v._2 + " . "

            } else {
                listOfPredicatesForQuery += "?mp rr:predicateObjectMap ?pom" + temp + " . " +
                    "?pom" + temp + " rr:predicate " + predicate + " . " +
                    "?pom" + temp + " rr:objectMap ?om" + temp + " . " /* ?om" + temp + " rml:reference ?r . "*/

                listOfPredicates.add(predicate)
                temp +=1

            }
            //if (temp == 0) else  listOfPredicatesForQuery += "?pom rr:predicate " + predicate + " . "
        }

        //println("- Predicates for the query " + listOfPredicatesForQuery)
        var queryString = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>" +
                            "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
                            "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
                            "PREFIX sa: <http://sparkall.com/ns/spec/>" +
            "SELECT distinct ?src ?type WHERE {" +
                "?mp rml:logicalSource ?ls . " +
                "?ls rml:source ?src . " +
                "?ls sa:store ?type . " +
                //"?mp rr:predicateObjectMap ?pom . " +
                //"?pom rr:objectMap ?om . " +
                //"?om rml:reference ?r . " +
                listOfPredicatesForQuery +
            "}"

        println("...for this, the following query will be executed: " + queryString)
        var query = QueryFactory.create(queryString)

        var in = FileManager.get().open(mappingFile)
        if (in == null) {
            throw new IllegalArgumentException("File: " + queryString + " not found")
        }

        var model = ModelFactory.createDefaultModel()
        model.read(in, null, "TURTLE")

        // Execute the query and obtain results
        var qe = QueryExecutionFactory.create(query, model)
        var results = qe.execSelect()

        while(results.hasNext) { // only one result expected (for the moment)
            val soln = results.nextSolution()
            val src = soln.get("src").toString
            val srcType = soln.get("type").toString

            println(">>> Relevant source detected [" + src + "] of type [" + srcType + "]") //NOTE: considering first only one src

            val pred_attr: HashMap[String, String] = HashMap()

            for (p <- listOfPredicates) {
                //println("pr: " + p)

                val getAttributeOfPredicate = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
                    "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
                    "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
                    "SELECT ?r WHERE {" +
                    "?mp rml:logicalSource ?ls . " +
                    "?ls rml:source \"" + src + "\" . " +
                    "?mp rr:predicateObjectMap ?pom . " +
                    "?pom rr:predicate  " + p + " . " +
                    "?pom rr:objectMap ?om . " +
                    "?om rml:reference ?r . " +
                    //"?mp rr:subjectMap ?sm . " +
                    "}"

                //println("GOING TO EXECUTE: " + getAttributeOfPredicate)

                var query1 = QueryFactory.create(getAttributeOfPredicate)
                var qe1 = QueryExecutionFactory.create(query1, model)
                var results1 = qe1.execSelect()
                while (results1.hasNext) {
                    var soln1 = results1.nextSolution()
                    var attr = soln1.get("r").toString
                    //println("- Predicate " + p + " corresponds to attribute " + attr + " in " + src)
                    pred_attr.put(p,attr)

                }
            }
            returnedSources.add((pred_attr, src, srcType))
        }

        qe.close() // Important: free up resources used running the query


        // Output query results
        //ResultSetFormatter.out(System.out, results, query);

        /*return x*/
        return returnedSources
    }

}

