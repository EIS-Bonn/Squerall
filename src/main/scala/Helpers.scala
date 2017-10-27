import java.util

import org.apache.jena.query.{QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.util.FileManager

import scala.collection.mutable

/**
  * Created by mmami on 26.07.17.
  */
class Helpers() {

}

object Helpers {
    def invertMap(prolog: util.Map[String, String]) = {
        var star_df : Map[String, String] = Map.empty

        var keys = prolog.keySet()
        var it = keys.iterator()
        while(it.hasNext) {
            var key : String = it.next()
            star_df += (prolog.get(key) -> key)
        }

        star_df
    }


    def omitQuestionMark(str: String) = str.replace("?","")


    def omitNamespace(URI: String): String = {
        val URIBits = URI.replace("<","").replace(">","").replace("#","/").split("/")
        return URIBits(URIBits.length-1)
    }

    def getNamespaceFromURI(URI: String): String = {
        return "" // TODO: to create
    }

    def getNS_pred(predicateURI: String): String = {

        val url = predicateURI.replace("<","").replace(">","")
        val URIBits = url.split("/")

        var pred = ""
        var ns = ""
        if(predicateURI.contains("#")) {
            pred = URIBits(URIBits.length-1).split("#")(1) // like: http://www.w3.org/2000/01/[rdf-schema#label]
        } else {
            pred = URIBits(URIBits.length-1)
        }

        ns = url.replace(pred, "")

        println("NAMSESPACE: " + ns)

        ns + "__:__" + pred
    }

    def getTypeFromURI(typeURI: String) : String = {
        var dataType = typeURI.split("/")

        var rtrn = dataType(dataType.length-1)

        rtrn
    }

    def getSelectColumnsFromSet(pred_attr: mutable.HashMap[String,String], star: String, prefixes: Map[String, String], select: util.List[String]): String = {
        var columns = ""
        var i = 0
        for(v <- pred_attr) {
            val attr = v._2
            val ns_pred = Helpers.getNS_pred(v._1)

            val short_ns_pred_bits = ns_pred.split("__:__")
            val shortNS = short_ns_pred_bits(0)
            val pred = short_ns_pred_bits(1)

            println(ns_pred + ":" + pred)

            val c = attr + " AS " + star + "_" + pred + "_" + prefixes(shortNS)
            println("SELECT CLAUSE: " + c)
            //columns = if(i == 0) columns + v else columns + "," + columns
            if(i == 0) columns += c else columns += "," + c
            i += 1
        }

        columns
    }

    def getID(sourcePath: String): String = {
        var mappingsFile = Config.get("mappings.file")

        var getID = "PREFIX rml: <http://semweb.mmlab.be/ns/rml#>" +
            "PREFIX rr: <http://www.w3.org/ns/r2rml#>" +
            "PREFIX foaf: <http://xmlns.com/foaf/spec/>" +
            "SELECT ?t WHERE {" +
                "?mp rml:logicalSource ?ls . " +
                "?ls rml:source \"" + sourcePath + "\" . " +
                "?mp rr:subjectMap ?sm . " +
                "?sm rr:template ?t " +
            "}"

        //println("GOING TO EXECUTE: " + getID)

        var in = FileManager.get().open(mappingsFile)

        var model = ModelFactory.createDefaultModel()
        model.read(in, null, "TURTLE")

        var id = ""

        var query1 = QueryFactory.create(getID)
        var qe1 = QueryExecutionFactory.create(query1, model)
        var results1 = qe1.execSelect()
        while (results1.hasNext) {
            var soln1 = results1.nextSolution()
            var template = soln1.get("t").toString

            var templateBits = template.split("/")
            id = templateBits(templateBits.length-1).replace("{","").replace("}","")
            //println("IIIIIIIIIIIIIIIIIIIIIDDDDDDDDDDDDDDDD: " + id)
        }

        id
    }

    def makeMongoURI(uri:String, database:String, collection:String) =
        s"mongodb://${uri}/${database}.${collection}"

}
