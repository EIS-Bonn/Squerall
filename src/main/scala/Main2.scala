import org.apache.jena.query.{QueryExecutionFactory, QueryFactory, ResultSetFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.syntax.{ElementPathBlock, ElementVisitorBase, ElementWalker}
import org.apache.jena.util.FileManager
/**
  * Created by mmami on 07.03.17.
  */

object Main2 extends App {

    override def main(args: Array[String]) = {

        var queryString =
            "PREFIX rml: <http://semweb.mmlab.be/ns/rml#> " +
            "PREFIX ql: <http://semweb.mmlab.be/ns/ql#> " +
            "SELECT ?data_location " +
            "WHERE {" +
                "<#InsuranceMapping> rml:logicalSource ?s ." +
                "?s rml:source ?data_location ." +
                "OPTIONAL {?s ?o ?p . ?g ?o ?d . ?a ?b 'dd'}" +
            "}"

        //logger.info(queryString)

        // 1 Read RML mappings file
        var inputFileName = "RMLmappings.ttl"
        var in = FileManager.get().open(inputFileName)
        if (in == null) {
            throw new IllegalArgumentException(
                "File: " + inputFileName + " not found")
        }

        // 2 Prepare RML mappings (model)
        var model = ModelFactory.createDefaultModel()
        model.read(in, null, "TURTLE")

        // 3 get BGPs of the query
        var query = QueryFactory.create(queryString)
        print(query.getQueryPattern.toString)

        var op = Algebra.compile(query);

        var bgpWalk = new bgpWalker()

        ElementWalker.walk(query.getQueryPattern(), bgpWalk);

        // 3 Execute the query and obtain results
        var qe = QueryExecutionFactory.create(query, model)

        var results = qe.execSelect()
        results = ResultSetFactory.copyResults(results);

        // 4 show results
        /*var x = ""
        while(results.hasNext) { // only one result expected
            var soln = results.nextSolution()
            var x = soln.get("data_location").asLiteral().getString
            var l = soln.getLiteral("VarL") ;   // Get a result variable - must be a literal
            println(x)
            //print(l)
        }*/

        // Output query results
        //ResultSetFormatter.out(System.out, results, query);

        // Important - free up resources used running the query
        qe.close()

    }
}

//var tp = scala.collection.mutable.Set[Triple]()

class bgpWalker extends ElementVisitorBase {
    override def visit(el: ElementPathBlock ) {
        // ...go through all the triples...
        val triples = el.patternElts();
        while (triples.hasNext()) {
            // ...and grab the subject
            //tp += triples.next().asTriple()
            println("---" + triples.next().asTriple());
        }
    }
}