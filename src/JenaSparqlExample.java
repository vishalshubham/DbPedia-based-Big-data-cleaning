import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.atlas.lib.StrUtils;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class JenaSparqlExample {
  String sparqlEndpoint = "http://www.dbpedia.org/sparql";

  // get expression values for uniprot acc Q16850
  DBP dbp = new DBP();
  public static final String prefixes = StrUtils.strjoin("\n", new String[] {
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
			"PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>",
			"PREFIX pf:     <http://jena.hpl.hp.com/ARQ/property#>",
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>",
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>",
			"PREFIX dcterms: <http://purl.org/dc/terms/>",
			"PREFIX dbo: <http://dbpedia.org/ontology/>",
			"PREFIX prop: <http://dbpedia.org/property/>",
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
			})+ "\n";

  public JenaSparqlExample() throws IOException {
		
//	  ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
//			  "SELECT DISTINCT ?country ?capital" +
//			  "WHERE { ?country rdf:type dbo:Country ;" +
//			  "rdfs:label ?label ;" +
//			  "dbo:capital ?capital ." + 
//			  "FILTER ( regex(?country, 'Brazil'))" + 
//			"}");  
	  	ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
	  		  "SELECT DISTINCT ?country " + 
	  		  "WHERE { ?city rdf:type dbo:City ;" + 
	  		  "rdfs:label ?label ;" + 
	  		  "dbo:country ?country ;" +
	  		  "FILTER ( regex(?city, '/resource/Mumbai'))" +
	  		  "} LIMIT 1"
	  			
//	  		  "SELECT DISTINCT ?country ?capital " +
//			  "WHERE { ?country rdf:type dbo:Country ;" +
//			  "rdfs:label ?label ;" +
//			  "dbo:capital ?capital ." + 
//			  "FILTER ( regex(?country, '/resource/Canada'))" + 
//			"}"
			  );  
	  
		ResultSet rs = dbp.query(qs);
		ResultSetFormatter.out(System.out, rs);
		
//		if (rs.hasNext()) {
//	
//			String str = rs.next().getResource("city").toString();
//			String str1 = rs.next().getResource("country").toString();
//			System.out.print(str + " - " + str1);
//			
//	    }
  }

  public static void main(String[] args) throws IOException {
    new JenaSparqlExample();
  }
}

