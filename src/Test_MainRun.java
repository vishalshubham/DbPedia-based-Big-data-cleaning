import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.jena.atlas.lib.StrUtils;


//import lt.utils.LTProperties;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.ResultSet;

public class Test_MainRun {
	
	public static DBP dbp = new DBP();
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

	public static void main(String[] args) throws FileNotFoundException {
		
		String filePath = "E:/capital.txt";
		
		File fileName = new File(filePath);
		
		Scanner scan = new Scanner(fileName);
		
		int count =0;
		int count2 =0;
		int num = 0;
		
		String country;
		
		while(scan.hasNextLine()){
			country = scan.nextLine();
			++num;
			try{
				String capital = getCapital2(country);
				System.out.print(num + " : " + country + " - " + capital);
			}
			catch(Exception e){
				System.out.print(num + " : " + country);
//				try{
//					getCapital2(country);
//				}
//				catch(Exception e2){
//					System.out.print("----------------------------Function getCapital2" + e);
//					count2++;
//					//e2.printStackTrace();
//				}
				count++;
				//e.printStackTrace();
			}
			System.out.println();
			return;
		}
		//System.out.println("Total incorrect results: " + count);
//		System.out.println("Total incorrect results2:" + count2);
	}
	
	public static String getCapital2(String name){
		
		
		
		name = name.replace(" ", "_");
		
		    ParameterizedSparqlString qs = new ParameterizedSparqlString(                    
				prefixes +
				"SELECT DISTINCT ?country ?city " + 
		  		  "WHERE { ?city rdf:type dbo:City ;" + 
		  		  "rdfs:label ?label ;" + 
		  		  "dbo:country ?country ;" +
		  		  "FILTER ( regex(?country, '/resource/Canada'))" +
		  		  "}");
		
		ResultSet rs = dbp.query(qs);
		String capital=null;
		while (rs.hasNext()) {
			
			String str = rs.next().getResource("city").toString();
			capital = getCapitalString(str);
			capital = capital.replace("_", " ");
			System.out.print(":" + name.replace("_", " ") + " - " + capital.replace("_", " "));
			
	    }
		return capital;
	}
	
	public static String getCapital(String name){
		
		//DBP dbp = new DBP();
		
		name = name.replace(" ", "_");
		
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
				"select DISTINCT ?capital where" +
				  "{" +
				  " <http://dbpedia.org/resource/United_States> prop:capital ?capital ." +
				  "}");
		
//		SELECT DISTINCT ?country ?capital WHERE { ?country rdf:type dbpedia-owl:Country ; dbpedia-owl:capital ?capital FILTER ( regex(?country, "Belgium")) }
		
		ResultSet rs = dbp.query(qs);
		String capital=null;
		while (rs.hasNext()) {
			
			String str = rs.next().getResource("capital").toString();
			capital = getCapitalString(str);
			capital = capital.replace("_", " ");
			//System.out.print(name.replace("_", " ") + " - " + capital.replace("_", " "));
			
	    }
		return capital;
	}
	
	
	public static String getCapitalString(String str){
		int lastSlash = str.lastIndexOf("/");
		return str.substring(lastSlash+1);
	}
}
