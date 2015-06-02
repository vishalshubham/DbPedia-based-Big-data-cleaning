import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.apache.jena.atlas.lib.StrUtils;

//import lt.utils.LTProperties;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

public class MainRun {
	
	public DBP dbp = new DBP();
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

	public static void main(String[] args) throws FileNotFoundException, SQLException {
		
		String country = null;
		String capital = null;
		String city = null;
		String conference = null;
		int id;
		
		DbConnect dbConnect = new DbConnect();   // To get all the uncleaned data(main table)
		DbConnect dbConnect1 = new DbConnect();  // To save the changed data in the main table
		
		
		MainRun mainRun = new MainRun();
		java.sql.ResultSet rs = dbConnect.getRawData();
		
		while(rs.next()){
			try {
				id = rs.getInt("per_id");
				country = rs.getString("per_country");
				capital = rs.getString("per_capital");
				city = rs.getString("per_city");
				conference = rs.getString("per_conference");
				
				//System.out.println(country + " - " + capital + " - " + city + " - " + conference );
				
				DbDataPack checkRS = mainRun.checkMaster(country, capital, city, conference);
				if(checkRS.value){
					country = checkRS.country;
					capital = checkRS.capital;
					city = checkRS.city;
					conference = checkRS.conference;
					System.out.println("No need to do anything. All is well");
				}
				else{
					
					DbPediaDataPack dbPediaDataPack = mainRun.checkDbPedia(country, capital, city, conference);
					System.out.println("Data cleaned. All is well");
					country = dbPediaDataPack.country;
					capital = dbPediaDataPack.capital;
					city = dbPediaDataPack.city;
					conference = dbPediaDataPack.conference;
				}
				
				dbConnect1.changeData(id, country, capital, city, conference);
			}
			catch (Exception e) {
				System.out.println("ERROR! " + e);
			}
		}
	}
	
	public DbPediaDataPack checkDbPedia(String country, String capital, String city, String conference) throws SQLException{
		
		DbConnect dbConnect = new DbConnect();
		DbPediaDataPack db = new DbPediaDataPack();
		boolean countryCapital = false;
		boolean countryCity = false;
		boolean capitalCity = false;
		
		if(checkCountry(country)){
			db.country = country;
		}
		else{
			country = "";
		}

		if(checkCapital(capital)){
			db.capital = capital;
		}
		else{
			capital = "";
		}
		
		if(checkCity(city)){
			db.city = city;
		}
		else{
			city = "";
		}
		
		
		
		if(getCountryByCapital(capital).equals(country) && !capital.equals("") && !country.equals("")){
			countryCapital = true;
			db.country = country;
			db.capital = capital;
			db.city = city;
			db.conference = conference;
			dbConnect.addMasterData(country, capital, city, conference);
			return db;
		}
		
		if(getCapitalByCity(city).equals(capital) && !capital.equals("") && !city.equals("")){
			country = getCountryByCapital(capital);
			capitalCity = true;
			db.country = country;
			db.capital = capital;
			db.city = city;
			db.conference = conference;
			dbConnect.addMasterData(country, capital, city, conference);
			return db;
		}
		
		if(getCountryByCity(city).equals(country) && !country.equals("") && !city.equals("")){
			capital = getCapitalByCountry(country);
			countryCity = true;
			db.country = country;
			db.capital = capital;
			db.city = city;
			db.conference = conference;
			dbConnect.addMasterData(country, capital, city, conference);
			return db;
		}
	
		if(country.equals("")){
			if(!capital.equals("")){
				country = getCountryByCapital(capital);
			}
			else if(!city.equals("")){
				country = getCountryByCity(city);
			}
		}
		
		if(capital.equals("")){
			if(!country.equals("")){
				capital = getCapitalByCountry(country);
			}
			else if(!city.equals("")){
				capital = getCapitalByCity(city);
			}
		}
		
		//Default added to master
			db.country = country;
			db.capital = capital;
			db.city = city;
			db.conference = conference;
			dbConnect.addMasterData(country, capital, city, conference);
			return db;
	}
	
	public DbPediaDataPack checkCountryCapital(String country, String capital){
		DbPediaDataPack dbPediaDataPack = new DbPediaDataPack();
		
		return dbPediaDataPack;
	}
	
	public DbDataPack checkMaster(String country, String capital, String city, String conference) throws SQLException{
		
		DbConnect dbConnect = new DbConnect();
		DbDataPack dbDataPack = new DbDataPack();
		dbDataPack = dbConnect.getData(country, capital, city);
		
		if(!dbDataPack.country.equals("") || !dbDataPack.capital.equals("") || !dbDataPack.city.equals("")){
			System.out.println("Clean data. Already in master database");
			dbDataPack.value = true;
			return dbDataPack;
		}
		else{
			if(dbConnect.getCountryCapitalData(country, capital) == 1){
				dbDataPack.countryCount++;
				dbDataPack.capitalCount++;
			}
			
			if(dbConnect.getCountryCityData(country, city) == 1){
				dbDataPack.countryCount++;
				dbDataPack.cityCount++;
			}
			
			if(dbConnect.getCapitalCityData(capital, city) == 1){
				dbDataPack.capitalCount++;
				dbDataPack.cityCount++;
			}
			
			if(dbDataPack.countryCount == 2 && dbDataPack.capitalCount == 2 && dbDataPack.cityCount == 2){
				System.out.println("Clean data. Already in master database");
				dbConnect.addMasterData(country, capital, city, conference);
				dbDataPack.value = true;
				dbDataPack.country = country;
				dbDataPack.capital = capital;
				dbDataPack.city = city;
				dbDataPack.conference = conference;
				return dbDataPack;
			}
			
			if(dbDataPack.countryCount == 1 && dbDataPack.capitalCount == 1 ){
				// Clean city. Not possible right now
				dbConnect.addMasterData(country, capital, city, conference);
				dbDataPack.value = true;
				dbDataPack.country = country;
				dbDataPack.capital = capital;
				dbDataPack.city = city;
				dbDataPack.conference = conference;
				return dbDataPack;
			}
			else if(dbDataPack.countryCount == 1 && dbDataPack.cityCount == 1){
				capital = getCapitalByCountry(country);
				dbConnect.addMasterData(country, capital, city, conference);
				dbDataPack.value = true;
				dbDataPack.country = country;
				dbDataPack.capital = capital;
				dbDataPack.city = city;
				dbDataPack.conference = conference;
				return dbDataPack;
			}
			else if(dbDataPack.capitalCount == 1 && dbDataPack.cityCount == 1){
				country = getCountryByCapital(capital);
				dbConnect.addMasterData(country, capital, city, conference);
				dbDataPack.value = true;
				dbDataPack.country = country;
				dbDataPack.capital = capital;
				dbDataPack.city = city;
				dbDataPack.conference = conference;
				return dbDataPack;
			}
			
			System.out.println("Not clean data OR Not in master database");
			dbDataPack.value = false;
			return dbDataPack;
		}
	}
	
	public String getCapitalByCountry(String name){

		String capital="";
		try{
			name = name.replace(" ", "_");
			
			ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
					
					"select DISTINCT ?capital where" +
					  "{" +
					  " <http://dbpedia.org/resource/" + name + "> prop:capital ?capital ." +
					  "}"
					  );
			
			ResultSet rs = dbp.query(qs);
			while (rs.hasNext()) {
				
				String str = rs.next().getResource("capital").toString();
				capital = getCapitalString(str);
				capital = capital.replace("_", " ");
		
		    }
		}
		catch(Exception ex){
			name = name.replace(" ", "_");
			
			ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
					"SELECT DISTINCT ?capital " +
					  "WHERE { ?country rdf:type dbo:Country ;" +
					  "rdfs:label ?label ;" +
					  "dbo:capital ?capital ." + 
					  "FILTER ( regex(?country, '/resource/" + name + "'))" + 
					"}"
					  );
			
			ResultSet rs = dbp.query(qs);
			
			while (rs.hasNext()) {
				
				String str = rs.next().getResource("capital").toString();
				capital = getCapitalString(str);
				capital = capital.replace("_", " ");
		
		    }
		}
		if(capital.equals(null) || capital.equals("")){
			name = name.replace(" ", "_");
			
			ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
					"SELECT DISTINCT ?capital " +
					  "WHERE { ?country rdf:type dbo:Country ;" +
					  "rdfs:label ?label ;" +
					  "dbo:capital ?capital ." + 
					  "FILTER ( regex(?country, '/resource/" + name + "'))" + 
					"}"
					  );
			
			ResultSet rs = dbp.query(qs);
			
			while (rs.hasNext()) {
				
				String str = rs.next().getResource("capital").toString();
				capital = getCapitalString(str);
				capital = capital.replace("_", " ");
		
		    }
		}
		return capital;
	}
	
	public String getCountryByCapital(String name){
		
		name = name.replace(" ", "_");
				
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
				"SELECT DISTINCT ?country " +
				  "WHERE { ?country rdf:type dbo:Country ;" +
				  "rdfs:label ?label ;" +
				  "dbo:capital ?capital ." + 
				  "FILTER ( regex(?capital, '/resource/" + name + "'))" + 
				"} LIMIT 1");
		
		ResultSet rs = dbp.query(qs);
		String capital="";
		if (rs.hasNext()) {
	
			String str = rs.next().getResource("country").toString();
			capital = getCapitalString(str);
			capital = capital.replace("_", " ");
			//System.out.print(name.replace("_", " ") + " - " + capital.replace("_", " "));
			
	    }
		return capital;
	}
	
	public String getCountryByCity(String name){
		
		name = name.replace(" ", "_");
				
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
				"SELECT DISTINCT ?country " + 
		  		  "WHERE { ?city rdf:type dbo:City ;" + 
		  		  "rdfs:label ?label ;" + 
		  		  "dbo:country ?country ;" +
		  		  "FILTER ( regex(?city, '/resource/" + name + "'))" +
		  		  "} LIMIT 1");
		
		ResultSet rs = dbp.query(qs);
		String capital="";
		if (rs.hasNext()) {
	
			String str = rs.next().getResource("country").toString();
			capital = getCapitalString(str);
			capital = capital.replace("_", " ");
			//System.out.print(name.replace("_", " ") + " - " + capital.replace("_", " "));
			
	    }
		return capital;
	}
	
	public String getCapitalByCity(String name){
		
		name = name.replace(" ", "_");
				
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
				"SELECT DISTINCT ?country " + 
		  		  "WHERE { ?city rdf:type dbo:City ;" + 
		  		  "rdfs:label ?label ;" + 
		  		  "dbo:country ?country ;" +
		  		  "FILTER ( regex(?city, '/resource/" + name + "'))" +
		  		  "} LIMIT 1");
		
		ResultSet rs = dbp.query(qs);
		String capital="";
		if (rs.hasNext()) {
	
			String str = rs.next().getResource("country").toString();
			capital = getCapitalString(str);
			capital = getCapitalByCountry(capital);
			capital = capital.replace("_", " ");
			//System.out.print(name.replace("_", " ") + " - " + capital.replace("_", " "));
			
	    }
		return capital;
	}
	
	public boolean checkCountry(String name){
		name = name.replace(" ", "_");
		
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
				"SELECT DISTINCT ?country" + 
				"WHERE { ?country rdf:type dbo:Country ;" +
				"rdfs:label ?label ;" +
				"dbo:capital ?capital ." +
				"FILTER ( regex(?country, '/resource/" + name + "'))" +
				"} LIMIT 1");
		
		ResultSet rs = dbp.query(qs);
		String capital="";
		if (rs.hasNext()) {
			return true;
	    }
		return false;
	}

	public boolean checkCapital(String name){
		name = name.replace(" ", "_");
		
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
				"SELECT DISTINCT ?capital" + 
				"WHERE { ?country rdf:type dbo:Country ;" +
				"rdfs:label ?label ;" +
				"dbo:capital ?capital ." +
				"FILTER ( regex(?capital, '/resource/" + name + "'))" +
				"} LIMIT 1");
		
		ResultSet rs = dbp.query(qs);
		String capital="";
		if (rs.hasNext()) {
			return true;
	    }
		return false;
	}

	public boolean checkCity(String name){
		name = name.replace(" ", "_");
		
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
				"SELECT DISTINCT ?city " + 
		  		  "WHERE { ?city rdf:type dbo:City ;" + 
		  		  "rdfs:label ?label ;" + 
		  		  "dbo:country ?country ;" +
		  		  "FILTER ( regex(?city, '/resource/" + name + "'))" +
		  		  "} LIMIT 1");
		
		ResultSet rs = dbp.query(qs);
		String capital="";
		if (rs.hasNext()) {
			return true;
	    }
		return false;
	}

	
	public String getCapitalString(String str){
		int lastSlash = str.lastIndexOf("/");
		return str.substring(lastSlash+1);
	}
}

class DbPediaDataPack {
	public String country;
	public String capital;
	public String city;
	public String conference;
	public boolean value;
}
