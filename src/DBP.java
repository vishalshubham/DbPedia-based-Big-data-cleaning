
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import org.apache.jena.atlas.lib.StrUtils;

//import lt.utils.LTProperties;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class DBP {
	
	public static final String dbpediaEndpoint = "http://dbpedia.org/sparql";//LTProperties.getInstance().dbpedia_sparql_endpoint;
	
	public static final String prefixes = StrUtils.strjoin("\n", new String[] {
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
			"PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>",
			"PREFIX pf:     <http://jena.hpl.hp.com/ARQ/property#>",
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>",
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>",
			"PREFIX dcterms: <http://purl.org/dc/terms/>",
			"PREFIX dbo: <http://dbpedia.org/ontology/>",
			})+ "\n";
	
	/**
	 * Executes a *SELECT* query described by ParameterizedSparqlString
	 * @param qs the ParameterizedSparqlString representing the query
	 * @return the ResultSet
	 */
	public ResultSet query(ParameterizedSparqlString qs) {
        QueryExecution exec = QueryExecutionFactory.sparqlService(dbpediaEndpoint, qs.asQuery());
        ResultSet results = ResultSetFactory.copyResults(exec.execSelect());
        return results;
	}
	
	/**
	 * Gets the resources with a label using rdfs:label
	 * @param labelString the label string to match
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the resources with label labelString
	 */
	public Set<Resource> getResourcesWithLabel(String labelString, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);
		
        ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select distinct ?resource where {\n" +
                "  ?resource rdfs:label ?label\n" +
                "}");
        
        Literal label = ResourceFactory.createLangLiteral(labelString, "en");
        qs.setParam("label", label);
        
        ResultSet rs = query(qs);
        
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("resource"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);
        
        return ret;
	}
	
	/**
	 * Gets the property value of a resource
	 * @param r the resource
	 * @param propertyName the name of the property
	 * @param printResults optional parameter to print the results, default false
	 * @return the values of the property
	 */
	public Set<Literal> getResourceProperty(Resource r, String propertyName, boolean... printResults) {
		HashSet<Literal> ret = new HashSet<Literal>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?uri ?property ?x } ");
        
        qs.setParam("uri", r);
        qs.setParam("property", ResourceFactory.createProperty(propertyName));
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getLiteral("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Gets the super classes of a resource using rdf:type
	 * @param r the resource to get the type for
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the resources that r is a type of
	 */
	public Set<Resource> getDirectTypes(Resource r, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?uri rdf:type ?x } ");
        
        qs.setParam("uri", r);
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Gets the resources that belong to a type using rdf:type
	 * @param r the type to get its resources
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the resources that belong to that type
	 */
	public Set<Resource> getResourcesForType(Resource r, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?x rdf:type ?type } ");
        
        qs.setParam("type", r);
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Gets the resources that belong to a type using rdf:type AND has 
	 * a certain property. This is similar to {@link #getResourcesForType(Resource, boolean...)} but 
	 * we limit the retrieved entities to those who have a value for the property.
	 * @param r the type to get its resources
	 * @param propertyName the propertyName to retrieve
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the resources that belong to that type and the values of their property
	 */
	public Map<Resource, Literal> getResourcesWithPropertyForType(Resource r, String propertyName, boolean... printResults) {
		HashMap<Resource, Literal> ret = new HashMap<Resource, Literal>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?res ?val where { " +
                "  ?res rdf:type ?type . "
                + "?res ?property ?val } ");
        
        qs.setParam("type", r);
        qs.setParam("property", ResourceFactory.createProperty(propertyName));
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	QuerySolution sol = rs.next();
        	ret.put(sol.getResource("res"), sol.getLiteral("val"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Gets the Wikipedia categories of a resource using dcterms:subject
	 * @param r the resource to get the categories for
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the wikiepdia categories resources that r belongs to
	 */
	public Set<Resource> getDirectCategories(Resource r, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?uri dcterms:subject ?x } ");
        
        qs.setParam("uri", r);
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Gets the resources that belong to a Wikipedia category using dcterms:subject
	 * @param r the wikipedia category to get its resources
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the resources that belong to the wikiepdia category
	 */
	public Set<Resource> getResourcesForCategory(Resource r, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?x dcterms:subject ?cat } ");
        
        qs.setParam("cat", r);
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);
        
        return ret;
	}
	
	/**
	 * Gets the resources that belong to a Wikipedia category using dcterms:subject
	 * and that have a specific property
	 * @param r the wikipedia category to get its resources
	 * @param propertyName the propertyName to get the value for
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the resources that belong to the wikiepdia category and the values of their property
	 */
	public Map<Resource, Literal> getResourcesWithPropertyForCategory(Resource r, String propertyName, boolean... printResults) {
		HashMap<Resource, Literal> ret = new HashMap<Resource, Literal>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?res ?val where { " 
				+ " ?res dcterms:subject ?cat ."
                + " ?res ?property ?val } ");
        
        qs.setParam("cat", r);
        qs.setParam("property", ResourceFactory.createProperty(propertyName));
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	QuerySolution sol = rs.next();
        	ret.put(sol.getResource("res"), sol.getLiteral("val"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);
        
        return ret;
	}
	
	/**
	 * Retrieve a list of the broader categories using skos:broader
	 * @param category
	 * @param printResults
	 * @return
	 */
	public Set<Resource> getBroaderCategories(Resource category, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?cat skos:broader ?x } ");
        
        qs.setParam("cat", category);
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Gets the super classes of a resource using rdfs:subClassOf
	 * @param r the resource to get the super classes for
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the resources that are a super class of r
	 */
	public Set<Resource> getDirectSuperClasses(Resource r, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?uri rdfs:subClassOf ?x } ");
        
        qs.setParam("uri", r);
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Gets the resources (subclasses) that belong to a class using rdfs:subClassOf
	 * @param r the class to get its resources
	 * @param printResults optional parameter to print the results, default false
	 * @return a set of the subclasses that belong to that class
	 */
	public Set<Resource> getResourcesForClass(Resource r, boolean... printResults) {
		HashSet<Resource> ret = new HashSet<Resource>();
		boolean print = (printResults.length > 0 && printResults[0] == true);

		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes +
                "select DISTINCT ?x where { " +
                " ?x rdfs:subClassOf ?cls } ");
        
        qs.setParam("cls", r);
        
        ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        if(print)
        	ResultSetFormatter.out(rs);

        return ret;
	}
	
	/**
	 * Recursively get all sub-classes (transitive) of a resource using rdfs:subClassOf+
	 * 
	 * FIXME: the limit does not work; it returns results < the limit 
	 * 
	 * @param r the source to get the subclasses for 
	 * @param limit optional to limit the number of subclasses to retrieve
	 * @return as set of the recursive subclasses
	 */
	public Set<Resource> getAllSubClasses_Transitive(Resource r, int... limit) {
		HashSet<Resource> ret = new HashSet<Resource>();
		int l = limit.length > 0? limit[0] : -1;
		
		String queryString = prefixes +
				"SELECT ?x WHERE \n" + 
				" { { SELECT *  WHERE \n" +
				"     { ?x rdfs:subClassOf+ ?y . } \n" +
				"   } \n" +
				" FILTER (?y = <" + r.getURI() +">) } " + (l > 0 ? (" limit " + l) : "");
		ParameterizedSparqlString qs = new ParameterizedSparqlString(prefixes + queryString);
		
		ResultSet rs = query(qs);
        while (rs.hasNext()) {
        	ret.add(rs.next().getResource("x"));
        }
        
        return ret;
	}
		
	/**
	 * Recursively check if a resource is a super class of another resource
	 * @param r1 resource 1
	 * @param r2 resource 2
	 * @return true if r1 is a super class of r2, false otherwise
	 */
	public boolean isSuperClassOf(Resource r1, Resource r2) {
		if (r1.equals(r2))
			return true;
		Set<Resource> r1SubClasses = getAllSubClasses_Transitive(r1);
		if(r1SubClasses.contains(r2))
			return true;
		return false;
	}
	
	/**
	 * Gets the least common ancestor node between two resources (usually classes) 
	 * in the ontology using subclasses only.
	 * @param r1 the first resource
	 * @param r2 the second resource
	 * @return the least common ancestor (least common super, LCS) nodes of r1 and r2
	 */
	public Set<Resource> getLeastCommonAncestor(Resource r1, Resource r2) {
		HashSet<Resource> ret = new HashSet<>();
		
		// base case, equal nodes
		if(r1.equals(r2)) {
			ret.add(r1);
			return ret;
		}
		
		// get direct super classes of r1
		Set<Resource> r1Parents = getDirectSuperClasses(r1);
		Set<Resource> r2Parents = getDirectSuperClasses(r2);
		
		for (Resource r1Parent : r1Parents) {
			if (r1Parent.equals(r1))
				continue;
			Set<Resource> temp = getLeastCommonAncestor(r1Parent, r2);
			ret.addAll(temp);
		}
		
		for (Resource r2Parent : r2Parents) {
			if (r2Parent.equals(r2))
				continue;
			Set<Resource> temp = getLeastCommonAncestor(r1, r2Parent);
			ret.addAll(temp);
		}
		
		boolean tag = true;
		while (tag) {
			tag = false;
			for (Resource s1 : ret) {
				for (Resource s2 : ret) {
					if (s1.equals(s2))
						continue;
					if (isSuperClassOf(s1, s2)) {
						ret.remove(s1);
						tag = true;
						break;
					}
				}
				if (tag == true)
					break;
			}
		}
		
		return ret;
	}
}
