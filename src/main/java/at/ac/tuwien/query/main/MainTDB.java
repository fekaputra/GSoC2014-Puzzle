package at.ac.tuwien.query.main;


import java.io.FileWriter;
import java.io.IOException;

import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.OntProperty;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;


public class MainTDB {
   public static void main(String[] args) { 
       String defaultURI;
       
       // Load a TDB-backed dataset
       //   the owl file already converted to TDB beforehand using TDBLoader 
       String directory = "tdb/db" ;
       Dataset dataset = TDBFactory.createDataset(directory) ;

       // Get model inside the read transaction to retrieve the defaultURI 
       dataset.begin(ReadWrite.READ) ;
       try {
           Model model = dataset.getDefaultModel() ;
           defaultURI = model.getNsPrefixURI("");
       } finally {
           dataset.end() ;
       }
       
       // Create the new class and several instances 
       dataset.begin(ReadWrite.WRITE) ;
       try {
           OntModel model = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM, dataset.getDefaultModel());
           
           // create the person class - Could do it in protege, but easier to do it here.
           OntClass person = model.createClass(defaultURI+"Person");
           OntProperty name = model.createOntProperty(defaultURI+"name");
           OntProperty email = model.createOntProperty(defaultURI+"email");
           
           // create the first person
           Individual person01 = person.createIndividual(defaultURI+"Person_"+"01");
           person01.addProperty(name, "Fajar");
           person01.addProperty(email, "fajar.juang@gmail.com");

           // create the second person
           Individual person02 = person.createIndividual(defaultURI+"Person_"+"02");
           person02.addProperty(name, "Richard");
           person02.addProperty(email, "richard.mordinyi@tuwien.ac.at");
           
           // queryA: SPARQL query for getting all the person and its attribute
           ParameterizedSparqlString queryA = new ParameterizedSparqlString();
           queryA.setNsPrefixes(model.getNsPrefixMap());
           queryA.append("select * where {?person a :Person . ?person :name ?name . ?person :email ?email }");
           doQuery(model, queryA.toString());
           
           // queryB: SPARQL query for getting experimentID & its respective publication title 
           //   currently it's impossible, since the publication title scattered in the bibtex property of the publication concept
           //   So instead, I put the publicationID
           ParameterizedSparqlString queryB = new ParameterizedSparqlString();
           queryB.setNsPrefixes(model.getNsPrefixMap());
           queryB.append("select ?ex ?bib where { ?pub :hasPublicationExperiment ?ex . ?pub :publicationID ?bib }");
           doQuery(model, queryB.toString());
           
           // get the changed data model and write it to file, instead of saving it to the TDB
           save(model, "tdb/output.owl");
           
       } finally {
           // do not persist the changes
           //   if you want to persist it, change to database.commit();
           dataset.end();
       }

       // make sure that the TDB synchronized
       TDB.sync(dataset);
   }

    private static void doQuery(OntModel model, String queryA) {
        QueryExecution qe = QueryExecutionFactory.create(queryA, model);
        ResultSetFormatter.out(System.out, qe.execSelect());
        
        qe.close();
    }

    public static void save(Model model, String outFile) {
        try {
            model.write(new FileWriter(outFile), "RDF/XML");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
   
}
