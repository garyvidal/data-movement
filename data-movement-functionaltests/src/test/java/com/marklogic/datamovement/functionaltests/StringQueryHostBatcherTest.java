/**
 * 
 */
package com.marklogic.datamovement.functionaltests;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawQueryByExampleDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.JobTicket;
import com.marklogic.datamovement.QueryHostBatcher;
import com.marklogic.datamovement.WriteHostBatcher;


/**
 * @author ageorge
 * Purpose : Test String Queries 
 * - On multiple documents using Java Client DocumentManager Write method and WriteHostbatcher.
 * - On meta-data.
 * - On non-existent document. Verify error message.
 * - With invalid string query. Verify error message.
 *
 */
public class StringQueryHostBatcherTest extends  BasicJavaClientREST {
	private static String dbName = "StringQueryHostBatcherDB";
	private static String [] fNames = {"StringQueryHostBatcherDB-1"};
	private static DataMovementManager dmManager = null;
	private static DataMovementManager moveMgr = DataMovementManager.newInstance();
	private static String restServerHost = null;
	private static String restServerName = null;
	private static int restServerPort = 0;
	private static DatabaseClient clientQHB = null;
	private static DatabaseClient client = null;
	private static String dataConfigDirPath = null;
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		loadGradleProperties();
		restServerPort = getRestAppServerPort();
		restServerHost = getRestAppServerHostName();
		
		restServerName = getRestAppServerName();
		dmManager = DataMovementManager.newInstance();
		dataConfigDirPath = getDataConfigDirPath();
		
		setupJavaRESTServer(dbName, fNames[0], restServerName, restServerPort);
		setupAppServicesConstraint(dbName);
		
		createUserRolesWithPrevilages("test-eval","xdbc:eval", "xdbc:eval-in","xdmp:eval-in","any-uri","xdbc:invoke");
	    createRESTUser("eval-user", "x", "test-eval","rest-admin","rest-writer","rest-reader","rest-extension-user","manage-user");
	    
	    // For use with Java/REST Client API
	    client = DatabaseClientFactory.newClient(restServerHost, restServerPort, "admin", "admin", Authentication.DIGEST);
	    // For use with QueryHostBatcher
	    clientQHB = DatabaseClientFactory.newClient(restServerHost, restServerPort, "eval-user", "x", Authentication.DIGEST);
	    moveMgr.setClient(clientQHB);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("In tearDownAfterClass");
		// Release clients
		client.release();
		clientQHB.release();
		associateRESTServerWithDB(restServerName, "Documents" );
		deleteRESTUser("eval-user");
		deleteDB(dbName);
		deleteForest(fNames[0]);		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		System.out.println("In setup");		  
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		System.out.println("In tearDown");
		clearDB(restServerPort);
	}
	
	/*
	 * To test String query with Document Manager (Java Client API write method) and WriteHostbatcher.
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws XpathException
	 */

	@Test
	public void testAndWordQuery() throws IOException, ParserConfigurationException, SAXException, InterruptedException
	{	
		System.out.println("Running testAndWordQuery");
		
		String[] filenames = {"constraint1.xml", "constraint2.xml", "constraint3.xml", "constraint4.xml", "constraint5.xml"};
		String queryOptionName = "absRangeConstraintWithVariousGrammarAndWordQueryOpt.xml";
			
		// write docs using Java Client API
		for(String filename : filenames) {
			writeDocumentUsingInputStreamHandle(client, filename, "/abs-range-constraint/", "XML");
		}
		
		setQueryOption(client, queryOptionName);
		
		QueryManager queryMgr = client.newQueryManager();
				
		// create query def
		StringQueryDefinition querydef = queryMgr.newStringDefinition(queryOptionName);
		querydef.setCriteria("(pop:high OR pop:medium) AND price:medium AND intitle:served");

		// create handle to search using Java Client API.
		JacksonHandle jh = new JacksonHandle();
		JsonNode jsonResults = queryMgr.search(querydef, jh).get();
		
		// Verify the results. 
		JsonNode searchResult = jsonResults.get("results").get(0);
		assertEquals(1, searchResult.get("index").asInt());
		assertEquals("/abs-range-constraint/constraint4.xml", searchResult.get("uri").asText());
		String contents = searchResult.get("content").asText();
		assertTrue("Expected String not available", contents.contains("Vannevar served"));
		assertTrue("Expected amt not available", contents.contains("12.34"));
						
		//Use WriteHostbatcher to write the same files.				
		WriteHostBatcher batcher = moveMgr.newWriteHostBatcher();
		
		batcher.withBatchSize(2);
		InputStreamHandle contentHandle1 = new InputStreamHandle();
		contentHandle1.set(new FileInputStream(dataConfigDirPath + filenames[0]));
		InputStreamHandle contentHandle2 = new InputStreamHandle();
		contentHandle2.set(new FileInputStream(dataConfigDirPath + filenames[1]));
		InputStreamHandle contentHandle3 = new InputStreamHandle();
		contentHandle3.set(new FileInputStream(dataConfigDirPath + filenames[2]));
		InputStreamHandle contentHandle4 = new InputStreamHandle();
		contentHandle4.set(new FileInputStream(dataConfigDirPath + filenames[3]));
		InputStreamHandle contentHandle5 = new InputStreamHandle();
		contentHandle5.set(new FileInputStream(dataConfigDirPath + filenames[4]));
				
		batcher.add("/abs-range-constraint/batcher-contraints1.xml", contentHandle1);
		batcher.add("/abs-range-constraint/batcher-contraints2.xml", contentHandle2);
		batcher.add("/abs-range-constraint/batcher-contraints3.xml", contentHandle3);
		batcher.add("/abs-range-constraint/batcher-contraints4.xml", contentHandle4);
		batcher.add("/abs-range-constraint/batcher-contraints5.xml", contentHandle5);
		
		// Verify if the batch flushes when batch size is reached.
		// Flush
		batcher.flush();
		// Hold for asserting the callbacks batch contents, since callback are on different threads than the main JUnit thread.
		// JUnit can not assert on different threads; other than the main one. 
		StringBuffer batchResults  = new StringBuffer();
		StringBuffer batchFailResults  = new StringBuffer();
				
		// Run a QueryHostBatcher on the new URIs.
		QueryHostBatcher queryBatcher1 = moveMgr.newQueryHostBatcher(querydef);
		JobTicket jobTicket = moveMgr.startJob(queryBatcher1);
		
		queryBatcher1.onUrisReady((client1, batch)-> {
			// We will have two items from the search. One from Java/REST insert. Other from WriteHostbatcher add. 			
			for (String str : batch.getItems()) {
				batchResults.append(str);
				batchResults.append("|");
			}
						
			batchResults.append(batch.getBytesMoved());
			batchResults.append("|");
			
			batchResults.append(batch.getForest().getForestName());
			batchResults.append("|");
			
			batchResults.append(batch.getBatchNumber());
			batchResults.append("|");
				        
	        });
		queryBatcher1.onQueryFailure((client1, throwable)-> {        	
        	System.out.println("Exceptions thrown from callback onQueryFailure");        	
            throwable.printStackTrace();
          	batchFailResults.append("Test has Exceptions");          	
        } );
		
		boolean bJobFinished = queryBatcher1.awaitTermination(3, TimeUnit.MINUTES);
		
		if (bJobFinished) {
			
			if( !batchFailResults.toString().isEmpty() && batchFailResults.toString().contains("Exceptions"))
				fail("Test failed due to exceptions"); 

			// Verify the batch results now.
			String[] res = batchResults.toString().split("\\|");

			assertTrue("URI returned not correct", res[0].contains("/abs-range-constraint/batcher-contraints4.xml")
					|| res[1].contentEquals("/abs-range-constraint/batcher-contraints4.xml"));

			assertTrue("URI returned not correct", res[0].contains("/abs-range-constraint/constraint4.xml")
					|| res[1].contentEquals("/abs-range-constraint/constraint4.xml"));
			assertEquals("Bytes Moved","0", res[2]);
			assertEquals("Forest Name",res[3], fNames[0]);
			assertEquals("Batch Number","0", res[4]);
		}		
	}
	
	/*
	 * To test query by example with WriteHostbatcher and QueryHostBatcher.
	 * @throws IOException
	 * @throws InterruptedException
	 */

	@Test
	public void testQueryByExample() throws IOException, InterruptedException
	{	
		System.out.println("Running testQueryByExample");

		String[] filenames = {"constraint1.json", "constraint2.json", "constraint3.json", "constraint4.json", "constraint5.json"};
		//Use WriteHostbatcher to write the same files.				
		WriteHostBatcher batcher = moveMgr.newWriteHostBatcher();

		batcher.withBatchSize(2);
	
		InputStreamHandle contentHandle1 = new InputStreamHandle();
		contentHandle1.set(new FileInputStream(dataConfigDirPath + filenames[0]));
		InputStreamHandle contentHandle2 = new InputStreamHandle();
		contentHandle2.set(new FileInputStream(dataConfigDirPath + filenames[1]));
		InputStreamHandle contentHandle3 = new InputStreamHandle();
		contentHandle3.set(new FileInputStream(dataConfigDirPath + filenames[2]));
		InputStreamHandle contentHandle4 = new InputStreamHandle();
		contentHandle4.set(new FileInputStream(dataConfigDirPath + filenames[3]));
		InputStreamHandle contentHandle5 = new InputStreamHandle();
		contentHandle5.set(new FileInputStream(dataConfigDirPath + filenames[4]));

		batcher.add("/batcher-contraints1.json", contentHandle1);
		batcher.add("/batcher-contraints2.json", contentHandle2);
		batcher.add("/batcher-contraints3.json", contentHandle3);
		batcher.add("/batcher-contraints4.json", contentHandle4);
		batcher.add("/batcher-contraints5.json", contentHandle5);
		
		// Flush
		batcher.flush();
		StringBuffer batchResults  = new StringBuffer();
		StringBuffer batchFailResults  = new StringBuffer();
		
		// get the query
		File file = new File(dataConfigDirPath + "qbe1.json");
		FileHandle fileHandle = new FileHandle(file);

		QueryManager queryMgr = client.newQueryManager();
		RawQueryByExampleDefinition qbyexDef = queryMgr.newRawQueryByExampleDefinition(fileHandle.withFormat(Format.JSON));

		// Run a QueryHostBatcher.
		QueryHostBatcher queryBatcher1 = moveMgr.newQueryHostBatcher(qbyexDef);
		JobTicket jobTicket = moveMgr.startJob(queryBatcher1);
		
		queryBatcher1.onUrisReady((client1, batch)-> {
						
			for (String str : batch.getItems()) {
				batchResults.append(str);
				batchResults.append("|");
			}

			batchResults.append(batch.getBytesMoved());
			batchResults.append("|");

			batchResults.append(batch.getForest().getForestName());
			batchResults.append("|");

			batchResults.append(batch.getBatchNumber());
			batchResults.append("|");

		});
		queryBatcher1.onQueryFailure((client1, throwable)-> {        	
			System.out.println("Exceptions thrown from callback onQueryFailure");        	
			throwable.printStackTrace();
			batchFailResults.append("Test has Exceptions");          	
		} );
		
		boolean bJobFinished = queryBatcher1.awaitTermination(3, TimeUnit.MINUTES);
		
		if (bJobFinished) {
			
			if( !batchFailResults.toString().isEmpty() && batchFailResults.toString().contains("Exceptions"))
				fail("Test failed due to exceptions"); 

			// Verify the batch results now.
			String[] res = batchResults.toString().split("\\|");

			assertTrue("URI returned not correct", res[0].contains("/batcher-contraints1.json"));
			assertEquals("Bytes Moved","0", res[1]);
			assertEquals("Forest Name",res[2], fNames[0]);
			assertEquals("Batch Number","0", res[3]);
		}	
	}
}
