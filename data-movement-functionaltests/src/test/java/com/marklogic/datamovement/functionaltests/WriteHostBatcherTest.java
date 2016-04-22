package com.marklogic.datamovement.functionaltests;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.datamovement.Batch;
import com.marklogic.datamovement.BatchFailureListener;
import com.marklogic.datamovement.BatchListener;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.Forest;
import com.marklogic.datamovement.ForestConfiguration;
import com.marklogic.datamovement.HostBatcher;
import com.marklogic.datamovement.WriteEvent;
import com.marklogic.datamovement.WriteHostBatcher;


public class WriteHostBatcherTest extends  BasicJavaClientREST {
	
	private static String dbName = "WriteHostBatcher";
	private static String [] fNames = {"WriteHostBatcher-1"};
	private static DataMovementManager dmManager = DataMovementManager.newInstance();
	private static final String TEST_DIR_PREFIX = "/testdata/";
	private static DatabaseClient dbClient;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		setupJavaRESTServer(dbName, fNames[0], "App-Services",8000);
		dbClient = DatabaseClientFactory.newClient("localhost", 8000, "admin", "admin", Authentication.DIGEST);
		dmManager.setClient(dbClient);
		/*createDB(dbName);
		Thread.currentThread().sleep(2000L);
		createForest(fNames[0],dbName);
		associateRESTServerWithDB("App-Services",dbName );*/
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		associateRESTServerWithDB("App-Services","Documents" );
		deleteForest(fNames[0]);
		deleteDB(dbName);
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		dmManager.stopJob(null);
	}
	/*
	@Test
	public void testAssign()  {
		ForestConfiguration fc = dmManager.readForestConfig();
		
		try{
			Forest f = fc.assign("");
			System.out.println("Forest ID:" +f.getForestId());
		}
		catch(Exception e){
			e.getMessage();
		}
		
		try{
			Forest f = fc.assign("$#@#$");
			System.out.println("Forest ID:" +f.getForestId());
		}
		catch(Exception e){
			e.getMessage();
		}
		
		try{
			Forest f = fc.assign(" ");
			System.out.println("Forest ID:" +f.getForestId());
		}
		catch(Exception e){
			e.getMessage();
		}
		
		try{
			Forest f = fc.assign(null);
			System.out.println("Forest ID:" +f.getForestId());
		}
		catch(Exception e){
			e.getMessage();
		}
	}
	
	@Test
	public void testListForests() {
	
	}
	
	@Test
	public void testGetForestClient()  {
	
	}
	
	@Test
	public void testAssignmentPolicy(){
	
	}
	
	@Test
	public void testHostBatcher()  {
	
	}
	
	@Test
	public void testFlush()  {
	
	}
	
	@Test
	public void testJob()  {
	
	}*/
	
		
	@Test
	public void testAdd() throws Exception{
		ClassLoader classLoader = getClass().getClassLoader();
		WriteHostBatcher ihb = dmManager.newWriteHostBatcher();
		ihb.onBatchFailure(new BatchFailureListener<WriteEvent>() {
			
			public void processEvent(DatabaseClient forestClient,
					Batch<WriteEvent> batch, Throwable throwable) {
				// TODO Auto-generated method stub
				System.out.println(throwable.getMessage());
				System.out.println(batch.getItems().length);
				System.out.println("ABCD "+batch.getItems()[0].getTargetUri());
				
			}
		}).onBatchSuccess(new BatchListener<WriteEvent>() {
			
			public void processEvent(DatabaseClient client, Batch<WriteEvent> batch) {
				// TODO Auto-generated method stub
				System.out.println("ABCDEF "+batch.getItems()[0].getTargetUri());
				
			}
		});
		
		JsonNode actualObj = new ObjectMapper().readTree("{\"k1\":\"v1\"}");
		JacksonHandle jh = new JacksonHandle(actualObj);
		
		String ntriple5 = "<http://example.org/s5> <http://example.com/p2> <http://example.org/o2> .";
		StringHandle sh = new StringHandle(ntriple5).withMimetype("application/n-triples");
		
		
		// read docs
		FileHandle contentHandle = new FileHandle(FileUtils.toFile(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX+"dir.json")));
		contentHandle.setFormat(Format.JSON);
		
		
		dmManager.startJob(ihb);
		ihb.withBatchSize(1);
		ihb.add("/local/json", jh).add("/local/filejson", contentHandle).add("/local/ntriples", sh);
		ihb.flush();
		System.out.println("fnkdsfnk");
		
	}
	

}
