package com.marklogic.datamovement.functionaltests;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.admin.ExtensionMetadata;
import com.marklogic.client.admin.TransformExtensionsManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.impl.DatabaseClientImpl;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.OutputStreamHandle;
import com.marklogic.client.io.OutputStreamSender;
import com.marklogic.client.io.ReaderHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.datamovement.DataMovementManager;
import com.marklogic.datamovement.WriteEvent;
import com.marklogic.datamovement.WriteHostBatcher;
import com.marklogic.datamovement.functionaltests.util.BasicJavaClientREST;



public class WriteHostBatcherTest extends  BasicJavaClientREST {
	
	private static String dbName = "WriteHostBatcher";
	private static DataMovementManager dmManager = DataMovementManager.newInstance();
	private static final String TEST_DIR_PREFIX = "/WriteHostBatcher-testdata/";
	
	private static DatabaseClient dbClient;
	private static String host = "localhost";
	private static String user = "admin";
	private static int port = 9876;
	private static String password = "admin";
	
	private static JacksonHandle jacksonHandle;
	private static StringHandle stringHandle;
	private static FileHandle fileHandle;
	private static DOMHandle domHandle;
	private static BytesHandle bytesHandle;
	private static InputStreamHandle isHandle;
	private static ReaderHandle readerHandle;
	private static OutputStreamHandle osHandle;
	private static DocumentMetadataHandle docMeta1;
	private static DocumentMetadataHandle docMeta2;
	private static ReaderHandle readerHandle1;
	private static OutputStreamHandle osHandle1;
	private static WriteHostBatcher ihbMT;
	private static JsonNode clusterInfo;
	private static String[] hostNames ;
	
	private static String stringTriple;
	private static File fileJson;
	private static Document docContent;

	private static FileInputStream inputStream;
	private static OutputStreamSender sender;
	private static OutputStreamSender sender1;
    private static BufferedReader docStream;
    private static BufferedReader docStream1;
    private static byte[] bytesJson;
	private static JsonNode jsonNode;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		hostNames = getHosts();	    
	  createDB(dbName);
		Thread.currentThread().sleep(1000L);
		int count = 1;
		for ( String forestHost : hostNames ) {
			createForestonHost(dbName+"-"+count,dbName,forestHost);
		    count ++;
			Thread.currentThread().sleep(1000L);
		}
			
		assocRESTServer(dbName+"-Server",dbName,port);
		
		dbClient = DatabaseClientFactory.newClient(host, port, user, password, Authentication.DIGEST);
		dmManager.setClient(dbClient);
		
		clusterInfo = ((DatabaseClientImpl) dbClient).getServices()
			      .getResource(null, "forestinfo", null, null, new JacksonHandle())
			      .get();
		
		//JacksonHandle
		jsonNode = new ObjectMapper().readTree("{\"k1\":\"v1\"}");
		jacksonHandle = new JacksonHandle();
		jacksonHandle.set(jsonNode);
		
		//StringHandle
		stringTriple = "<abc>xml</abc>";
		stringHandle = new StringHandle(stringTriple);
		
		
		// FileHandle
		fileJson = FileUtils.toFile(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX+"dir.json"));
		fileHandle = new FileHandle(fileJson);
		fileHandle.setFormat(Format.JSON);
		

	    
	    //DomHandle
		DocumentBuilderFactory dbfac = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = dbfac.newDocumentBuilder();
		docContent = docBuilder.parse(FileUtils.toFile(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX+"xml-original-test.xml")));			 
	   
	  	domHandle = new DOMHandle();
		domHandle.set(docContent);



	    
	
	    
	    docMeta1= new DocumentMetadataHandle()
		 .withCollections("Sample Collection 1").withProperty("docMeta-1", "true").withQuality(1);
		docMeta1.setFormat(Format.XML);
		 
		docMeta2 = new DocumentMetadataHandle()
		 .withCollections("Sample Collection 2").withProperty("docMeta-2", "true").withQuality(0);
		docMeta1.setFormat(Format.XML);
		
		
	}
	
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		associateRESTServerWithDB(dbName+"-Server","Documents" );
		for (int i =0 ; i < clusterInfo.size(); i++){
			System.out.println(dbName+"-"+(i+1));
			detachForest(dbName, dbName+"-"+(i+1));
			deleteForest(dbName+"-"+(i+1));
		}
		
		deleteDB(dbName);
	}

	

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
		dmManager.stopJob(null);
		Map<String, String> props = new HashMap<String, String>();
 		props.put("group-id","Default");
 		props.put("view","status");
		
 		JsonNode output = getState(props, "/manage/v2/servers/"+dbName+"-Server").path("server-status").path("status-properties");
 		props.clear();
 		String s = output.findValue("enabled").get("value").asText();
 		System.out.println("S is "+s);
		if(s.trim().equals("false")){
			props.put("server-name",dbName+"-Server");
			props.put("group-name", "Default");
			props.put("enabled", "true");
   			changeProperty(props,"/manage/v2/servers/"+dbName+"-Server/properties");
		}
			
		clearDB(port);

	}
	

	private void replenishStream() throws Exception{
		
		// InputStreamHandle
		isHandle = new InputStreamHandle();
		inputStream = new FileInputStream(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX+"myJSONFile.json").getPath());
		isHandle.withFormat(Format.JSON);
		isHandle.set(inputStream);
		
		// OutputStreamHandle
		sender = new OutputStreamSender() {
            // the callback receives the output stream
			public void write(OutputStream out) throws IOException {
        		// acquire the content
				InputStream docStream = new FileInputStream(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX + "WrongFormat.json").getPath());
				
        		// copy content to the output stream
        		byte[] buf = new byte[1024];
        		int byteCount = 0;
        		while ((byteCount=docStream.read(buf)) != -1) {
        			out.write(buf, 0, byteCount);
        		}
        		
            }
        };
        
        // create the handle
        osHandle = new OutputStreamHandle(sender);
	    osHandle.withFormat(Format.JSON);
	
		sender1 = new OutputStreamSender() {
            // the callback receives the output stream
			public void write(OutputStream out) throws IOException {
        		// acquire the content
				InputStream docStream = new FileInputStream(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX + "product-apple.json").getPath());
				
        		// copy content to the output stream
        		byte[] buf = new byte[1024];
        		int byteCount = 0;
        		while ((byteCount=docStream.read(buf)) != -1) {
        			out.write(buf, 0, byteCount);
        		}
            }
        };
        
        // create the handle
        osHandle1 = new OutputStreamHandle(sender1);
	    osHandle1.withFormat(Format.JSON);
	    

	    
		// ReaderHandle
		docStream = new BufferedReader(new FileReader(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX + "WrongFormat.xml").getPath()));
	    readerHandle = new ReaderHandle();
	    readerHandle.withFormat(Format.XML);
	    readerHandle.set(docStream);
		
	    
	    docStream1 = new BufferedReader(new FileReader(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX + "employee.xml").getPath()));
	    readerHandle1 = new ReaderHandle();
	    readerHandle1.withFormat(Format.XML);
	    readerHandle1.set(docStream1);
	    
		//BytesHandle
		File file = FileUtils.toFile(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX+"dir.json"));		 
	    FileInputStream fis = new FileInputStream(file);
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    byte[] buf = new byte[1024];
	    for (int readNum; (readNum = fis.read(buf)) != -1;) 
	    {
	        bos.write(buf, 0, readNum);
	    }
	    
	    bytesJson = bos.toByteArray();
	    
	    fis.close();
	    bos.close();
	    	    
	    bytesHandle = new BytesHandle();
	    bytesHandle.setFormat(Format.JSON);
	    bytesHandle.set(bytesJson);
	}
	
	// ISSUE 45
	@Test
	public void testAdd() throws Exception{
	    final StringBuffer successBatch = new StringBuffer();
	    final StringBuffer failureBatch = new StringBuffer();
	    final String query1 = "fn:count(fn:doc())";

    	// Test 1 few failures with add (batchSize =1)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(1);
		ihb1.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb1);
		ihb1.add("/doc/jackson", jacksonHandle).add("/doc/reader_wrongxml", readerHandle).add("/doc/string", docMeta1, stringHandle).add("/doc/file", docMeta2, fileHandle).add("/doc/is", isHandle)
		.add("/doc/os_wrongjson", docMeta2, osHandle).add("/doc/bytes", docMeta1, bytesHandle).add("/doc/dom", domHandle);
		
		
		ihb1.flush();
		
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==6);
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/os_wrongjson"));
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));

    	DocumentMetadataHandle  mHandle = readMetadataFromDocument(dbClient, "/doc/string", "XML");
    	Assert.assertEquals(1,mHandle.getQuality());
    	Assert.assertEquals("Sample Collection 1",mHandle.getCollections().iterator().next());
     	Assert.assertTrue(mHandle.getCollections().size()==1);
    	
    	DocumentMetadataHandle  mHandle1 = readMetadataFromDocument(dbClient, "/doc/file", "XML");
    	Assert.assertEquals(0,mHandle1.getQuality());
    	Assert.assertEquals("Sample Collection 2",mHandle1.getCollections().iterator().next());
     	Assert.assertTrue(mHandle1.getCollections().size()==1);
     	
     	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
    	
    	
    	//ISSUE # 38
	    // Test 2 All failure with add (batchSize =8)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
	    replenishStream();
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(8);
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb2);
		ihb2.add("/doc/jackson", jacksonHandle).add("/doc/reader_wrongxml", readerHandle).add("/doc/string", docMeta1, stringHandle).add("/doc/file", docMeta2, fileHandle).add("/doc/is", isHandle)
		.add("/doc/os_wrongjson", docMeta2, osHandle).add("/doc/bytes", docMeta1, bytesHandle).add("/doc/dom", domHandle);
		
		
		ihb2.flush();
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
     	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));
    	
    	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
   
	 // Test 3 All success with add (batchSize =8)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb3 =  dmManager.newWriteHostBatcher();
		ihb3.withBatchSize(8);
		ihb3.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        		 
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		 
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb3);
		
		ihb3.add("/doc/jackson", docMeta2, jacksonHandle).add("/doc/reader_xml",docMeta1, readerHandle1).add("/doc/string", stringHandle).add("/doc/file",  fileHandle).add("/doc/is", docMeta2,isHandle)
		.add("/doc/os_json",  osHandle1).add("/doc/bytes",  bytesHandle).add("/doc/dom", docMeta1, domHandle);
		
		
		ihb3.flush();
		System.out.println("Size is "+dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue());
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==8);
    	
    	DocumentMetadataHandle  mHandle2 = readMetadataFromDocument(dbClient, "/doc/reader_xml", "XML");
    	Assert.assertEquals(1,mHandle2.getQuality());
    	Assert.assertEquals("Sample Collection 1",mHandle2.getCollections().iterator().next());
     	Assert.assertTrue(mHandle2.getCollections().size()==1);
    	
    	DocumentMetadataHandle  mHandle3 = readMetadataFromDocument(dbClient, "/doc/jackson", "XML");
    	Assert.assertEquals(0,mHandle3.getQuality());
    	Assert.assertEquals("Sample Collection 2",mHandle3.getCollections().iterator().next());
     	Assert.assertTrue(mHandle3.getCollections().size()==1);

     	Assert.assertTrue(uriExists(successBatch.toString(),"/doc/os_json"));
    	Assert.assertFalse(uriExists(successBatch.toString(),"/doc/reader_wrongxml"));
         	
     	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
    	
		// Test 4 All failures in 2 batches
    	Thread.currentThread().sleep(1500L);
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb4 =  dmManager.newWriteHostBatcher();
		ihb4.withBatchSize(4);
		ihb4.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        		 
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		 
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb4);
		
		ihb4.add("/doc/jackson", docMeta2, jacksonHandle).add("/doc/reader_wrongxml",docMeta1, readerHandle).add("/doc/string", stringHandle).add("/doc/file",  fileHandle);
		ihb4.flush();
		
		ihb4.add("/doc/is", docMeta2,isHandle).add("/doc/os_wrongjson",  osHandle).add("/doc/bytes",  bytesHandle).add("/doc/dom", docMeta1, domHandle);
		ihb4.flush();
		
		
		Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/os_wrongjson"));
    	
    	
	}
	
	//ISSUE 33
	@Test
	public void testAddAs() throws Exception{
	    final StringBuffer successBatch = new StringBuffer();
	    final StringBuffer failureBatch = new StringBuffer();
	    final String query1 = "fn:count(fn:doc())";

    	// Test 1 few failures with addAs (batchSize =1)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(1);
		ihb1.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		System.out.println("Success:"+ w.getTargetUri()+":");
		        		successBatch.append(w.getTargetUri()+":");
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  throwable.printStackTrace();
		        	  for(WriteEvent w: batch.getItems()){
		        		  System.out.println("Failure:"+ w.getTargetUri()+":");
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb1);
		// Issue with adding json using File Object
		//ihb1.addAs("/doc/string", docMeta1, fileJson);
		ihb1.addAs("/doc/jackson", jsonNode).addAs("/doc/reader_wrongxml", docStream).addAs("/doc/string", docMeta1, stringTriple).addAs("/doc/file", docMeta2, fileJson)
		.addAs("/doc/is", inputStream).addAs("/doc/bytes", docMeta1, bytesJson).addAs("/doc/dom", docContent);
		
		
		ihb1.flush();
		
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==6);
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/os_wrongjson"));
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));

    	DocumentMetadataHandle  mHandle = readMetadataFromDocument(dbClient, "/doc/string", "XML");
    	Assert.assertEquals(1,mHandle.getQuality());
    	Assert.assertEquals("Sample Collection 1",mHandle.getCollections().iterator().next());
     	Assert.assertTrue(mHandle.getCollections().size()==1);
    	
    	DocumentMetadataHandle  mHandle1 = readMetadataFromDocument(dbClient, "/doc/file", "XML");
    	Assert.assertEquals(0,mHandle1.getQuality());
    	Assert.assertEquals("Sample Collection 2",mHandle1.getCollections().iterator().next());
     	Assert.assertTrue(mHandle1.getCollections().size()==1);
     	
     	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
    	
    	
    	//ISSUE # 38
	    // Test 2 All failure with addAs (batchSize =8)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
	    replenishStream();
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(8);
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb2);
		ihb2.addAs("/doc/jackson", jsonNode).addAs("/doc/reader_wrongxml", docStream).addAs("/doc/string", docMeta1, stringTriple).addAs("/doc/file", docMeta2, fileJson).addAs("/doc/is", inputStream)
		.addAs("/doc/bytes", docMeta1, bytesJson).addAs("/doc/dom", docContent);
		
		
		ihb2.flush();
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
     	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));
    	
    	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
   
	 // Test 3 All success with addAs (batchSize =8)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb3 =  dmManager.newWriteHostBatcher();
		ihb3.withBatchSize(8);
		ihb3.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        		 
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		 
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb3);
		
		ihb3.addAs("/doc/jackson", docMeta2, jsonNode).addAs("/doc/reader_xml",docMeta1, docStream1).addAs("/doc/string", stringTriple).addAs("/doc/file",  fileJson).addAs("/doc/is", docMeta2,inputStream)
		.addAs("/doc/bytes",  bytesJson).addAs("/doc/dom", docMeta1, docContent);
		
		
		ihb3.flush();
		System.out.println("Size is "+dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue());
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==7);
    	
    	DocumentMetadataHandle  mHandle2 = readMetadataFromDocument(dbClient, "/doc/reader_xml", "XML");
    	Assert.assertEquals(1,mHandle2.getQuality());
    	Assert.assertEquals("Sample Collection 1",mHandle2.getCollections().iterator().next());
     	Assert.assertTrue(mHandle2.getCollections().size()==1);
    	
    	DocumentMetadataHandle  mHandle3 = readMetadataFromDocument(dbClient, "/doc/jackson", "XML");
    	Assert.assertEquals(0,mHandle3.getQuality());
    	Assert.assertEquals("Sample Collection 2",mHandle3.getCollections().iterator().next());
     	Assert.assertTrue(mHandle3.getCollections().size()==1);

     	Assert.assertTrue(uriExists(successBatch.toString(),"/doc/os_json"));
    	Assert.assertFalse(uriExists(successBatch.toString(),"/doc/reader_wrongxml"));
         	
     	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
    	
		// Test 4 All failures in 2 batches
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb4 =  dmManager.newWriteHostBatcher();
		ihb4.withBatchSize(4);
		ihb4.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        		 
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		 
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb4);
		
		ihb4.addAs("/doc/jackson", docMeta2, jsonNode).addAs("/doc/reader_wrongxml",docMeta1, docStream).addAs("/doc/string",stringTriple).addAs("/doc/file",  fileJson);
		ihb4.flush();
		replenishStream();
		ihb4.addAs("/doc/is", docMeta2,inputStream).addAs("/doc/reader_wrongxml",docMeta1, docStream).addAs("/doc/bytes",  bytesJson).addAs("/doc/dom", docMeta1, docContent);
		ihb4.flush();
		
		
		Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/os_wrongjson"));
    	
    	
	}
	
	@Test
	public void testAddandAddAs() throws Exception{
	    final StringBuffer successBatch = new StringBuffer();
	    final StringBuffer failureBatch = new StringBuffer();
	    final String query1 = "fn:count(fn:doc())";

    	// Test 1 few failures with addAs and add(batchSize =1)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(1);
		ihb1.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb1);
		ihb1.add("/doc/jackson", jacksonHandle).addAs("/doc/reader_wrongxml", readerHandle).add("/doc/string", docMeta1, stringHandle).addAs("/doc/file", docMeta2, fileHandle).add("/doc/is", isHandle)
		.addAs("/doc/os_wrongjson", docMeta2, osHandle).add("/doc/bytes", docMeta1, bytesHandle).addAs("/doc/dom", domHandle);
		
		
		ihb1.flush();
		
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==6);
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/os_wrongjson"));
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));

    	DocumentMetadataHandle  mHandle = readMetadataFromDocument(dbClient, "/doc/string", "XML");
    	Assert.assertEquals(1,mHandle.getQuality());
    	Assert.assertEquals("Sample Collection 1",mHandle.getCollections().iterator().next());
     	Assert.assertTrue(mHandle.getCollections().size()==1);
    	
    	DocumentMetadataHandle  mHandle1 = readMetadataFromDocument(dbClient, "/doc/file", "XML");
    	Assert.assertEquals(0,mHandle1.getQuality());
    	Assert.assertEquals("Sample Collection 2",mHandle1.getCollections().iterator().next());
     	Assert.assertTrue(mHandle1.getCollections().size()==1);
     	
     	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
    	
    	
    	//ISSUE # 38
	    // Test 2 All failure with addAs and add(batchSize =8)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
	    replenishStream();
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(8);
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb2);
		ihb2.add("/doc/jackson", jacksonHandle).addAs("/doc/reader_wrongxml", readerHandle).add("/doc/string", docMeta1, stringHandle).addAs("/doc/file", docMeta2, fileHandle).add("/doc/is", isHandle)
		.addAs("/doc/os_wrongjson", docMeta2, osHandle).add("/doc/bytes", docMeta1, bytesHandle).addAs("/doc/dom", domHandle);
		
		
		ihb2.flush();
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
     	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));
    	
    	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
   
	 // Test 3 All success with addAs and add(batchSize =8)
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb3 =  dmManager.newWriteHostBatcher();
		ihb3.withBatchSize(8);
		ihb3.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        		 
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		 
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb3);
		
		ihb3.add("/doc/jackson", docMeta2, jacksonHandle).addAs("/doc/reader_xml",docMeta1, readerHandle1).add("/doc/string", stringHandle).addAs("/doc/file",  fileHandle).add("/doc/is", docMeta2,isHandle)
		.addAs("/doc/os_json",  osHandle1).add("/doc/bytes",  bytesHandle).addAs("/doc/dom", docMeta1, domHandle);
		
		
		ihb3.flush();
		System.out.println("Size is "+dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue());
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==8);
    	
    	DocumentMetadataHandle  mHandle2 = readMetadataFromDocument(dbClient, "/doc/reader_xml", "XML");
    	Assert.assertEquals(1,mHandle2.getQuality());
    	Assert.assertEquals("Sample Collection 1",mHandle2.getCollections().iterator().next());
     	Assert.assertTrue(mHandle2.getCollections().size()==1);
    	
    	DocumentMetadataHandle  mHandle3 = readMetadataFromDocument(dbClient, "/doc/jackson", "XML");
    	Assert.assertEquals(0,mHandle3.getQuality());
    	Assert.assertEquals("Sample Collection 2",mHandle3.getCollections().iterator().next());
     	Assert.assertTrue(mHandle3.getCollections().size()==1);

     	Assert.assertTrue(uriExists(successBatch.toString(),"/doc/os_json"));
    	Assert.assertFalse(uriExists(successBatch.toString(),"/doc/reader_wrongxml"));
         	
     	successBatch.delete(0,successBatch.length());
    	failureBatch.delete(0,failureBatch.length());
    	clearDB(port);
    	
		// Test 4 All failures in 2 batches
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	replenishStream();
		WriteHostBatcher ihb4 =  dmManager.newWriteHostBatcher();
		ihb4.withBatchSize(4);
		ihb4.onBatchSuccess(
		        (client, batch) -> {
		        	for(WriteEvent w: batch.getItems()){
		        		successBatch.append(w.getTargetUri()+":");
		        		 
		        	}
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  for(WriteEvent w: batch.getItems()){
		        		 
		        		  failureBatch.append(w.getTargetUri()+":");
			        	}

		           
		          });
		dmManager.startJob(ihb4);
		
		ihb4.add("/doc/jackson", docMeta2, jacksonHandle).addAs("/doc/reader_wrongxml",docMeta1, readerHandle).add("/doc/string", stringHandle).addAs("/doc/file",  fileHandle);
		ihb4.flush();
		
		ihb4.add("/doc/is", docMeta2,isHandle).addAs("/doc/os_wrongjson",  osHandle).add("/doc/bytes",  bytesHandle).addAs("/doc/dom", docMeta1, domHandle);
		ihb4.flush();
		
		
		Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/reader_wrongxml"));
    	Assert.assertTrue(uriExists(failureBatch.toString(),"/doc/os_wrongjson"));
    	
    	
	}
	
	private boolean uriExists(String s, String in){
		if (s.contains(in)) {
		    return true;
		}
		return false;
	}
	
	
	//Immutability of WriteHostBatcher- ISSUE # 26 ea 3
	@Ignore
	public void testHostBatcherImmutability() throws Exception{
		
		WriteHostBatcher ihb = dmManager.newWriteHostBatcher();
		ihb.withJobName(null);
		ihb.withBatchSize(2);
		dmManager.startJob(ihb);
	

		ihb.withJobName("Job 1");
		ihb.add("/local/triple", stringHandle);
		ihb.withBatchSize(1);
		ihb.flush();
		
	}
	
	//ISSUE # 38
	@Test
	public void testNumberofBatches() throws Exception{
	   
	    final MutableInt numberOfSuccessFulBatches = new MutableInt(0);
	    final MutableBoolean state = new MutableBoolean(true);
	    
		WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(5);
		ihb1.onBatchSuccess(
		        (client, batch) -> {
		        	numberOfSuccessFulBatches.add(1);
		        	        
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	state.isFalse();
		           
		          });
		dmManager.startJob(ihb1);
	
		for (int i =0 ;i < 100; i++){
			String uri ="/local/json-"+ i;
			ihb1.add(uri, jacksonHandle);
		}
	
		ihb1.flush();
		Assert.assertTrue(state.booleanValue());
		Assert.assertTrue(numberOfSuccessFulBatches.intValue()==100/5);
		
	}
	

	// ISSUE # 39, 40
	@Test
	public void testClientObject() throws Exception{
	   
	  
	    final StringBuffer successHost = new StringBuffer();
	    final StringBuffer successUser = new StringBuffer();
	    final StringBuffer successPassword = new StringBuffer();
	    final StringBuffer successPort = new StringBuffer();
	    final StringBuffer successForest = new StringBuffer();
	    
	    final StringBuffer failureHost = new StringBuffer();
	    final StringBuffer failureUser = new StringBuffer();
	    final StringBuffer failurePassword = new StringBuffer();
	    final StringBuffer failurePort = new StringBuffer();
	    final StringBuffer failureForest = new StringBuffer();
	  
	    	    
		WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(1);
		ihb1.onBatchSuccess(
		        (client, batch) -> {
		        	successHost.append(client.getHost()+":");  
		        	successUser.append(client.getUser()+":");  
		        	successPassword.append(client.getPassword()+":");  
		        	successPort.append(client.getPort()+":");  
		        	successForest.append(client.getForestName()+":");  
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	failureHost.append(client.getHost()+":");  
					failureUser.append(client.getUser()+":");  
					failurePassword.append(client.getPassword()+":");  
					failurePort.append(client.getPort()+":");  
					failureForest.append(client.getForestName()+":");  
		           
		          });
		dmManager.startJob(ihb1);
	
		for (int i =0 ;i < 10; i++){
			String uri ="/local/json-"+ i;
			ihb1.add(uri, stringHandle);
		}
		
		for (int i =0 ;i < 5; i++){
			ihb1.add("", stringHandle);
		}
		ihb1.flush();

		
		Assert.assertTrue(successForest.toString().contains("null"));
		System.out.println(successUser.toString());
		System.out.println(count(successUser.toString(),user));
		Assert.assertTrue(count(successUser.toString(),user)==10);
		Assert.assertTrue(count(successPassword.toString(),password)==10);
		Assert.assertTrue(count(successPort.toString(),String.valueOf(port))==10);
		Assert.assertTrue(count(successHost.toString(),String.valueOf(host))!=10);
				

		Assert.assertTrue(failureForest.toString().contains("null"));
		Assert.assertTrue(count(failureUser.toString(),user)==5);
		Assert.assertTrue(count(failurePassword.toString(),password)==5);
		Assert.assertTrue(count(failurePort.toString(),String.valueOf(port))==5);
		Assert.assertTrue(count(failureHost.toString(),String.valueOf(host))!=5);
	}
	
	
	
	@Ignore
	public void testBatchObject() throws Exception{
	   
	  
	    final StringBuffer successBatchNum = new StringBuffer();
	    final StringBuffer successBytesMoved = new StringBuffer();
	    final StringBuffer successForestName = new StringBuffer();
	    final StringBuffer successJobID = new StringBuffer();
	    final StringBuffer successTime = new StringBuffer();
	    
	    final StringBuffer failureBatchNum = new StringBuffer();
	    final StringBuffer failureBytesMoved = new StringBuffer();
	    final StringBuffer failureForestName = new StringBuffer();
	    final StringBuffer failureJobID = new StringBuffer();
	    final StringBuffer failureTime = new StringBuffer();
	    	    
		WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(1);
		ihb1.onBatchSuccess(
		        (client, batch) -> {
		        	System.out.println(batch.getBatchNumber());
		        	System.out.println(batch.getBytesMoved());
		        	System.out.println(batch.getForest()== null);
		        	System.out.println(batch.getJobTicket() == null);
		        	System.out.println(batch.getTimestamp()==null);
		        //	System.out.println(batch.getForest().getForestName());
		        	successBatchNum.append(batch.getBatchNumber());
		        	successBytesMoved.append(batch.getBytesMoved());
		        //	successForestName.append(batch.getForest().getForestName());
		        //	successJobID.append(batch.getJobTicket().getJobId());
		        //	successTime.append(batch.getTimestamp().getTime().getTime());
		         	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	failureBatchNum.append(batch.getBatchNumber());
  		        	failureBytesMoved.append(batch.getBytesMoved());
  		        	failureForestName.append(batch.getForest().getForestName());
  		        	failureJobID.append(batch.getJobTicket().getJobId());
  		        	failureTime.append(batch.getTimestamp().getTime().getTime());
		      	    
		          });
		dmManager.startJob(ihb1);
	
		for (int i =0 ;i < 10; i++){
			String uri ="/local/json-"+ i;
			ihb1.add(uri, stringHandle);
		}
		
		for (int i =0 ;i < 5; i++){
			ihb1.add("", stringHandle);
		}
		ihb1.flush();

		
		System.out.println(successBatchNum.toString());
		System.out.println(successBytesMoved.toString());
		System.out.println(successForestName.toString());
		System.out.println(successJobID.toString());
		System.out.println(successTime.toString());
		
		System.out.println(failureBatchNum.toString());
		System.out.println(failureBytesMoved.toString());
		System.out.println(failureForestName.toString());
		System.out.println(failureJobID.toString());
		System.out.println(failureTime.toString());
	}
	
	private int count(String s, String in){
		int i = 0;
		Pattern p = Pattern.compile(in);
		Matcher m = p.matcher( s );
		while (m.find()) {
		    i++;
		}
		return i;
	}
	
	//ISSUE # 28
	@Test
	public void testWithBatch() throws Exception{
		
		final String query1 = "fn:count(fn:doc())";
		WriteHostBatcher ihb1 = dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(-20);
		
		dmManager.startJob(ihb1);
		
		ihb1.add("/local/json", jacksonHandle);
		
		ihb1.flush();
		Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue() == 0);
		
		WriteHostBatcher ihb2 = dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(0);
		
		dmManager.startJob(ihb2);
		
		ihb2.add("/local/json", jacksonHandle);
		ihb2.flush();

		Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue() == 0);

		
	}
	
	
	@Test
	public void testInsertoReadOnlyForest() throws Exception{
		Map <String, String> properties = new HashMap();
		properties.put("updates-allowed", "read-only");
		for (int i =0 ; i < clusterInfo.size(); i++)
		 	changeProperty(properties,"/manage/v2/forests/"+dbName+"-"+(i+1)+"/properties");
		final String query1 = "fn:count(fn:doc())";
	 	
       	final MutableInt successCount = new MutableInt(0);
       	
       	final MutableBoolean failState = new MutableBoolean(false);
       	final MutableInt failCount = new MutableInt(0);
       	
		for (int i =0 ; i < clusterInfo.size(); i++)
		 	changeProperty(properties,"/manage/v2/forests/"+dbName+"-"+(i+1)+"/properties");
				
		     	
    
    	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(25);
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	
		        	successCount.add(batch.getItems().length);
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failState.setTrue();
		        	  failCount.add(batch.getItems().length);
		          });
		
		dmManager.startJob(ihb2);
		for (int j =0 ;j < 20; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, stringHandle);
		}
	
		ihb2.flush();
		
		properties.put("updates-allowed", "all");
		for (int i =0 ; i < clusterInfo.size(); i++)
		 	changeProperty(properties,"/manage/v2/forests/"+dbName+"-"+(i+1)+"/properties");
      
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
		
		Assert.assertTrue(failState.booleanValue());
		
		Assert.assertTrue(successCount.intValue() == 0);
		Assert.assertTrue(failCount.intValue() == 20);
	}
	
	@Test
	public void testDuplicates() throws Exception{
		Map <String, String> properties = new HashMap<String, String>();
		properties.put("updates-allowed", "read-only");
		
		final String query1 = "fn:count(fn:doc())";
	 	
       	final MutableBoolean successState = new MutableBoolean(false);
       	final MutableBoolean failState = new MutableBoolean(false);
       	
       	final MutableInt successCount = new MutableInt(0);
       	final MutableInt failureCount = new MutableInt(0);
       	
       	
		WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
		ihb1.withBatchSize(5);
	
		dmManager.startJob(ihb1);
		
		for (int i =0 ;i < 20; i++){
			String uri ="/local/json-"+ i;
			ihb1.add(uri, stringHandle);
		}
	
		ihb1.flush();
		
	 	Number response = dbClient.newServerEval().xquery(query1).eval().next().getNumber();
    	Assert.assertTrue(response.intValue()==20);
    	
		for (int i =0 ; i < clusterInfo.size() -1; i++)
		 	changeProperty(properties,"/manage/v2/forests/"+dbName+"-"+(i+1)+"/properties");
       	

    	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		dmManager.startJob(ihb2);
		ihb2.withBatchSize(1);
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	successCount.add(batch.getItems().length);
		        	successState.setTrue();
		        	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failureCount.add(batch.getItems().length);
		        	  failState.setTrue();
		        	  
		          });
		
		for (int j =0 ;j < 21; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, stringHandle);
		}

		ihb2.flush();
		
		properties.put("updates-allowed", "all");
		for (int i =0 ; i < clusterInfo.size(); i++)
		 	changeProperty(properties,"/manage/v2/forests/"+dbName+"-"+(i+1)+"/properties");
    	
    	System.out.println("Success count: "+successCount);
      	System.out.println("Failure count: "+failureCount);
      	
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==21);
	
    	Assert.assertTrue(successState.booleanValue());
		Assert.assertTrue(failState.booleanValue());
	}
	
	@Test
	public void testInsertoDisabledDB() throws Exception{
		Map <String, String> properties = new HashMap();
		properties.put("enabled", "false");
		final String query1 = "fn:count(fn:doc())";
	 	
       	final MutableInt successCount = new MutableInt(0);
       	
       	final MutableBoolean failState = new MutableBoolean(false);
       	final MutableInt failCount = new MutableInt(0);

       	
           	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(30);
		dmManager.startJob(ihb2);
		
		for (int j =0 ;j < 20; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, stringHandle);
		}
	
		
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	
		        	successCount.add(batch.getItems().length);
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failState.setTrue();
		        	  failCount.add(batch.getItems().length);
		          });

		changeProperty(properties,"/manage/v2/databases/"+dbName+"/properties");
		
		ihb2.flush();
		
		properties.put("enabled", "true");
		changeProperty(properties,"/manage/v2/databases/"+dbName+"/properties");
		
    	System.out.println("Fail : "+failCount.intValue());
    	System.out.println("Success : "+successCount.intValue());
    	System.out.println("Count : "+ dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue());
    	
    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
		Assert.assertTrue(failState.booleanValue());
		
		Assert.assertTrue(failCount.intValue() == 20);
		
	}
	
	
	@Test
	public void testServerXQueryTransformSuccess() throws Exception
    {      
		   final String query1 = "fn:count(fn:doc())";             
		   final MutableInt successCount = new MutableInt(0);
	       	
	       final MutableBoolean failState = new MutableBoolean(false);
	       final MutableInt failCount = new MutableInt(0);
           TransformExtensionsManager transMgr = 
                        dbClient.newServerConfigManager().newTransformExtensionsManager();
           ExtensionMetadata metadata = new ExtensionMetadata();
           metadata.setTitle("Adding attribute xquery Transform");
           metadata.setDescription("This plugin transforms an XML document by adding attribute to root node");
           metadata.setProvider("MarkLogic");
           metadata.setVersion("0.1");
           // get the transform file from add-attr-xquery-transform.xqy
           File transformFile = FileUtils.toFile(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX+"add-attr-xquery-transform.xqy"));
           FileHandle transformHandle = new FileHandle(transformFile);
           transMgr.writeXQueryTransform("add-attr-xquery-transform", transformHandle, metadata);
           
           ServerTransform transform = new ServerTransform("add-attr-xquery-transform");
           transform.put("name", "Lang");
           transform.put("value", "English");
           
           String xmlStr1 = "<?xml  version=\"1.0\" encoding=\"UTF-8\"?><foo>This is so foo</foo>";
           String xmlStr2 = "<?xml  version=\"1.0\" encoding=\"UTF-8\"?><foo>This is so bar</foo>";
                  
           //Use WriteHostbatcher to write the same files.                      
           WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
	   	   ihb1.withBatchSize(5);
	   	   ihb1.withTransform(transform);
	   	   ihb1.onBatchSuccess(
	   			   (client, batch) -> {
		        	
	   				   successCount.add(batch.getItems().length);
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failState.setTrue();
		        	  failCount.add(batch.getItems().length);
		          });
	   	   dmManager.startJob(ihb1);
           StringHandle handleFoo = new StringHandle();
           handleFoo.set(xmlStr1);
           
           StringHandle handleBar = new StringHandle();
           handleBar.set(xmlStr2);
           
           String uri1 = null;
           String uri2 = null;
         
           for (int i = 0; i < 4; i++) {
                  uri1 = "foo" + i + ".xml";
                  uri2 = "bar" + i + ".xml";
                  ihb1.add(uri1, handleFoo).add(uri2, handleBar);;
           }
           // Flush
           ihb1.flush();
           Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==8);
   		   Assert.assertFalse(failState.booleanValue());
   		   Assert.assertTrue(successCount.intValue()==8);
    }
	
	@Test
	public void testServerXQueryTransformFailure() throws Exception
    {      
		   final String query1 = "fn:count(fn:doc())";             
		   final MutableInt successCount = new MutableInt(0);
	       	
	       final MutableBoolean failState = new MutableBoolean(false);
	       final MutableInt failCount = new MutableInt(0);
           TransformExtensionsManager transMgr = 
                        dbClient.newServerConfigManager().newTransformExtensionsManager();
           ExtensionMetadata metadata = new ExtensionMetadata();
           metadata.setTitle("Adding attribute xquery Transform");
           metadata.setDescription("This plugin transforms an XML document by adding attribute to root node");
           metadata.setProvider("MarkLogic");
           metadata.setVersion("0.1");
           // get the transform file from add-attr-xquery-transform.xqy
           File transformFile = FileUtils.toFile(WriteHostBatcherTest.class.getResource(TEST_DIR_PREFIX+"add-attr-xquery-transform.xqy"));
           FileHandle transformHandle = new FileHandle(transformFile);
           transMgr.writeXQueryTransform("add-attr-xquery-transform", transformHandle, metadata);
           
           ServerTransform transform = new ServerTransform("add-attr-xquery-transform");
           transform.put("name", "Lang");
           transform.put("value", "English");
           
           String xmlStr1 = "<?xml  version=\"1.0\" encoding=\"UTF-8\"?><foo>This is so foo</foo>";
           String xmlStr2 = "<?xml  version=\"1.0\" encoding=\"UTF-8\"?><foo>This is so bar</foo";
                  
           //Use WriteHostbatcher to write the same files.                      
           WriteHostBatcher ihb1 =  dmManager.newWriteHostBatcher();
	   	   ihb1.withBatchSize(1);
	   	   ihb1.withTransform(transform);
	   	   ihb1.onBatchSuccess(
	   			   (client, batch) -> {
		        	
	   				   successCount.add(batch.getItems().length);
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failState.setTrue();
		        	  failCount.add(batch.getItems().length);
		          });
	   	   dmManager.startJob(ihb1);
           StringHandle handleFoo = new StringHandle();
           handleFoo.set(xmlStr1);
           
           StringHandle handleBar = new StringHandle();
           handleBar.set(xmlStr2);
           
           String uri1 = null;
           String uri2 = null;
           
           for (int i = 0; i < 4; i++) {
                  uri1 = "foo" + i + ".xml";
                  uri2 = "bar" + i + ".xml";
                  ihb1.add(uri1, handleFoo).add(uri2, handleBar);;
           }
           // Flush
           ihb1.flush();
           Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==4);
   		   Assert.assertTrue(failState.booleanValue());
   		   Assert.assertTrue(successCount.intValue()==4);
   		   Assert.assertTrue(failCount.intValue()==4);
    }
	
	@Test
	public void testAddMultiThreadedSuccess() throws Exception{
		
		final String query1 = "fn:count(fn:doc())";
		ihbMT =  dmManager.newWriteHostBatcher();
       	ihbMT.withBatchSize(100);
       	ihbMT.onBatchSuccess(
		        (client, batch) -> {
		        	System.out.println("Batch size "+batch.getItems().length);
		        	for(WriteEvent w:batch.getItems()){
		        		System.out.println("Success "+w.getTargetUri());
		        	}
		        		
		        	
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  throwable.printStackTrace();
		        		for(WriteEvent w:batch.getItems()){
		        			System.out.println("Failure "+w.getTargetUri());
		        		}
			        		
		       
		});
		dmManager.startJob(ihbMT);

       	class MyRunnable implements Runnable {
       	  
       	  @Override
       	  public void run() {
         		System.out.println("Now executing thread "+Thread.currentThread().getId());
           		for (int j =0 ;j < 100; j++){
    				String uri ="/local/json-"+ j+"-"+Thread.currentThread().getId();
    				System.out.println("URI is "+uri);
    				ihbMT.add(uri, fileHandle);
    				
    				
    			}
           		ihbMT.flush();
       	  }  
           		
       	} 
       	Thread t1,t2,t3;
       	t1 = new Thread(new MyRunnable());
       	t2 = new Thread(new MyRunnable());
       	t3 = new Thread(new MyRunnable());
       	t1.start();
       	t2.start();
       	t3.start();
       	
       	t1.join();
       	t2.join();
       	t3.join();
       	    	
		Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==300);
	}
	
	@Test
	public void testAddMultiThreadedFailureEventCount() throws Exception{
		
		final MutableInt eventCount = new MutableInt(0);
		ihbMT =  dmManager.newWriteHostBatcher();
       	ihbMT.withBatchSize(120);
       	ihbMT.onBatchSuccess(
		        (client, batch) -> {
		        	synchronized(eventCount){
		        		 eventCount.add(batch.getItems().length);
		        	}
		       		        		
		        	
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  	synchronized(eventCount){
			        		 eventCount.add(batch.getItems().length);
			        	}
		      
		});
		dmManager.startJob(ihbMT);

       	class MyRunnable implements Runnable {
       	  
       	  @Override
       	  public void run() {
         		
           		for (int j =0 ;j < 100; j++){
    				String uri ="/local/json-"+ j;
    				ihbMT.add(uri, fileHandle);
    			}
           		ihbMT.flush();
       	  }  
           		
       	} 
       	Thread t1,t2,t3;
       	t1 = new Thread(new MyRunnable());
       	t2 = new Thread(new MyRunnable());
       	t3 = new Thread(new MyRunnable());
       	t1.start();
      
       	t2.start();
   
       	t3.start();
       	
       	t1.join();
       	t2.join();
       	t3.join();
       	System.out.println(eventCount.intValue());
       	Assert.assertTrue(eventCount.intValue()==300);
		
	}
	
	@Test
	public void testAddMultiThreadedMultiWriteHostBatchers() throws Exception{
		
		final String query1 = "fn:count(fn:doc())";
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		final MutableInt successCount = new MutableInt();
		final MutableInt failureCount = new MutableInt();
		
 		ihb2.withBatchSize(50);
	    ihb2.onBatchSuccess(
		   (client, batch) -> {
			   synchronized(successCount){
				   successCount.add(batch.getItems().length);   
			   }
			 	for(WriteEvent e: batch.getItems()){
		        		System.out.println("Success : "+e.getTargetUri());
		        	}
		        	
			   
        	
        	}
        )
        .onBatchFailure(
          (client, batch, throwable) -> {
        	   synchronized(failureCount){
        		   failureCount.add(batch.getItems().length);   
			   }
        	 	for(WriteEvent e: batch.getItems()){
		        		System.out.println("Failure : "+e.getTargetUri());
		        	}
		        	
          });
   

 		

       	class MyRunnable implements Runnable {
       	  
       	  @Override
       	  public void run() {
       		  DataMovementManager dm = DataMovementManager.newInstance();
       		  DatabaseClient dbc = DatabaseClientFactory.newClient(host, port, user, password, Authentication.DIGEST);
       		  dm.setClient(dbc);
       		  WriteHostBatcher ihb1 =  dm.newWriteHostBatcher();
       		  ihb1.withBatchSize(100);
       		  ihb1.onBatchSuccess(
  	   			   (client, batch) -> {
  		        	for(WriteEvent e: batch.getItems()){
  		        		System.out.println("Success : "+e.getTargetUri());
  		        	}
  		        		
  	   			   synchronized(successCount){
  					   successCount.add(batch.getItems().length);   
  				   }
  	        	
  		        	
  		        	}
  		        )
  		        .onBatchFailure(
  		          (client, batch, throwable) -> {
  		        	   synchronized(failureCount){
  		        		 failureCount.add(batch.getItems().length);   
  					   }
  		        	 	for(WriteEvent e: batch.getItems()){
  	  		        		System.out.println("Failure : "+e.getTargetUri());
  	  		        	}
  	  		        	
  		        	
  		          });
  	   	   
         		
           		for (int j =0 ;j < 1000; j++){
           			String uri ="/local/json-"+ j;
    				ihb1.add(uri, fileHandle);
    				
    				
    			}
           		ihb1.flush();
       	  }  
           		
       	} 
       	Thread t1;
       	t1 = new Thread(new MyRunnable());
       	t1.start();
    	
 		for (int j =0 ;j < 1000; j++){
 			String uri ="/local/gson-"+ j;
			ihb2.add(uri, stringHandle);
			
			
		}
       	ihb2.flush();
       	t1.join();

		Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==2000);
		Assert.assertTrue(successCount.intValue()==2000);
		Assert.assertTrue(failureCount.intValue()==0);
	}
	
	
	//ISSUE # 58
	@Ignore
	public void testTransactionSize() throws Exception{
		try{
			final String query1 = "fn:count(fn:doc())";
		 	
	       	final MutableInt successCount = new MutableInt(0);
	       	
	       	final MutableBoolean failState = new MutableBoolean(false);
	       	final MutableInt failCount = new MutableInt(0);
	
	       	
	           	
			WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
			ihb2.withBatchSize(3000);
			ihb2.withTransactionSize(2);
			dmManager.startJob(ihb2);
			
				
			ihb2.onBatchSuccess(
			        (client, batch) -> {
			        	
			        	successCount.add(batch.getItems().length);
			        	 System.out.println("Success Batch size "+batch.getItems().length);
				        	for(WriteEvent w:batch.getItems()){
				        		System.out.println("Success "+w.getTargetUri());
				        	}
			        	
			        	}
			        )
			        .onBatchFailure(
			          (client, batch, throwable) -> {
			        	  throwable.printStackTrace();
			        	  System.out.println("Failure Batch size "+batch.getItems().length);
				        	for(WriteEvent w:batch.getItems()){
				        		System.out.println("Failure "+w.getTargetUri());
				        	}
			        	  failState.setTrue();
			        	  failCount.add(batch.getItems().length);
			          });
			for (int j =0 ;j < 500; j++){
				String uri ="/local/ABC-"+ j;
				ihb2.add(uri, stringHandle);
			}
		
			
		    ihb2.flush();
		    
	    	System.out.println("Fail : "+failCount.intValue());
	    	System.out.println("Success : "+successCount.intValue());
	    	System.out.println("Count : "+ dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue());
	    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==500);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Test
	public void testNPECallBack() throws Exception{
		
		final String query1 = "fn:count(fn:doc())";
	 	
       	final MutableInt successCount = new MutableInt(0);
       	
       	final MutableBoolean failState = new MutableBoolean(false);
       	final MutableInt failCount = new MutableInt(0);

       	
           	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(5);
		dmManager.startJob(ihb2);
		
	
		
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	String s= null;
		        	s.length();
		        	System.out.println("Success host : "+client.getHost());
		        	System.out.println(batch.getItems().length);
		        	successCount.add(batch.getItems().length);
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failState.setTrue();
		        	  failCount.add(batch.getItems().length);
		          });


		for (int j =0 ;j < 30; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, stringHandle);
		}
							
    	System.out.println("Fail : "+failCount.intValue());
    	System.out.println("Success : "+successCount.intValue());
    	System.out.println("Count : "+ dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue());
    	
	}

	@Test
	public void testInserttoDisabledAppServer() throws Exception{
		
		final String query1 = "fn:count(fn:doc())";
	 	Map<String,String> properties = new HashMap<String,String>();
     
      	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(3000);
		
		
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		         	System.out.println("Success Batch size "+batch.getItems().length);
		        	for(WriteEvent w:batch.getItems()){
		        		System.out.println("Success "+w.getTargetUri());
		        	}
		        
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  	throwable.printStackTrace();
		        	 	System.out.println("Failure Batch size "+batch.getItems().length);
			        	for(WriteEvent w:batch.getItems()){
			        		System.out.println("Failure "+w.getTargetUri());
			        	}
		          });
		
		dmManager.startJob(ihb2);
		for (int j =0 ;j < 200; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, stringHandle);
		}

		properties.put("server-name",dbName+"-Server");
		properties.put("group-name", "Default");
		properties.put("enabled", "false");
		changeProperty(properties,"/manage/v2/servers/"+dbName+"-Server/properties");
		Thread.currentThread().sleep(1000L);
		ihb2.flush();
		
		properties.put("enabled", "true");
		changeProperty(properties,"/manage/v2/servers/"+dbName+"-Server/properties");
		

    	Assert.assertTrue(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue()==0);
    	
	}
	
	@Ignore
	public void testDisableAppServerDuringInsert() throws Exception{
		
		Thread t1 = new Thread(new StopServerRunnable());
     	t1.setName("Status Check");
     	  	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(5);
		
		
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		         	System.out.println("Success Batch size "+batch.getItems().length);
		        	for(WriteEvent w:batch.getItems()){
		        		System.out.println("Success "+w.getTargetUri());
		        	}
		        
		        	
		        	}
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  	throwable.printStackTrace();
		        	 	System.out.println("Failure Batch size "+batch.getItems().length);
			        	for(WriteEvent w:batch.getItems()){
			        		System.out.println("Failure "+w.getTargetUri());
			        	}
		          });
		
		dmManager.startJob(ihb2);
		t1.start();
		
		for (int j =0 ;j < 2000; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, fileHandle);
		}
				
		ihb2.flush();
		t1.join();
    	
    	
	}
	class StopServerRunnable implements Runnable {
	  final String query1 = "fn:count(fn:doc())";
	  Map<String,String> properties = new HashMap<String,String>();
	
   	  @Override
   	  public void run() {
   		  properties.put("server-name",dbName+"-Server");
		  properties.put("group-name", "Default");
		  properties.put("enabled", "false");
   		  boolean state = true;
   		  while (state){
   			 int count =dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue();
   			 System.out.println("Count is "+count);
   			 if(count >= 100){
   				
     			changeProperty(properties,"/manage/v2/servers/"+dbName+"-Server/properties");
     			state=false;
   			 }
   				
   		  }
   	  }  
       		
  } 
	
	@Test
	public void testDisableDBDuringInsert() throws Exception{
		
	    Thread t1 = new Thread(new DisabledDBRunnable());
		MutableBoolean failCheck = new MutableBoolean(false);
		MutableInt successCount = new MutableInt(0);
		MutableInt failureCount = new MutableInt(0);
		
     	t1.setName("Status Check");
     	Map<String,String> properties = new HashMap<String,String>();  	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(5);
				
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	successCount.add(batch.getItems().length);
		          	
		        	
		        	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failCheck.setTrue();
		        	  failureCount.add(batch.getItems().length);
		        	  throwable.printStackTrace();
		        	        
			        	
			        	

		           
		          });
		dmManager.startJob(ihb2);
		t1.start();
		
		for (int j =0 ;j < 1000; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, fileHandle);
		}
				
		ihb2.flush();
		t1.join();
		properties.put("enabled", "true");
		changeProperty(properties,"/manage/v2/databases/"+dbName+"/properties");
		Assert.assertTrue(failCheck.booleanValue());
		Assert.assertTrue(successCount.intValue() >= 100);
		Assert.assertTrue(successCount.intValue() < 1000);
		Assert.assertTrue(failureCount.intValue() <= 900);
		
    	
	}
	class DisabledDBRunnable implements Runnable {
	  final String query1 = "fn:count(fn:doc())";
	  Map<String,String> properties = new HashMap<String,String>();
	
   	  @Override
   	  public void run() {
   	
		  properties.put("enabled", "false");

			
   		  boolean state = true;
   		  while (state){
   			 int count =dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue();
   			 System.out.println("Count is "+count);
   			 if(count >= 100){
   				changeProperty(properties,"/manage/v2/databases/"+dbName+"/properties");
     			
     			state=false;
   			 }
   				
   		  }
   	  }  
       		
  } 
	

	@Ignore
	public void testOfflineForestStopServerDuringInsert() throws Exception{
		
		Thread t1 = new Thread(new OffLineForestStopServerRunnable());
		MutableBoolean failCheck = new MutableBoolean(false);
		MutableInt successCount = new MutableInt(0);
		MutableInt failureCount = new MutableInt(0);
		
     	t1.setName("Status Check");
     	Map<String,String> properties = new HashMap<String,String>(); 
     	
     	properties.put("forest-name","WriteHostBatcher-1");
     	properties.put("availability","offline");
     	changeProperty(properties,"/manage/v2/forests/WriteHostBatcher-1/properties");
     	properties.clear();
     	
		WriteHostBatcher ihb2 =  dmManager.newWriteHostBatcher();
		ihb2.withBatchSize(5);
				
		ihb2.onBatchSuccess(
		        (client, batch) -> {
		        	successCount.add(batch.getItems().length);
		        	
		        	System.out.println("Success host: "+client.getHost());
		        	System.out.println("Success Batch size "+batch.getItems().length);
		        	for(WriteEvent w:batch.getItems()){
		        		System.out.println("Success "+w.getTargetUri());
		        	}
		        
		        	
		        	
		        	
		          }
		        )
		        .onBatchFailure(
		          (client, batch, throwable) -> {
		        	  failCheck.setTrue();
		        	  failureCount.add(batch.getItems().length);
		        	  throwable.printStackTrace();
		        	  System.out.println("Failure host: "+client.getHost());
		        	  System.out.println("Failure Batch size "+batch.getItems().length);
			        	for(WriteEvent w:batch.getItems()){
			        		System.out.println("Failure "+w.getTargetUri());
			        	}
			        
			        	
			        	

		           
		          });
		dmManager.startJob(ihb2);
		t1.start();
		
		for (int j =0 ;j < 10000; j++){
			String uri ="/local/json-"+ j;
			ihb2.add(uri, fileHandle);
		}
				
		ihb2.flush();
		t1.join();
		
				
     	properties.put("forest-name","WriteHostBatcher-1");
     	properties.put("availability","online");
     	changeProperty(properties,"/manage/v2/forests/WriteHostBatcher-1/properties");
     	
		Assert.assertTrue(failCheck.booleanValue());
		Assert.assertTrue(successCount.intValue() >= 100);
		Assert.assertTrue(failureCount.intValue() <= 900);
    	
	}
	
	class OffLineForestStopServerRunnable implements Runnable {
	  final String query1 = "fn:count(fn:doc())";
	  Map<String,String> properties = new HashMap<String,String>();
	
   	  @Override
   	  public void run() {
  		properties.put("host-name", hostNames[0]);
  		properties.put("group", "default");
  		properties.put("state", "shutdown");

			
   		  boolean state = true;
   		  while (state){
   			 int count =dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue();
   			 System.out.println("Count is "+count);
   			 if(count >= 100){
   				changeProperty(properties,"/manage/v2/hosts/"+hostNames[0]+"/properties");
     			
     			state=false;
   			 }
   				
   		  }
   		try {
			Thread.currentThread().sleep(35000L);
			properties.clear();
			properties.put("host-name", hostNames[0]);
			properties.put("group", "default");
			properties.put("state", "restart");
			changeProperty(properties,"/manage/v2/hosts/"+hostNames[0]+"/properties");
			
			Thread.currentThread().sleep(5000L);
	     	System.out.println(dbClient.newServerEval().xquery(query1).eval().next().getNumber().intValue());
	     	
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   	  }  
       		
  } 
	
}
