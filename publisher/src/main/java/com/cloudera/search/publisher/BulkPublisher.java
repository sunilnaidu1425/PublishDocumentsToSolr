package com.cloudera.search.publisher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class BulkPublisher {

	static final int batchSize = 1000;
	static final String solrUrl = "http://localhost:8983/solr/news";
	private static final String Email_Pattern = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
			+ "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})";
	static Pattern FromPattern = Pattern.compile("From: ([^ @\n]+@[^ \n]+) (.+)\n");
	static Pattern FromPattern2 = Pattern.compile("From: ([^@<\n]+) (<[^<>\n]+>)");
	static Pattern FromPattern3 = Pattern.compile("From: ([^ @\n]+@[^ \n]+)\\s*\n");
	static Pattern subjectPattern = Pattern.compile("Subject: (.+)\n");
	static HttpSolrServer server = new HttpSolrServer(solrUrl);
	final static Logger logger = Logger.getLogger(BulkPublisher.class);
	public static void main(String args[]) {
		if (args.length != 1) {
			logger.info("Usage: BulkPublusher {inputDir}");
			System.exit(1);
		}
		String inputDir = args[0];
		try {
			processTheData(inputDir);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void processTheData(String inputDir) throws SolrServerException, IOException {
		File directory = new File(inputDir);
		File[] files = directory.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				processSubDirectories(file);
			} else if (file.isFile()) {
				server.add(processAFile(file, FilenameUtils.getName(inputDir)));
			}
		}
	}

	private static void processSubDirectories(File subDir) throws SolrServerException, IOException {
		File[] files = subDir.listFiles();
		logger.info("Processing directory:" + subDir.getName() + "\n");
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		int count = 0;
		for (File file : files) {
			// read each file, extract metadata, create Solr document, index
			docs.add(processAFile(file, FilenameUtils.getName(subDir.getName())));
			count++;
			if(count > batchSize){
				server.add(docs);
				server.commit();
				logger.info("Batch indexed");
				docs = new ArrayList<SolrInputDocument>();
			}
		}
		server.add(docs);
		server.commit();
	}

	private static SolrInputDocument processAFile(File file, String parentDir) {
		String fileContent = Utils.fetchContent(file);
		Matcher m = FromPattern.matcher(fileContent);
		String emailId = null;
		String name = null;
		String subject = null;
		String content = null;
		String orgName = null;
		if (m.find()) {
			emailId = m.group(1);
			name = m.group(2);
			// System.out.println(emailId+"-->"+name);
		} else {
			Matcher m2 = FromPattern2.matcher(fileContent);
			if (m2.find()) {
				name = m2.group(1);
				emailId = m2.group(2);
			} else {
				Matcher m3 = FromPattern3.matcher(fileContent);
				if (m3.find()) {
					emailId = m3.group(1);
				} else {
					logger.info("-------------------------" + file.getName());
				}
			}
		}
		emailId = Utils.cleanseEmailId(emailId);
		name = Utils.cleanseName(name);
		orgName = Utils.findOrgName(emailId);
		// System.out.println(emailId + "-->" + name);

		Matcher subjectM = subjectPattern.matcher(fileContent);
		if (subjectM.find()) {
			subject = subjectM.group(1);
			subject = Utils.cleanseSubject(subject);
			content = fileContent.substring(subjectM.end());
		} else {
			content = fileContent;
		}
		SolrInputDocument doc = new SolrInputDocument();
		String[] categories = parentDir.split("\\.");

		for(int i = 0; i < categories.length; i++){
			doc.addField("category_level"+i, categories[i]);
			doc.addField("category", categories[i]);
		}
		/*doc.addField("category", "0/" + category + "/");
		for (int i = 1; i < categories.length; i++) {

			String thisCategory = i + "/" + category + "/" + categories[i] + "/";
			category += "/" + categories[i];
			doc.addField("category", thisCategory);
		}*/
		String id = parentDir + "-" + file.getName();
		doc.addField("id", id);
		doc.addField("organization", orgName);
		doc.addField("senderEmail", emailId);
		doc.addField("sender", name);
		doc.addField("subject", subject);
		doc.addField("content", content);
		
		return doc;
	}
}
