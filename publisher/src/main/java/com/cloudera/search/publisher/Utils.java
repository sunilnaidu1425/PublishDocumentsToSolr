package com.cloudera.search.publisher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class Utils {
	public static String fetchContent(File file){
		Reader reader = null;
		try{
			reader = new BufferedReader(new FileReader(file));
			StringBuilder fileContent = new StringBuilder((int)file.length()+1024);
			char[] buffer = new char[4096];
			for (;;) {
				int len = 0;
				try {
					len = reader.read(buffer, 0, 4096);
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (len < 0)
					break;
				fileContent.append(buffer, 0, len);
			}
			return fileContent.toString();
		}catch(IOException ie){
			ie.printStackTrace();
		}finally{
			if(reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return "";
	}
	
	public static String cleanseEmailId(String emailId){
		if(emailId != null){
			if(emailId.startsWith("<") || emailId.startsWith("(")){
				emailId = emailId.substring(1);
			}
			if(emailId.endsWith(">") || emailId.endsWith(")")){
				emailId = emailId.substring(0, emailId.length()-1);
			}	
			return emailId.trim();
		}		
		return emailId;
	}
	
	public static String cleanseName(String name){
		if(name != null){
			if(name.startsWith("<") || name.startsWith("(") || name.startsWith("\"")){
				name = name.substring(1);
			}
			if(name.endsWith(">") || name.endsWith(")") || name.endsWith("\"")){
				name = name.substring(0, name.length()-1);
			}	
			name = name.trim();
		}		
		return name;
	}
	
	public static String findOrgName(String emailId){
		return (emailId != null ?emailId.substring(emailId.indexOf("@")+1).trim() : null);
	}
	
	public static String cleanseSubject(String subject){
		return subject.replaceAll("Re:", "").trim();
	}
}
