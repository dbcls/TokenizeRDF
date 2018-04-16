/*
 * This code is derived from arq.examples.riot.ExRIOT_6, which is 
 * distributed under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Database Center for Life Science (DBCLS) has developed this code
 * and releases it under MIT style license. 
 */

/* Yet to be done to handle those literals which contain CR */

package jp.ac.rois.dbcls;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotNotFoundException;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.graph.Triple;

public class TokenizeRDF {

	static { LogCtl.setCmdLogging(); }

	private static void issuer(String filename) {
		File f = new File(filename);
		if(f.isDirectory()){
			File[] fileList = f.listFiles();
			for (File ef: fileList){
				if(ef.getName().startsWith("."))
					continue;
				issuer(ef.getPath());
			}
		}

		System.err.println(filename);
		final int buffersize = 100000;
		final int pollTimeout = 300; // Poll timeout in milliseconds
		final int maxPolls = 1000;    // Max poll attempts

		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>(buffersize, true, pollTimeout, maxPolls);
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Runnable parser = new Runnable() {

			@Override
			public void run() {
				RDFParser parser_object = RDFParserBuilder
				.create()
				.errorHandler(ErrorHandlerFactory.errorHandlerDetailed())
				.source(filename)
				.checking(true)
				.build();
				try{
					parser_object.parse(inputStream);
				}
				catch (RiotParseException e){
					String location = "";
					if(e.getLine() >= 0 && e.getCol() >= 0)
						location = " at the line: " + e.getLine() + " and the column: " + e.getCol();
					System.err.println("Parse error"
							+ location
							+ " in \""
							+ filename
							+ "\", and cannot parse this file anymore. Reason: "
							+ e.getMessage());
					inputStream.finish();
				}
				catch (RiotNotFoundException e){
					System.err.println("Format error for the file \"" + filename + "\": " + e.getMessage());
					inputStream.finish();
				}
			}
		};

		executor.submit(parser);

		while (iter.hasNext()) {
			Triple next = iter.next();
			int sbj_type = 0;
			int obj_type = 0;
			String sbj_lexform = "";
			if(next.getSubject().isBlank()){
				sbj_type = 0;
				sbj_lexform = next.getSubject().getBlankNodeLabel();
			} else if(next.getSubject().isURI()){
				sbj_type = 1;
				sbj_lexform = next.getSubject().getURI();
			}
			String obj_lexform = "";
			if(next.getObject().isBlank()){
				obj_type = 0;
				obj_lexform = next.getObject().getBlankNodeLabel();
			}else if(next.getObject().isURI()){
				obj_type = 1;
				obj_lexform = next.getObject().getURI();
			}else if(next.getObject().isLiteral()){
				obj_type = 2;
				obj_lexform = next.getObject().getLiteralLexicalForm();
				if(next.getObject().getLiteralLanguage().length() > 0){
					obj_type = 3;
				}
				obj_lexform = obj_lexform.replace('\n', ' ');
			}
			System.out.println("0" + "\t" + sbj_type + "\t" + sbj_lexform);
			System.out.println("1" + "\t" + "1" + "\t" + next.getPredicate().getURI());
			System.out.println("2" + "\t" + obj_type + "\t" + obj_lexform);
		}

		executor.shutdown();
	}

	public static void main(String[] args) {

		if(args.length == 0){
			System.out.println("Please specify the filename to be converted.");
			return;
		} else {
			for (String arg: args) {
				File file = new File(arg);
				if(!file.exists() || !file.canRead()){
					System.out.println("Can't read " + file);
					return;
				}
				issuer(arg);
			}
		}
	}

}