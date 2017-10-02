/*
 * This code is derived from arq.examples.riot.ExRIOT_6, which is 
 * distributed under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Database Center for Life Science (DBCLS) has developed this code
 * and releases it under MIT style license. 
 */

package jp.ac.rois.dbcls;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotNotFoundException;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.graph.Triple;

public class TokenizeRDF {

	public static void main(String[] args) {

		final Map<String, Lang> optmap = new HashMap<String, Lang>() {
			private static final long serialVersionUID = 1L;
			{put("turtle", RDFLanguages.TURTLE);}
            {put("rdfxml", RDFLanguages.RDFXML);}
        };

		final int buffersize = 100000;
		String informat = "rdfxml";
		int idx = 0;
		if(args.length == 0){
			System.out.println("Please specify the filename to be converted.");
			return;
		} else {
			if(args[idx].startsWith("-i:")){
				informat = args[idx].substring(3);
				idx++;
			}
			File file = new File(args[idx]);
			if(!file.exists() || !file.canRead()){
				System.out.println("Can't read " + file);
				return;
			}
		}
		if(!optmap.containsKey(informat)){
			System.out.println("Input format is either turtle or rdfxml.");
			return;
		}
		final String filename = args[idx];
		//final Lang inputformat = optmap.get(informat);

		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>(buffersize);
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Runnable parser = new Runnable() {

			@Override
			public void run() {
				try{
					RDFParser.source(filename).parse(inputStream);
					//RDFDataMgr.parse(inputStream, filename, "file:///", inputformat, null);
				}
				catch (RiotNotFoundException e){
					System.err.println("File format error.");
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
			}
			System.out.println("0" + "\t" + sbj_type + "\t" + sbj_lexform);
			System.out.println("1" + "\t" + "1" + "\t" + next.getPredicate().getURI());
			System.out.println("2" + "\t" + obj_type + "\t" + obj_lexform);
		}

		executor.shutdown();
	}

}