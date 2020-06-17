package org.biofid.pipeline.runner;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.biofid.gazetteer.util.UnicodeRegexSegmenter;
import org.biofid.pipeline.runner.util.AnnotationRemover;
import org.dkpro.core.io.xmi.XmiReader;
import org.texttechnologylab.uima.conll.extractor.ConllBIO2003Writer;
import org.texttechnologylab.uima.conll.extractor.OneClassPerColumnWriter;

import java.io.IOException;

import static java.lang.System.exit;

public class ExtractAnnotated {
	public static String annotatedDir;
	public static String conllDir;
	private static String fileIncludePattern = "[+]*.xmi";
	
	
	public static void main(String[] args) throws IOException, UIMAException {
		Options options = new Options();
		options.addOption("h", "Print this message.");
		
		Option rootDirOption = new Option("r", "root", true,
				"Path to the root folder.");
		rootDirOption.setRequired(true);
		options.addOption(rootDirOption);
		
		Option taggedDirOption = new Option("t", "tagged", true,
				"Tagged XMI output path if not {rootDir}/xmi/annotated");
		taggedDirOption.setRequired(false);
		options.addOption(taggedDirOption);
		
		Option outputDirOption = new Option("o", "output", true,
				"Tagged CoNLL output path if not {rootDir}/conll/annotated");
		outputDirOption.setRequired(false);
		options.addOption(outputDirOption);
		
		Option singleTagOption = new Option("single", false,
				"If set, will only tag Taxon.");
		singleTagOption.setRequired(false);
		options.addOption(singleTagOption);
		
		try {
			DefaultParser defaultParser = new DefaultParser();
			CommandLine commandLine = defaultParser.parse(options, args);
			
			String rootDir = StringUtils.appendIfMissing(commandLine.getOptionValue("r"), "/");
			
			if (!commandLine.hasOption("t"))
				annotatedDir = rootDir + "xmi/annotated/";
			else
				annotatedDir = commandLine.getOptionValue("t");
			
			if (!commandLine.hasOption("o"))
				conllDir = rootDir + "conll/annotated/";
			else
				conllDir = commandLine.getOptionValue("o");
			
			extractCoNLL(commandLine.hasOption("single"));
		} catch (ParseException e) {
			printHelp(options);
			exit(-1);
		}
	}
	
	private static void printHelp(Options options) {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.printHelp(
				"",
				"",
				options,
				""
		);
	}
	
	private static void extractCoNLL(boolean single) {
		try {
			String[] annotatorBlacklist = {"0", "302904", "303228", "306320", "305718", "306513"};
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, annotatedDir,
					XmiReader.PARAM_PATTERNS, fileIncludePattern,
					XmiReader.PARAM_LENIENT, true
			);
			AggregateBuilder aggregateBuilder = new AggregateBuilder();
			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(AnnotationRemover.class,
					AnnotationRemover.PARAM_REMOVE_ANNOTATION, Token.class.getName(),
					AnnotationRemover.PARAM_INCLUDING_SUBTYPES, true
			
			));
			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(UnicodeRegexSegmenter.class,
					UnicodeRegexSegmenter.PARAM_WRITE_SENTENCE, false,
					UnicodeRegexSegmenter.PARAM_WRITE_TOKEN, true,
					UnicodeRegexSegmenter.PARAM_WRITE_FORM, false
			));
			if (single) {
				aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(ConllBIO2003Writer.class,
						ConllBIO2003Writer.PARAM_TARGET_LOCATION, conllDir,
						ConllBIO2003Writer.PARAM_OVERWRITE, true,
						ConllBIO2003Writer.PARAM_USE_TTLAB_TYPESYSTEM, true,
						ConllBIO2003Writer.PARAM_USE_TTLAB_CONLL_FEATURES, false,
						ConllBIO2003Writer.PARAM_FILTER_FINGERPRINTED, false,
						ConllBIO2003Writer.PARAM_ANNOTATOR_RELATION, ConllBIO2003Writer.BLACKLIST,
						ConllBIO2003Writer.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
						ConllBIO2003Writer.PARAM_MIN_VIEWS, 1,
						ConllBIO2003Writer.PARAM_FILTER_EMPTY_SENTENCES, true,
						ConllBIO2003Writer.PARAM_ONLY_PRINT_PRESENT, false,
						ConllBIO2003Writer.PARAM_RETAIN_CLASSES, new String[]{
								org.texttechnologylab.annotation.type.Taxon.class.getName(),
								org.texttechnologylab.annotation.type.concept.Taxon.class.getName(),
						}
//						, ConllBIO2003Writer.PARAM_TAG_ALL_AS, Taxon.class.getSimpleName()
				
				));
			} else {
				aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(OneClassPerColumnWriter.class,
						OneClassPerColumnWriter.PARAM_TARGET_LOCATION, conllDir,
						OneClassPerColumnWriter.PARAM_OVERWRITE, true,
						OneClassPerColumnWriter.PARAM_USE_TTLAB_TYPESYSTEM, true,
						OneClassPerColumnWriter.PARAM_USE_TTLAB_CONLL_FEATURES, false,
						OneClassPerColumnWriter.PARAM_FILTER_FINGERPRINTED, false,
						OneClassPerColumnWriter.PARAM_ANNOTATOR_RELATION, OneClassPerColumnWriter.BLACKLIST,
						OneClassPerColumnWriter.PARAM_ANNOTATOR_LIST, annotatorBlacklist,
						OneClassPerColumnWriter.PARAM_MIN_VIEWS, 1,
						OneClassPerColumnWriter.PARAM_FILTER_EMPTY_SENTENCES, true,
						OneClassPerColumnWriter.PARAM_ONLY_PRINT_PRESENT, false
				));
			}
			SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
		} catch (UIMAException | IOException e) {
			e.printStackTrace();
		}
	}
	
}
