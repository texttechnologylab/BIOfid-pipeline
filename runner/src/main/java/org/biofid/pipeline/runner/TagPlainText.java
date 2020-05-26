package org.biofid.pipeline.runner;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.biofid.deep_eos.DeepEosTagger;
import org.biofid.gazetteer.BIOfidTreeGazetteer;
import org.biofid.gazetteer.UnicodeRegexSegmenter;
import org.dkpro.core.io.xmi.XmiReader;
import org.dkpro.core.io.xmi.XmiWriter;
import org.texttechnologylab.annotation.type.*;
import org.texttechnologylab.uima.conll.extractor.OneClassPerColumnWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static java.lang.System.exit;

public class TagPlainText {
	public static String sourceDir;
	public static String cleanDir;
	public static String classesDir;
	public static String taggedDir;
	public static String conllDir;
	private static final String plainIncludePattern = "[+]wikipedia*.txt";
	private static final String xmiIncludePattern = "[+]wikipedia*.xmi";
	
	public static void main(String[] args) throws IOException, UIMAException {
		Options options = new Options();
		options.addOption("h", "Print this message.");
		
		Option rootDirOption = new Option("r", "root", true,
				"Path to the root folder.");
		rootDirOption.setRequired(true);
		options.addOption(rootDirOption);
		
		Option classesDirOption = new Option("c", "classes", true,
				"Path to the folder containing the '{class}/*.list' folders.");
		classesDirOption.setRequired(false);
		options.addOption(classesDirOption);
		
		Option sourceDirOption = new Option("s", "source", true,
				"XMI file source path if not in {rootDir}/plain/");
		sourceDirOption.setRequired(false);
		options.addOption(sourceDirOption);
		
		Option cleanDirOption = new Option("c", "clean", true,
				"Cleaned XMI output path if not {rootDir}/xmi/clean");
		cleanDirOption.setRequired(false);
		options.addOption(cleanDirOption);
		
		Option taggedDirOption = new Option("t", "tagged", true,
				"Tagged XMI output path if not {rootDir}/xmi/tagged");
		taggedDirOption.setRequired(false);
		options.addOption(taggedDirOption);
		
		Option outputDirOption = new Option("o", "output", true,
				"Tagged CoNLL output path if not {rootDir}/conll/tagged");
		outputDirOption.setRequired(false);
		options.addOption(outputDirOption);
		
		try {
			DefaultParser defaultParser = new DefaultParser();
			CommandLine commandLine = defaultParser.parse(options, args);
			
			String rootDir = StringUtils.appendIfMissing(commandLine.getOptionValue("r"), "/");
			
			if (!commandLine.hasOption("c"))
				classesDir = rootDir + "classes/biofid/";
			else
				classesDir = commandLine.getOptionValue("c");
			
			if (!commandLine.hasOption("s"))
				sourceDir = rootDir + "plain/";
			else
				sourceDir = commandLine.getOptionValue("s");
			
			if (!commandLine.hasOption("s"))
				cleanDir = rootDir + "xmi/clean/";
			else
				cleanDir = commandLine.getOptionValue("c");
			
			if (!commandLine.hasOption("t"))
				taggedDir = rootDir + "xmi/tagged/";
			else
				taggedDir = commandLine.getOptionValue("t");
			
			if (!commandLine.hasOption("o"))
				conllDir = rootDir + "conll/tagged/";
			else
				conllDir = commandLine.getOptionValue("o");
			
			Option noPreProcessOption = new Option("nopre", false,
					"If set, no pre-processing will be done.");
			noPreProcessOption.setRequired(false);
			options.addOption(noPreProcessOption);
			
			Option noTagOption = new Option("notag", false,
					"If set, no tagging will be done.");
			noTagOption.setRequired(false);
			options.addOption(noTagOption);
			
			if (!commandLine.hasOption("nopre"))
				preProcess();
			
			if (!commandLine.hasOption("notag"))
				tagTaxa();
			
			extractCoNLL();
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
	
	public static void preProcess() throws IOException, UIMAException {
		String CONDA_PREFIX = System.getenv("HOME") + "/anaconda3/";
		
		CollectionReader reader = CollectionReaderFactory.createReader(PlainTextCollectionReader.class,
				PlainTextCollectionReader.PARAM_SOURCE_LOCATION, sourceDir,
				PlainTextCollectionReader.PARAM_PATTERNS, plainIncludePattern
		);
		
		AggregateBuilder aggregateBuilder = new AggregateBuilder();
		aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(UnicodeRegexSegmenter.class,
				UnicodeRegexSegmenter.PARAM_TOKEN_BOUNDARY_REGEX, "\\s+",
				UnicodeRegexSegmenter.PARAM_WRITE_SENTENCE, false,
				UnicodeRegexSegmenter.PARAM_WRITE_TOKEN, true,
				UnicodeRegexSegmenter.PARAM_WRITE_FORM, false
		));
		aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(DeepEosTagger.class,
				DeepEosTagger.PARAM_MODEL_NAME, "de-wiki",
				DeepEosTagger.PARAM_PYTHON_HOME, CONDA_PREFIX + "envs/keras",
				DeepEosTagger.PARAM_LIBJEP_PATH, CONDA_PREFIX + "envs/keras/lib/python3.7/site-packages/jep/libjep.so"
		));
		aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
				XmiWriter.PARAM_TARGET_LOCATION, cleanDir,
				XmiWriter.PARAM_PRETTY_PRINT, true,
				XmiWriter.PARAM_OVERWRITE, true
		));
		
		new File(cleanDir).mkdirs();
		SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
		reader.destroy();
	}
	
	private static void tagTaxa() {
		try {
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, cleanDir,
					XmiReader.PARAM_PATTERNS, xmiIncludePattern
			);
			AggregateBuilder aggregateBuilder = new AggregateBuilder();
			
			for (ImmutablePair<String, String> pair : Arrays.asList(
					ImmutablePair.of("Archaea", Archaea.class.getName()),
					ImmutablePair.of("Bacteria", Bacteria.class.getName()),
					ImmutablePair.of("Chromista", Chromista.class.getName()),
					ImmutablePair.of("Fungi", Fungi.class.getName()),
//					ImmutablePair.of("Habitat", Habitat.class.getName()),
					ImmutablePair.of("Lichen", Lichen.class.getName()),
					ImmutablePair.of("Protozoa", Protozoa.class.getName()),
					ImmutablePair.of("Viruses", Viruses.class.getName())
			)) {
				aggregateBuilder.add(
						String.format("BIOfidTreeGazetteer[%s]", pair.left),
						AnalysisEngineFactory.createEngineDescription(
								BIOfidTreeGazetteer.class,
								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, classesDir + pair.left,
								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
								BIOfidTreeGazetteer.PARAM_FILTER_LOCATION, classesDir + "vocab-5000.txt",
								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
						));
			}
			new File(taggedDir).mkdirs();
			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
					XmiWriter.PARAM_TARGET_LOCATION, taggedDir,
					XmiWriter.PARAM_USE_DOCUMENT_ID, true,
					XmiWriter.PARAM_PRETTY_PRINT, true,
					XmiWriter.PARAM_OVERWRITE, true
			));
			SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		 * Single class tagging
		 */
		try {
			for (ImmutablePair<String, String> pair : Arrays.asList(
					ImmutablePair.of("Animal_Fauna", Animal_Fauna.class.getName()),
					ImmutablePair.of("Plant_Flora", Plant_Flora.class.getName()),
					ImmutablePair.of("Taxon", Taxon.class.getName())
			)) {
				final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
						XmiReader.PARAM_SOURCE_LOCATION, taggedDir,
						XmiReader.PARAM_PATTERNS, xmiIncludePattern
				);
				AggregateBuilder aggregateBuilder = new AggregateBuilder();
				aggregateBuilder.add(
						String.format("BIOfidTreeGazetteer[%s]", pair.left),
						AnalysisEngineFactory.createEngineDescription(
								BIOfidTreeGazetteer.class,
								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, classesDir + pair.left,
								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
								BIOfidTreeGazetteer.PARAM_FILTER_LOCATION, classesDir + "vocab-5000.txt",
								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
						));
				aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
						XmiWriter.PARAM_TARGET_LOCATION, taggedDir,
						XmiWriter.PARAM_USE_DOCUMENT_ID, true,
						XmiWriter.PARAM_PRETTY_PRINT, true,
						XmiWriter.PARAM_OVERWRITE, true
				));
				SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void extractCoNLL() {
		try {
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, taggedDir,
					XmiReader.PARAM_PATTERNS, xmiIncludePattern
			);
			AnalysisEngineDescription oneClassPerColumnWriter = AnalysisEngineFactory.createEngineDescription(OneClassPerColumnWriter.class,
					OneClassPerColumnWriter.PARAM_TARGET_LOCATION, conllDir,
					OneClassPerColumnWriter.PARAM_OVERWRITE, true,
					OneClassPerColumnWriter.PARAM_USE_TTLAB_TYPESYSTEM, true,
					OneClassPerColumnWriter.PARAM_USE_TTLAB_CONLL_FEATURES, false,
					OneClassPerColumnWriter.PARAM_FILTER_FINGERPRINTED, false,
					OneClassPerColumnWriter.PARAM_REMOVE_DUPLICATES_SAME_TYPE, false,
					OneClassPerColumnWriter.PARAM_MERGE_VIEWS, false,
					OneClassPerColumnWriter.PARAM_MIN_VIEWS, 0,
					OneClassPerColumnWriter.PARAM_FILTER_EMPTY_SENTENCES, true,
					OneClassPerColumnWriter.PARAM_ONLY_PRINT_PRESENT, false
			);
			SimplePipeline.runPipeline(reader, oneClassPerColumnWriter);
		} catch (UIMAException | IOException e) {
			e.printStackTrace();
		}
	}
	
}
