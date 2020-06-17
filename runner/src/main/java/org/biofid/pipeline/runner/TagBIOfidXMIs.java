package org.biofid.pipeline.runner;

import com.google.common.collect.ImmutableSet;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.*;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.flow.impl.FixedFlowController;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.biofid.gazetteer.MultiClassTreeGazetteer;
import org.biofid.gazetteer.SingleClassTreeGazetteer;
import org.dkpro.core.io.xmi.XmiReader;
import org.dkpro.core.io.xmi.XmiWriter;
import org.texttechnologylab.annotation.type.*;
import org.texttechnologylab.uima.conll.extractor.ConllBIO2003Writer;
import org.texttechnologylab.uima.conll.extractor.OneClassPerColumnWriter;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.exit;

public class TagBIOfidXMIs {
	public static String sourceDir;
	public static String cleanDir;
	public static String classesDir;
	public static String taggedDir;
	public static String conllDir;
	private static String marMoTModelLocation;
	private static String fileIncludePattern = "[+]*.xmi";
	
	
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
				"XMI file source path if not in {rootDir}/xmi/source");
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
		
		Option marMoTModelLocationOption = new Option("m", "marmot", true,
				"Path to the MarMoT model.");
		marMoTModelLocationOption.setRequired(false);
		options.addOption(marMoTModelLocationOption);
		
		Option noPreProcessOption = new Option("nopre", false,
				"If set, no pre-processing will be done.");
		noPreProcessOption.setRequired(false);
		options.addOption(noPreProcessOption);
		
		Option noTagOption = new Option("notag", false,
				"If set, no tagging will be done.");
		noTagOption.setRequired(false);
		options.addOption(noTagOption);
		
		Option singleTagOption = new Option("single", false,
				"If set, will only tag Taxon.");
		singleTagOption.setRequired(false);
		options.addOption(singleTagOption);
		
		try {
			DefaultParser defaultParser = new DefaultParser();
			CommandLine commandLine = defaultParser.parse(options, args);
			
			String rootDir = StringUtils.appendIfMissing(commandLine.getOptionValue("r"), "/");
			
			marMoTModelLocation = commandLine.getOptionValue("m");
			
			if (!commandLine.hasOption("c"))
				classesDir = rootDir + "classes/biofid/";
			else
				classesDir = commandLine.getOptionValue("c");
			
			if (!commandLine.hasOption("s"))
				sourceDir = rootDir + "xmi/source/";
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
			
			if (!commandLine.hasOption("nopre"))
				preProcess();
			
			if (!commandLine.hasOption("notag")) {
				if (commandLine.hasOption("single")) {
					tagTaxaSingle();
				} else {
					tagTaxa();
				}
			}
			
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
	
	public static void preProcess() throws IOException, UIMAException {
		CollectionReader xmiReader = CollectionReaderFactory.createReader(XmiReader.class,
				XmiReader.PARAM_SOURCE_LOCATION, sourceDir,
				XmiReader.PARAM_PATTERNS, "[+]**.xmi",
				XmiReader.PARAM_LENIENT, true
		);
		
		AggregateBuilder aggregateBuilder = new AggregateBuilder();
		aggregateBuilder.setFlowControllerDescription(FlowControllerFactory.createFlowControllerDescription(FixedFlowController.class,
				FixedFlowController.PARAM_ACTION_AFTER_CAS_MULTIPLIER, "drop"));
		aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(PreprocessingEngine.class
				, PreprocessingEngine.PARAM_MARMOT_MODEL_LOCATION, marMoTModelLocation
		));
		aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
				XmiWriter.PARAM_TARGET_LOCATION, cleanDir,
				XmiWriter.PARAM_PRETTY_PRINT, true,
				XmiWriter.PARAM_OVERWRITE, true
		));
		
		new File(cleanDir).mkdirs();
		SimplePipeline.runPipeline(xmiReader, aggregateBuilder.createAggregate());
		xmiReader.destroy();
	}
	
	private static void tagTaxa() {
		try {
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, cleanDir,
					XmiReader.PARAM_PATTERNS, fileIncludePattern
			);
			AggregateBuilder aggregateBuilder = new AggregateBuilder();
			
			ArrayList<String> sourceLocations = new ArrayList<>();
			ArrayList<String> typeNames = new ArrayList<>();
			List<Class<? extends NamedEntity>> taggingClasses = Arrays.asList(
					Archaea.class,
					Bacteria.class,
					Chromista.class,
					Fungi.class,
//					Habitat.class,
					Lichen.class,
					Protozoa.class,
					Animal_Fauna.class,
					Plant_Flora.class,
					Taxon.class,
					Viruses.class
			);
			for (Class<? extends NamedEntity> aClass : taggingClasses) {
				sourceLocations.add(classesDir + aClass.getSimpleName());
				typeNames.add(aClass.getName());
			}
			aggregateBuilder.add(
					"MultiClassTreeGazetteer",
					AnalysisEngineFactory.createEngineDescription(MultiClassTreeGazetteer.class,
							MultiClassTreeGazetteer.PARAM_SOURCE_LOCATION, sourceLocations.toArray(new String[0]),
							MultiClassTreeGazetteer.PARAM_CLASS_MAPPING, typeNames.toArray(new String[0]),
							MultiClassTreeGazetteer.PARAM_USE_LOWERCASE, true,
							MultiClassTreeGazetteer.PARAM_USE_STRING_TREE, true,
							MultiClassTreeGazetteer.PARAM_FILTER_LOCATION, classesDir + "vocab-5000.txt",
							MultiClassTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
					));
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
	}
	
	private static void tagTaxaSingle() {
		try {
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, cleanDir,
					XmiReader.PARAM_PATTERNS, fileIncludePattern
			);
			AggregateBuilder aggregateBuilder = new AggregateBuilder();
			
			ArrayList<String> sourceLocations = new ArrayList<>();
			List<Class<? extends NamedEntity>> taggingClasses = Arrays.asList(
					Archaea.class,
					Bacteria.class,
					Chromista.class,
					Fungi.class,
//					Habitat.class,
					Lichen.class,
					Protozoa.class,
					Animal_Fauna.class,
					Plant_Flora.class,
					Taxon.class,
					Viruses.class
			);
			for (Class<? extends NamedEntity> aClass : taggingClasses) {
				sourceLocations.add(classesDir + aClass.getSimpleName());
			}
			aggregateBuilder.add(
					"SingleClassTreeGazetteer",
					AnalysisEngineFactory.createEngineDescription(SingleClassTreeGazetteer.class,
							SingleClassTreeGazetteer.PARAM_SOURCE_LOCATION, sourceLocations.toArray(new String[0]),
							SingleClassTreeGazetteer.PARAM_TAGGING_TYPE_NAME, Taxon.class.getName(),
							SingleClassTreeGazetteer.PARAM_USE_LOWERCASE, true,
							SingleClassTreeGazetteer.PARAM_USE_STRING_TREE, true,
							SingleClassTreeGazetteer.PARAM_FILTER_LOCATION, classesDir + "filter_words-de_en.txt",
							SingleClassTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
					));
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
	}
	
	private static void extractCoNLL(boolean single) {
		try {
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, taggedDir,
					XmiReader.PARAM_PATTERNS, fileIncludePattern
			);
			AnalysisEngineDescription conllWriter;
			if (single) {
				conllWriter = AnalysisEngineFactory.createEngineDescription(ConllBIO2003Writer.class,
						ConllBIO2003Writer.PARAM_TARGET_LOCATION, conllDir,
						ConllBIO2003Writer.PARAM_OVERWRITE, true,
						ConllBIO2003Writer.PARAM_USE_TTLAB_TYPESYSTEM, true,
						ConllBIO2003Writer.PARAM_USE_TTLAB_CONLL_FEATURES, false,
						ConllBIO2003Writer.PARAM_FILTER_FINGERPRINTED, false,
						ConllBIO2003Writer.PARAM_REMOVE_DUPLICATES_SAME_TYPE, false,
						ConllBIO2003Writer.PARAM_MERGE_VIEWS, false,
						ConllBIO2003Writer.PARAM_MIN_VIEWS, 0,
						ConllBIO2003Writer.PARAM_FILTER_EMPTY_SENTENCES, true
				);
			} else {
				conllWriter = AnalysisEngineFactory.createEngineDescription(OneClassPerColumnWriter.class,
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
			}
			SimplePipeline.runPipeline(reader, conllWriter);
		} catch (UIMAException | IOException e) {
			e.printStackTrace();
		}
	}
	
	@Deprecated
	public static void clean() {
		try {
			new File(cleanDir).mkdirs();
			CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_PATTERNS, "[+]**.xmi",
					XmiReader.PARAM_SOURCE_LOCATION, sourceDir,
					XmiReader.PARAM_LENIENT, true
			);
			JCas jCas = JCasFactory.createJCas();
			while (reader.hasNext()) {
				try {
					reader.getNext(jCas.getCas());
					final JCas initialView = jCas.getView("_InitialView");
					
					JCas copy = JCasFactory.createText(initialView.getDocumentText());
					
					for (Class<? extends Annotation> cls : ImmutableSet.of(Sentence.class, Token.class)) {
						JCasUtil.select(initialView, cls).forEach(
								top -> AnnotationFactory.createAnnotation(copy, top.getBegin(), top.getEnd(), cls)
						);
					}
					
					JCasUtil.select(initialView, Lemma.class).forEach(
							top -> {
								Lemma annotation = AnnotationFactory.createAnnotation(copy, top.getBegin(), top.getEnd(), Lemma.class);
								annotation.setValue(top.getValue());
							}
					);
					JCasUtil.select(initialView, POS.class).forEach(
							top -> {
								POS annotation = AnnotationFactory.createAnnotation(copy, top.getBegin(), top.getEnd(), POS.class);
								annotation.setPosValue(top.getPosValue());
								annotation.setCoarseValue(top.getCoarseValue());
							}
					);
					
					DocumentMetaData documentMetaData = DocumentMetaData.get(initialView);
					DocumentMetaData.copy(initialView, copy);
					
					File file = new File(cleanDir + documentMetaData.getDocumentId() + ".xmi");
					if (file.exists()) {
						file.delete();
					}
					XmiSerializationSharedData sharedData = new XmiSerializationSharedData();
					XmiCasSerializer.serialize(copy.getCas(), copy.getTypeSystem(), new FileOutputStream(file), true, sharedData);
				} catch (UIMAException | SAXException e) {
					e.printStackTrace();
				}
				jCas.reset();
			}
		} catch (IOException | UIMAException e) {
			e.printStackTrace();
		}
	}
}
