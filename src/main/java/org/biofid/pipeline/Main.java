package org.biofid.pipeline;

import com.google.common.collect.ImmutableSet;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.*;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.flow.impl.FixedFlowController;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.biofid.gazetteer.BIOfidTreeGazetteer;
import org.dkpro.core.io.xmi.XmiReader;
import org.dkpro.core.io.xmi.XmiWriter;
import org.texttechnologylab.annotation.type.*;
import org.texttechnologylab.uima.conll.extractor.OneClassPerColumnWriter;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class Main {
	public static final String rootDir = System.getenv("ROOTDIR");
	public static final String sourceDir = rootDir + "data/xmi/source/";
	public static final String cleanDir = rootDir + "data/xmi/clean/";
	public static final String classesDir = rootDir + "data/classes/biofid/";
	public static final String annotatedDir = rootDir + "data/annotated/";
	public static final String conllDir = rootDir + "data/conll/";
	
	public static void main(String[] args) throws IOException, UIMAException {
		preProcess();
		tagTaxa();
//		try {
//			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
//					XmiReader.PARAM_SOURCE_LOCATION, "src/test/resources/out/plain/",
//					Conll2003Reader.PARAM_PATTERNS, "[+]**.xmi"
//			);
//			AggregateBuilder aggregateBuilder = new AggregateBuilder();
//			for (ImmutablePair<String, String> pair : Arrays.asList(
//					ImmutablePair.of(Animal_Fauna.class.getSimpleName(), Animal_Fauna.class.getName()),
//					ImmutablePair.of(Archaea.class.getSimpleName(), Archaea.class.getName()),
//					ImmutablePair.of(Bacteria.class.getSimpleName(), Bacteria.class.getName()),
//					ImmutablePair.of(Chromista.class.getSimpleName(), Chromista.class.getName()),
//					ImmutablePair.of(Feeling_Emotion.class.getSimpleName(), Feeling_Emotion.class.getName()),
//					ImmutablePair.of(Food.class.getSimpleName(), Food.class.getName()),
//					ImmutablePair.of(Fungi.class.getSimpleName(), Fungi.class.getName()),
//					ImmutablePair.of(Habitat.class.getSimpleName(), Habitat.class.getName()),
//					ImmutablePair.of(Lichen.class.getSimpleName(), Lichen.class.getName()),
//					ImmutablePair.of(NaturalPhenomenon.class.getSimpleName(), NaturalPhenomenon.class.getName()),
//					ImmutablePair.of(Plant_Flora.class.getSimpleName(), Plant_Flora.class.getName()),
//					ImmutablePair.of(Protozoa.class.getSimpleName(), Protozoa.class.getName()),
//					ImmutablePair.of(Quantity_Amount.class.getSimpleName(), Quantity_Amount.class.getName()),
//					ImmutablePair.of(Reproduction.class.getSimpleName(), Reproduction.class.getName()),
//					ImmutablePair.of(Shape.class.getSimpleName(), Shape.class.getName()),
//					ImmutablePair.of(Substance.class.getSimpleName(), Substance.class.getName()),
//					ImmutablePair.of(Viruses.class.getSimpleName(), Viruses.class.getName())
//			)) {
//				aggregateBuilder.add(
//						String.format("BIOfidTreeGazetteer[%s]", pair.left),
//						AnalysisEngineFactory.createEngineDescription(
//								BIOfidTreeGazetteer.class,
//								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, "src/test/resources/" + pair.left + ".list",
//								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
//								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
//								BIOfidTreeGazetteer.PARAM_USE_STRING_TREE, true,
//								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
//						));
//			}
//			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
//					XmiWriter.PARAM_TARGET_LOCATION, "src/test/resources/out/annotated/",
//					XmiWriter.PARAM_USE_DOCUMENT_ID, true,
//					XmiWriter.PARAM_PRETTY_PRINT, true,
//					XmiWriter.PARAM_OVERWRITE, true
//			));
//			SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		try {
//			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
//					XmiReader.PARAM_SOURCE_LOCATION, "src/test/resources/out/annotated/",
//					Conll2003Reader.PARAM_PATTERNS, "[+]**.xmi"
//			);
//			AggregateBuilder aggregateBuilder = new AggregateBuilder();
//			for (ImmutablePair<String, String> pair : Arrays.asList(
//					ImmutablePair.of(Attribute_Property.class.getSimpleName(), Attribute_Property.class.getName()),
//					ImmutablePair.of(Body_Corpus.class.getSimpleName(), Body_Corpus.class.getName()),
//					ImmutablePair.of(Cognition_Ideation.class.getSimpleName(), Cognition_Ideation.class.getName()),
//					ImmutablePair.of(Morphology.class.getSimpleName(), Morphology.class.getName()),
//					ImmutablePair.of(Motive.class.getSimpleName(), Motive.class.getName())
//			)) {
//				aggregateBuilder.add(
//						String.format("BIOfidTreeGazetteer[%s]", pair.left),
//						AnalysisEngineFactory.createEngineDescription(
//								BIOfidTreeGazetteer.class,
//								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, "src/test/resources/" + pair.left + ".list",
//								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
//								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
//								BIOfidTreeGazetteer.PARAM_USE_STRING_TREE, true,
//								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
//						));
//			}
//			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
//					XmiWriter.PARAM_TARGET_LOCATION, "src/test/resources/out/annotated/",
//					XmiWriter.PARAM_USE_DOCUMENT_ID, true,
//					XmiWriter.PARAM_PRETTY_PRINT, true,
//					XmiWriter.PARAM_OVERWRITE, true
//			));
//			SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		try {
//			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
//					XmiReader.PARAM_SOURCE_LOCATION, "src/test/resources/out/annotated/",
//					Conll2003Reader.PARAM_PATTERNS, "[+]**.xmi"
//			);
//			AggregateBuilder aggregateBuilder = new AggregateBuilder();
//			for (ImmutablePair<String, String> pair : Arrays.asList(
//					ImmutablePair.of(Possession_Property.class.getSimpleName(), Possession_Property.class.getName()),
//					ImmutablePair.of(Relation.class.getSimpleName(), Relation.class.getName()),
//					ImmutablePair.of(Society.class.getSimpleName(), Society.class.getName()),
//					ImmutablePair.of(State_Condition.class.getSimpleName(), State_Condition.class.getName()),
//					ImmutablePair.of(Time.class.getSimpleName(), Time.class.getName())
//			)) {
//				aggregateBuilder.add(
//						String.format("BIOfidTreeGazetteer[%s]", pair.left),
//						AnalysisEngineFactory.createEngineDescription(
//								BIOfidTreeGazetteer.class,
//								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, "src/test/resources/" + pair.left + ".list",
//								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
//								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
//								BIOfidTreeGazetteer.PARAM_USE_STRING_TREE, true,
//								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
//						));
//			}
//			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
//					XmiWriter.PARAM_TARGET_LOCATION, "src/test/resources/out/annotated/",
//					XmiWriter.PARAM_USE_DOCUMENT_ID, true,
//					XmiWriter.PARAM_PRETTY_PRINT, true,
//					XmiWriter.PARAM_OVERWRITE, true
//			));
//			SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		/*
//		 * Single class tagging
//		 */
//		try {
//			for (ImmutablePair<String, String> pair : Arrays.asList(
////					ImmutablePair.of(Act_Action_Activity.class.getSimpleName(), Act_Action_Activity.class.getName()),
//					ImmutablePair.of(Artifact.class.getSimpleName(), Artifact.class.getName()),
//					ImmutablePair.of(Communication.class.getSimpleName(), Communication.class.getName()),
//					ImmutablePair.of(Event_Happening.class.getSimpleName(), Event_Happening.class.getName()),
//					ImmutablePair.of(Group_Collection.class.getSimpleName(), Group_Collection.class.getName()),
//					ImmutablePair.of(Location_Place.class.getSimpleName(), Location_Place.class.getName()),
//					ImmutablePair.of(NaturalObject.class.getSimpleName(), NaturalObject.class.getName()),
//					ImmutablePair.of(Person_HumanBeing.class.getSimpleName(), Person_HumanBeing.class.getName()),
//					ImmutablePair.of(Process.class.getSimpleName(), Process.class.getName())
//			)) {
//				final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
//						XmiReader.PARAM_SOURCE_LOCATION, "src/test/resources/out/annotated/",
//						Conll2003Reader.PARAM_PATTERNS, "[+]**.xmi"
//				);
//				AggregateBuilder aggregateBuilder = new AggregateBuilder();
//				aggregateBuilder.add(
//						String.format("BIOfidTreeGazetteer[%s]", pair.left),
//						AnalysisEngineFactory.createEngineDescription(
//								BIOfidTreeGazetteer.class,
//								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, "src/test/resources/" + pair.left + ".list",
//								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
//								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
//								BIOfidTreeGazetteer.PARAM_USE_STRING_TREE, true,
//								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
//						));
//				aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
//						XmiWriter.PARAM_TARGET_LOCATION, "src/test/resources/out/annotated/",
//						XmiWriter.PARAM_USE_DOCUMENT_ID, true,
//						XmiWriter.PARAM_PRETTY_PRINT, true,
//						XmiWriter.PARAM_OVERWRITE, true
//				));
//				SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		try {
//			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
//					XmiReader.PARAM_SOURCE_LOCATION, "src/test/resources/out/annotated/",
//					Conll2003Reader.PARAM_PATTERNS, "[+]**.xmi"
//			);
//			AggregateBuilder aggregateBuilder = new AggregateBuilder();
//			aggregateBuilder.add(
//					String.format("BIOfidTreeGazetteer[%s]", Taxon.class.getSimpleName()),
//					AnalysisEngineFactory.createEngineDescription(
//							BIOfidTreeGazetteer.class,
//							BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, "src/test/resources/BIOfidTaxa.zip",
//							BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, Taxon.class.getName(),
//							BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
//							BIOfidTreeGazetteer.PARAM_USE_STRING_TREE, true,
//							BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
//					));
//			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
//					XmiWriter.PARAM_TARGET_LOCATION, "src/test/resources/out/annotated/",
//					XmiWriter.PARAM_USE_DOCUMENT_ID, true,
//					XmiWriter.PARAM_PRETTY_PRINT, true,
//					XmiWriter.PARAM_OVERWRITE, true
//			));
//			SimplePipeline.runPipeline(reader, aggregateBuilder.createAggregate());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}
	
	private static void tagTaxa() {
		try {
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, cleanDir,
					XmiReader.PARAM_PATTERNS, "[+]**.xmi"
			);
			AggregateBuilder aggregateBuilder = new AggregateBuilder();
			
			for (ImmutablePair<String, String> pair : Arrays.asList(
					ImmutablePair.of(Archaea.class.getSimpleName(), Archaea.class.getName()),
					ImmutablePair.of(Bacteria.class.getSimpleName(), Bacteria.class.getName()),
					ImmutablePair.of(Chromista.class.getSimpleName(), Chromista.class.getName()),
					ImmutablePair.of(Fungi.class.getSimpleName(), Fungi.class.getName()),
//					ImmutablePair.of(Habitat.class.getSimpleName(), Habitat.class.getName()),
					ImmutablePair.of(Lichen.class.getSimpleName(), Lichen.class.getName()),
					ImmutablePair.of(Protozoa.class.getSimpleName(), Protozoa.class.getName()),
					ImmutablePair.of(Viruses.class.getSimpleName(), Viruses.class.getName())
			)) {
				aggregateBuilder.add(
						String.format("BIOfidTreeGazetteer[%s]", pair.left),
						AnalysisEngineFactory.createEngineDescription(
								BIOfidTreeGazetteer.class,
								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, classesDir + pair.left,
								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
								BIOfidTreeGazetteer.PARAM_USE_STRING_TREE, true,
								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true,
								BIOfidTreeGazetteer.PARAM_USE_LEMMATA, true
						));
			}
			new File(annotatedDir).mkdirs();
			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
					XmiWriter.PARAM_TARGET_LOCATION, annotatedDir,
					XmiWriter.PARAM_USE_DOCUMENT_ID, true,
					XmiWriter.PARAM_PRETTY_PRINT, true,
					XmiWriter.PARAM_OVERWRITE, true
			));
			new File(conllDir).mkdirs();
			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(OneClassPerColumnWriter.class,
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
					ImmutablePair.of(Animal_Fauna.class.getSimpleName(), Animal_Fauna.class.getName()),
					ImmutablePair.of(Plant_Flora.class.getSimpleName(), Animal_Fauna.class.getName()),
					ImmutablePair.of(Taxon.class.getSimpleName(), Taxon.class.getName())
			)) {
				final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
						XmiReader.PARAM_SOURCE_LOCATION, conllDir,
						XmiReader.PARAM_PATTERNS, "[+]**.xmi"
				);
				AggregateBuilder aggregateBuilder = new AggregateBuilder();
				aggregateBuilder.add(
						String.format("BIOfidTreeGazetteer[%s]", pair.left),
						AnalysisEngineFactory.createEngineDescription(
								BIOfidTreeGazetteer.class,
								BIOfidTreeGazetteer.PARAM_SOURCE_LOCATION, classesDir + pair.left,
								BIOfidTreeGazetteer.PARAM_TAGGING_TYPE_NAME, pair.right,
								BIOfidTreeGazetteer.PARAM_USE_LOWERCASE, true,
								BIOfidTreeGazetteer.PARAM_USE_STRING_TREE, true,
								BIOfidTreeGazetteer.PARAM_USE_SENTECE_LEVEL_TAGGING, true
						));
				aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
						XmiWriter.PARAM_TARGET_LOCATION, conllDir,
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
	
	public static void preProcess() throws IOException, UIMAException {
		CollectionReader xmiReader = CollectionReaderFactory.createReader(XmiReader.class,
				XmiReader.PARAM_SOURCE_LOCATION, sourceDir,
				XmiReader.PARAM_PATTERNS, "[+]**.xmi",
				XmiReader.PARAM_LENIENT, true
		);
		
		AggregateBuilder aggregateBuilder = new AggregateBuilder();
		aggregateBuilder.setFlowControllerDescription(FlowControllerFactory.createFlowControllerDescription(FixedFlowController.class,
				FixedFlowController.PARAM_ACTION_AFTER_CAS_MULTIPLIER, "drop"));
		
		aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(PreprocessingEngine.class,
				PreprocessingEngine.PARAM_MARMOT_MODEL_LOCATION, rootDir + "all-tiger_nolemm_embeddings_lexicon.marmot"
		));
		aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(XmiWriter.class,
				XmiWriter.PARAM_TARGET_LOCATION, cleanDir,
				XmiWriter.PARAM_PRETTY_PRINT, true,
				XmiWriter.PARAM_OVERWRITE, true
		));
		
		new File(cleanDir).mkdirs();
		SimplePipeline.runPipeline(xmiReader, aggregateBuilder.createAggregate());
		
	}
	
	
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
