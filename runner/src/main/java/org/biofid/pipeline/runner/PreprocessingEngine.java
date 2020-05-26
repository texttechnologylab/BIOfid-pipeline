package org.biofid.pipeline.runner;

import de.tudarmstadt.ukp.dkpro.core.api.anomaly.type.Anomaly;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.AbstractCas;
import org.apache.uima.fit.component.JCasMultiplier_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.biofid.deep_eos.DeepEosTagger;
import org.biofid.gazetteer.util.UnicodeRegexSegmenter;
import org.dkpro.core.api.transform.alignment.AlignedString;
import org.hucompute.textimager.uima.marmot.MarMoTLemma;
import org.hucompute.textimager.uima.marmot.MarMoTTagger;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.stream.Collectors;

public class PreprocessingEngine extends JCasMultiplier_ImplBase {
	
	private static final String CONDA_PREFIX = System.getenv("HOME") + "/anaconda3/";
	
	/**
	 * The file path to the MarMoT model. If none is given, lemmatization and POS tagging will be disabled.
	 */
	public static final String PARAM_MARMOT_MODEL_LOCATION = "pParamMarmotModelLocation";
	@ConfigurationParameter(name = PARAM_MARMOT_MODEL_LOCATION, mandatory = false)
	private String pParamMarmotModelLocation;
	
	private JCas outputCas;
	private AnalysisEngine analysisEngine;
	
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		
		try {
			AggregateBuilder aggregateBuilder = new AggregateBuilder();
			getLogger().info("Creating RegexSegmenter");
			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(UnicodeRegexSegmenter.class,
					UnicodeRegexSegmenter.PARAM_WRITE_SENTENCE, false,
					UnicodeRegexSegmenter.PARAM_WRITE_TOKEN, true,
					UnicodeRegexSegmenter.PARAM_WRITE_FORM, false
			));
			getLogger().info("Creating Deep-EOS Tagger");
			aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(DeepEosTagger.class,
					DeepEosTagger.PARAM_MODEL_NAME, "de-wiki",
					DeepEosTagger.PARAM_PYTHON_HOME, CONDA_PREFIX + "envs/keras",
					DeepEosTagger.PARAM_LIBJEP_PATH, CONDA_PREFIX + "envs/keras/lib/python3.7/site-packages/jep/libjep.so"
			));
			if (StringUtils.isNotEmpty(pParamMarmotModelLocation)) {
				getLogger().info("Creating MarMoT POS Tagger");
				aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(MarMoTTagger.class,
						MarMoTTagger.PARAM_LANGUAGE, "de",
						MarMoTTagger.PARAM_MODEL_LOCATION, pParamMarmotModelLocation
				));
				getLogger().info("Creating MarMoT Lemma Tagger");
				aggregateBuilder.add(AnalysisEngineFactory.createEngineDescription(MarMoTLemma.class,
						MarMoTLemma.PARAM_LANGUAGE, "de",
						MarMoTLemma.PARAM_MODEL_LOCATION, pParamMarmotModelLocation
				));
			}
			analysisEngine = aggregateBuilder.createAggregate();
			getLogger().info("Finished initialization");
		} catch (UIMAException e) {
			throw new ResourceInitializationException(e);
		}
		
	}
	
	
	@Override
	public boolean hasNext() throws AnalysisEngineProcessException {
		return outputCas != null;
	}
	
	@Override
	public AbstractCas next() throws AnalysisEngineProcessException {
		JCas buffer = outputCas;
		outputCas = null;
		return buffer;
	}
	
	
	@Override
	public void process(JCas inputCas) throws AnalysisEngineProcessException {
		outputCas = getEmptyJCas();
		
		String cleanedDocumentText;
		if (true) {
			final String documentText = inputCas.getDocumentText();
			AlignedString alignedString = new AlignedString(documentText);
			JCasUtil.select(inputCas, Anomaly.class)
					.stream().sorted(Comparator.comparingInt(Anomaly::getBegin).reversed())
					.sequential()
					.forEach(
							anomaly -> {
								int begin = anomaly.getBegin();
								int end = anomaly.getEnd();
								if (begin <= documentText.length() && end <= documentText.length())
									alignedString.delete(begin, end);
							}
					);
			cleanedDocumentText = alignedString.get();
		} else {
			HashSet<Token> annomalyTokens = JCasUtil.indexCovered(inputCas, Anomaly.class, Token.class).values().stream().flatMap(Collection::stream).collect(Collectors.toCollection(HashSet::new));
			JCasUtil.indexCovered(inputCas, Token.class, Token.class).values().stream().flatMap(Collection::stream).forEach(annomalyTokens::add);
			cleanedDocumentText = JCasUtil.select(inputCas, Token.class).stream()
					.sequential()
					.filter(o -> !annomalyTokens.contains(o))
					.map(Annotation::getCoveredText)
					.map(String::trim)
					.filter(StringUtils::isNotEmpty)
					.collect(Collectors.joining(" "));
		}
		
		outputCas.setDocumentText(cleanedDocumentText);
		outputCas.setDocumentLanguage(inputCas.getDocumentLanguage());
		DocumentMetaData.copy(inputCas, outputCas);
		DocumentMetaData documentMetaData = DocumentMetaData.get(outputCas);
		documentMetaData.setCollectionId("BIOfid_Corpus-preprocessed");
		
		SimplePipeline.runPipeline(outputCas, analysisEngine);
	}
}
