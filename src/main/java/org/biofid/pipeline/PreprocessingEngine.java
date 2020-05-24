package org.biofid.pipeline;

import de.tudarmstadt.ukp.dkpro.core.api.anomaly.type.Anomaly;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.AbstractCas;
import org.apache.uima.fit.component.JCasMultiplier_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.biofid.deep_eos.DeepEosTagger;
import org.dkpro.core.tokit.RegexSegmenter;
import org.hucompute.textimager.uima.marmot.MarMoTLemma;
import org.hucompute.textimager.uima.marmot.MarMoTTagger;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

public class PreprocessingEngine extends JCasMultiplier_ImplBase {
	
	private static final String CONDA_PREFIX = System.getenv("HOME") + "/anaconda3/";
	
	public static final String PARAM_MARMOT_MODEL_LOCATION = "pParamMarmotModelLocation";
	@ConfigurationParameter(name = PARAM_MARMOT_MODEL_LOCATION)
	private String pParamMarmotModelLocation;
	
	private AnalysisEngine regexSegmenter;
	private AnalysisEngine deepEosTagger;
	private AnalysisEngine marMoTTagger;
	private AnalysisEngine marMoTLemma;
	private JCas outputCas;
	
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		
		try {
			outputCas = JCasFactory.createJCas();
			getLogger().info("Creating RegexSegmenter");
			regexSegmenter = AnalysisEngineFactory.createEngine(RegexSegmenter.class,
					RegexSegmenter.PARAM_TOKEN_BOUNDARY_REGEX, "[^\\p{Alnum}-]+",
					RegexSegmenter.PARAM_WRITE_SENTENCE, false,
					RegexSegmenter.PARAM_WRITE_TOKEN, true,
					RegexSegmenter.PARAM_WRITE_FORM, false
			);
			getLogger().info("Creating Deep-EOS Tagger");
			deepEosTagger = AnalysisEngineFactory.createEngine(DeepEosTagger.class,
					DeepEosTagger.PARAM_MODEL_NAME, "biofid",
					DeepEosTagger.PARAM_PYTHON_HOME, CONDA_PREFIX + "envs/keras",
					DeepEosTagger.PARAM_LIBJEP_PATH, CONDA_PREFIX + "envs/keras/lib/python3.7/site-packages/jep/libjep.so"
			);
			getLogger().info("Creating MarMoT POS Tagger");
			marMoTTagger = AnalysisEngineFactory.createEngine(MarMoTTagger.class,
					MarMoTTagger.PARAM_LANGUAGE, "de",
					MarMoTTagger.PARAM_MODEL_LOCATION, pParamMarmotModelLocation
			);
			getLogger().info("Creating MarMoT Lemma Tagger");
			marMoTLemma = AnalysisEngineFactory.createEngine(MarMoTLemma.class,
					MarMoTLemma.PARAM_LANGUAGE, "de",
					MarMoTLemma.PARAM_MODEL_LOCATION, pParamMarmotModelLocation
			);
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
		
		HashSet<Token> annomalyTokens = JCasUtil.indexCovered(inputCas, Anomaly.class, Token.class).values().stream().flatMap(Collection::stream).collect(Collectors.toCollection(HashSet::new));
		String cleanedDocumentText = JCasUtil.select(inputCas, Token.class).stream()
				.sequential()
				.filter(o -> !annomalyTokens.contains(o))
				.map(Annotation::getCoveredText)
				.collect(Collectors.joining(" "));
		
		outputCas.setDocumentText(cleanedDocumentText);
		outputCas.setDocumentLanguage(inputCas.getDocumentLanguage());
		DocumentMetaData.copy(inputCas, outputCas);
		DocumentMetaData documentMetaData = DocumentMetaData.get(outputCas);
		documentMetaData.setCollectionId("BIOfid_Corpus-preprocessed");
		
		SimplePipeline.runPipeline(outputCas, deepEosTagger, regexSegmenter, marMoTTagger, marMoTLemma);
	}
}
