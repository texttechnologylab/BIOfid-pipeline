package pipeline.util;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.resource.ResourceInitializationException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AnnotationRemover extends JCasAnnotator_ImplBase {
	
	public static final String PARAM_REMOVE_ANNOTATION = "pRemoveAnnotation";
	@ConfigurationParameter(name = PARAM_REMOVE_ANNOTATION)
	String pRemoveAnnotation;
	
	public static final String PARAM_INCLUDING_SUBTYPES = "pIncludingSubtypes";
	@ConfigurationParameter(name = PARAM_INCLUDING_SUBTYPES, defaultValue = "true")
	Boolean pIncludingSubtypes;
	private Map<String, Integer> jCasIndexMap;
	
	
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		try {
			jCasIndexMap = IntStream
					.range(0, JCasRegistry.getNumberOfRegisteredClasses())
					.boxed()
					.collect(Collectors.toMap(
							i -> JCasRegistry.getClassForIndex(i).getName(),
							Function.identity(),
							(u, v) -> u,
							LinkedHashMap::new
					));
		} catch (Exception e) {
			throw new ResourceInitializationException(e);
		}
	}
	
	@Override
	public void process(JCas aJCas) throws AnalysisEngineProcessException {
		try {
			Integer i = jCasIndexMap.get(pRemoveAnnotation);
			if (i == null) {
				getLogger().info("Type not found in JCasRegistry, falling back to Class.forName");
				JCasUtil.select(aJCas, Class.forName(pRemoveAnnotation).asSubclass(TOP.class)).forEach(
						aJCas::removeFsFromIndexes
				);
			} else {
				if (pIncludingSubtypes) {
					aJCas.removeAllIncludingSubtypes(i);
				} else {
					aJCas.removeAllExcludingSubtypes(i);
				}
			}
		} catch (NullPointerException | ClassNotFoundException e) {
			throw new AnalysisEngineProcessException(e);
		}
	}
}
