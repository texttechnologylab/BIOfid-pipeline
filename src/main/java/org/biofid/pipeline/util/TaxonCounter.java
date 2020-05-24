package org.biofid.pipeline.util;

import com.google.common.collect.Iterators;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.type.Taxon;

public class TaxonCounter extends JCasAnnotator_ImplBase {
	
	@Override
	public void process(JCas aJCas) throws AnalysisEngineProcessException {
		
		getLogger().info(String.format("Found %d taxa!", Iterators.size(aJCas.getAllIndexedFS(Taxon.class))));
	}
}
