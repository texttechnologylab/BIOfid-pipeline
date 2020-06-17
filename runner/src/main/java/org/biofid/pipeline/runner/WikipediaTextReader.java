package org.biofid.pipeline.runner;

import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.dkpro.core.api.io.ResourceCollectionReaderBase;
import org.dkpro.core.api.resources.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;

public class WikipediaTextReader extends ResourceCollectionReaderBase {
	
	private String wikipediaInternalLinkMatcher = "\\[ \\[(?>[^\\]]+ \\| )?([^\\]]+) \\] \\]";
	private String wikipediaFloatTextMatcher = "\\{ \\{ (?>[^\\}]+ \\| )?([^\\}]+) \\} \\}";
	private String wikipediaLinkMatcher = "\\[ (\\S+ )?([^\\]]+) \\]";
	private String wikipediaMarkupCharacters = "['*=#]";
	
	@Override
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		Resource res = nextFile();
		
		try (InputStream is = CompressionUtils.getInputStream(res.getLocation(), res.getInputStream())) {
			String documentText = IOUtils.toString(is, Charsets.UTF_8);
			documentText = documentText.replaceAll(wikipediaInternalLinkMatcher, "");
			documentText = documentText.replaceAll(wikipediaFloatTextMatcher, "");
			documentText = documentText.replaceAll(wikipediaLinkMatcher, "");
			documentText = documentText.replaceAll(wikipediaMarkupCharacters, "");
			aCAS.setDocumentText(documentText);
		}
		
		initCas(aCAS, res);
	}
}
