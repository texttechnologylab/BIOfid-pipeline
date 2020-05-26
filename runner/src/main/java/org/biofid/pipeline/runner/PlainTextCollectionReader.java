package org.biofid.pipeline.runner;

import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.dkpro.core.api.io.ResourceCollectionReaderBase;
import org.dkpro.core.api.resources.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;

public class PlainTextCollectionReader extends ResourceCollectionReaderBase {
	@Override
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		Resource res = nextFile();
		
		try (InputStream is = CompressionUtils.getInputStream(res.getLocation(), res.getInputStream())) {
			aCAS.setDocumentText(IOUtils.toString(is, Charsets.UTF_8));
		}
		
		initCas(aCAS, res);
	}
}
