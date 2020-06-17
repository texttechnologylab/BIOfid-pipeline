package org.biofid.pipeline.runner;

import com.google.common.base.Charsets;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.uima.UIMAException;
import org.biofid.gazetteer.models.StringGazetteerModel;
import org.texttechnologylab.annotation.type.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.lang.System.exit;

public class CreateClassificationSkipGramCorpus {
	public static String tsvDir;
	public static String classificationDir;
	private static String fileIncludePattern = "[+]*.xmi";
	
	
	public static void main(String[] args) throws IOException, UIMAException {
		Options options = new Options();
		options.addOption("h", "Print this message.");
		
		Option rootDirOption = new Option("r", "root", true,
				"Path to the root folder.");
		rootDirOption.setRequired(true);
		options.addOption(rootDirOption);
		
		Option taggedDirOption = new Option("t", "tagged", true,
				"Tagged TSV path if not {rootDir}/classes/biofid");
		taggedDirOption.setRequired(false);
		options.addOption(taggedDirOption);
		
		Option outputDirOption = new Option("o", "output", true,
				"TODO {rootDir}/classification/tagged");
		outputDirOption.setRequired(false);
		options.addOption(outputDirOption);
		
		try {
			DefaultParser defaultParser = new DefaultParser();
			CommandLine commandLine = defaultParser.parse(options, args);
			
			String rootDir = StringUtils.appendIfMissing(commandLine.getOptionValue("r"), "/");
			
			if (!commandLine.hasOption("t"))
				tsvDir = rootDir + "classes/biofid";
			else
				tsvDir = commandLine.getOptionValue("t");
			
			if (!commandLine.hasOption("o"))
				classificationDir = rootDir + "classification/tagged/";
			else
				classificationDir = commandLine.getOptionValue("o");
			
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
	
	private static void extractCoNLL() throws IOException {
		List<Class<? extends NamedEntity>> taggingClasses = Arrays.asList(
				Archaea.class,
				Bacteria.class,
				Chromista.class,
				Fungi.class,
				Lichen.class,
				Protozoa.class,
				Animal_Fauna.class,
				Plant_Flora.class,
				Viruses.class
		);
		
		final Logger logger = Logger.getLogger(CreateClassificationSkipGramCorpus.class);
		taggingClasses.stream().map(Class::getSimpleName).forEach(taggingType ->
				{
					try {
						logger.info("Processing " + taggingType);
						BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(classificationDir, taggingType + ".tsv"), Charsets.UTF_8);
						List<String> lines = Files.readAllLines(Paths.get(tsvDir, taggingType + ".tsv"), Charsets.UTF_8);
						for (int i = 0; i < lines.size(); i++) {
							String line = lines.get(i);
							String taxon = line.split("\\s+", 2)[1];
							if (taxon.split(" ").length <= 5) {
								System.out.printf("%d/%d\r", i + 1, lines.size());
								Set<String> skipGramsFromTaxon = StringGazetteerModel.getSkipGramsFromTaxon(taxon, true, 3, true, false);
								for (String entity : skipGramsFromTaxon) {
									bufferedWriter.write(String.format("__label__%s %s\n", taggingType, entity));
								}
							}
						}
						bufferedWriter.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
		);
	}
	
}
