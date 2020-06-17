package org.biofid.pipeline.runner;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiReader;
import org.texttechnologylab.annotation.type.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.System.exit;

public class ExtractAnnotatedClassification {
	public static String annotatedDir;
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
				"Tagged XMI output path if not {rootDir}/xmi/annotated");
		taggedDirOption.setRequired(false);
		options.addOption(taggedDirOption);
		
		Option outputDirOption = new Option("o", "output", true,
				"Tagged CoNLL output path if not {rootDir}/conll/annotated");
		outputDirOption.setRequired(false);
		options.addOption(outputDirOption);
		
		try {
			DefaultParser defaultParser = new DefaultParser();
			CommandLine commandLine = defaultParser.parse(options, args);
			
			String rootDir = StringUtils.appendIfMissing(commandLine.getOptionValue("r"), "/");
			
			if (!commandLine.hasOption("t"))
				annotatedDir = rootDir + "xmi/annotated/";
			else
				annotatedDir = commandLine.getOptionValue("t");
			
			if (!commandLine.hasOption("o"))
				classificationDir = rootDir + "classification/annotated/";
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
		try {
			String[] annotatorBlacklist = {"0", "302904", "303228", "306320", "305718", "306513"};
			final CollectionReader reader = CollectionReaderFactory.createReader(XmiReader.class,
					XmiReader.PARAM_SOURCE_LOCATION, annotatedDir,
					XmiReader.PARAM_PATTERNS, fileIncludePattern,
					XmiReader.PARAM_LENIENT, true
			);
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
			
			Map<String, HashSet<String>> classMap = taggingClasses.stream().map(Class::getSimpleName).collect(Collectors.toMap(
					Function.identity(),
					name -> new HashSet<>()
			));
			JCas jCas = JCasFactory.createJCas();
			while (reader.hasNext()) {
				jCas.reset();
				reader.getNext(jCas.getCas());
				Pattern pattern = Pattern.compile("[^\\p{Alnum}\\.-]", Pattern.UNICODE_CHARACTER_CLASS);
				jCas.getViewIterator().forEachRemaining(viewCas -> {
							String viewName = StringUtils.substringAfterLast(viewCas.getViewName().trim(), "/");
							if (!viewName.isEmpty() && !ImmutableSet.copyOf(annotatorBlacklist).contains(viewName)) {
								for (Class<? extends NamedEntity> taggingClass : taggingClasses) {
									for (NamedEntity match : JCasUtil.select(viewCas, taggingClass)) {
										HashSet<String> classSet = classMap.get(taggingClass.getSimpleName());
										String entity = match.getCoveredText();
										entity = pattern.matcher(entity).replaceAll(" ");
										entity = entity.replaceAll("\\s+", " ").trim();
										classSet.add(entity);
									}
								}
							}
						}
				
				);
			}
			Map<String, BufferedWriter> fileMap = taggingClasses.stream().map(Class::getSimpleName).collect(Collectors.toMap(
					Function.identity(),
					name -> {
						try {
							return Files.newBufferedWriter(Paths.get(classificationDir, name + ".tsv"), Charsets.UTF_8);
						} catch (IOException e) {
							e.printStackTrace();
						}
						return null;
					}
			));
			for (Map.Entry<String, BufferedWriter> writerEntry : fileMap.entrySet()) {
				try {
					String taggingClass = writerEntry.getKey();
					BufferedWriter bufferedWriter = writerEntry.getValue();
					HashSet<String> strings = classMap.get(taggingClass);
					
					for (String entity : strings) {
						bufferedWriter.write(String.format("__label__%s %s\n", taggingClass, entity));
					}
					bufferedWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (UIMAException e) {
			e.printStackTrace();
		}
	}
	
}
