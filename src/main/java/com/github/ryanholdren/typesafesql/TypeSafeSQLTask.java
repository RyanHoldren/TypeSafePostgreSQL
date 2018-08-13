package com.github.ryanholdren.typesafesql;

import static com.github.ryanholdren.typesafesql.Constants.PARAMETER_PATTERN;
import com.github.ryanholdren.typesafesql.ImmutableJavaClassWriter;
import static com.github.ryanholdren.typesafesql.JavaClassWriter.LINE_BREAK;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.nio.file.Path;
import static java.nio.file.Files.newBufferedWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.RelativePath;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.logging.Logger;
import org.gradle.api.tasks.TaskAction;

class TypeSafeSQLTask extends DefaultTask implements Constants {

	private static final String DEFAULT_MIGRATION_DIRECTORY = "filesystem:src/main/resources/db/migration";

	private String sourceDirectory;
	private String destinationDirectory;
	private ConfigurableFileCollection migrationDirectories;
	private Map<String, String> migrationPlaceholders;
	private Map<String, String> schemas;

	public TypeSafeSQLTask(String defaultSourceDirectory, String defaultDestinationDirectory) {
		this.sourceDirectory = defaultSourceDirectory;
		this.destinationDirectory = defaultDestinationDirectory;
		this.destinationDirectory = DEFAULT_MIGRATION_DIRECTORY;
	}

	public TypeSafeSQLTask setSourceDirectory(String sourceDirectory) {
		this.sourceDirectory = sourceDirectory;
		return this;
	}

	public TypeSafeSQLTask setDestinationDirectory(String destinationDirectory) {
		this.destinationDirectory = destinationDirectory;
		return this;
	}

	public TypeSafeSQLTask setMigrationDirectory(ConfigurableFileCollection migrationDirectories) {
		this.migrationDirectories = migrationDirectories;
		return this;
	}

	public TypeSafeSQLTask setMigrationPlaceholders(Map<String, String> migrationPlaceholders) {
		this.migrationPlaceholders = migrationPlaceholders;
		return this;
	}

	public TypeSafeSQLTask setSchemas(Map<String, String> schemas) {
		this.schemas = schemas;
		return this;
	}

	@TaskAction
	public void createJavaFilesFromSQL() throws IOException {
		final Project project = getProject();
		final Gradle gradle = project.getGradle();
		gradle.addListener(this);
		final Logger logger = getLogger();
		logger.info(
			"Creating Java files from SQL files in '%s' and writing them to '%s'...",
			sourceDirectory,
			destinationDirectory
		);
		final File output = project.file(destinationDirectory);
		final FileTree files = project.files(sourceDirectory).getAsFileTree();
		if (files.isEmpty()) {
			logger.info("There are no files to be processed!");
		}
		final EmbeddedPostgres postgres = EmbeddedPostgres.builder().start();
		final DataSource dataSource = postgres.getPostgresDatabase();
		final Flyway flyway = new Flyway();
		flyway.setDataSource(dataSource);
		flyway.setLocations(Iterables.toArray(Iterables.transform(migrationDirectories, migrationDirectory -> {
			return "filesystem:" + migrationDirectory.getAbsolutePath();
		}), String.class));
		flyway.setPlaceholders(migrationPlaceholders);
		flyway.migrate();
		files.visit(details -> {
			if (details.isDirectory()) {
				return;
			}
			final String fileName = details.getName();
			if (fileName.startsWith("R__") || Pattern.compile("^V[0-9]+__").matcher(fileName).find()) {
				return;
			}
			final List<String> parts = Splitter.on('.').splitToList(fileName);
			final int indexOfLastPart = parts.size() - 1;
			if (indexOfLastPart == 0) {
				return;
			}
			final String extension = parts.get(indexOfLastPart);
			if ("sql".equalsIgnoreCase(extension) == false) {
				return;
			}
			final String className = parts.get(0);
			final RelativePath relative = details.getRelativePath();
			logger.info("Creating Java file from '%s'...", relative);
			final File sqlFile = details.getFile().getAbsoluteFile();
			final Path javaFile = relative.replaceLastName(className + ".java").getFile(output).toPath();
			final String path = details.getPath();
			final String namespace = path.substring(0, path.lastIndexOf('/')).replace('/', '.');
			try {
				final String sql = Files.toString(sqlFile, UTF_8);
				final ArrayList<String> parameterNames = new ArrayList<>();
				final StringBuffer buffer = new StringBuffer();
				final Matcher matcher = PARAMETER_PATTERN.matcher(sql);
				while (matcher.find()) {
					parameterNames.add(matcher.group().substring(1));
					matcher.appendReplacement(buffer, "?");
				}
				matcher.appendTail(buffer);
				final String jdbcSql = buffer.toString();
				try (Connection connection = dataSource.getConnection()) {
					try {
						final String schema = getSchemaFrom(sqlFile, sql);
						connection.createStatement().execute("SET search_path TO " + schemas.get(schema) + ';');
						final PreparedStatement statement = connection.prepareStatement(jdbcSql);
						final Parameters parameters = Parameters.from(parameterNames, statement.getParameterMetaData());
						final ResultColumns resultColumns = ResultColumns.from(statement.getMetaData());
						javaFile.getParent().toFile().mkdirs();
						try (final BufferedWriter writer = newBufferedWriter(javaFile)) {
							ImmutableJavaClassWriter
								.builder()
								.namespace(namespace)
								.className(className)
								.sql(sql)
								.parameters(parameters)
								.resultColumns(resultColumns)
								.build()
								.writeTo(writer);
						}
					} catch (SQLException exception) {
						throw new RuntimeException(sqlFile + " is not valid SQL!", exception);
					}
				}
			} catch (SQLException | IOException exception) {
				throw new RuntimeException(exception);
			}
		});
	}

	public static final String SCHEMA_PREFIX = "-- Schema: ";

	private static String getSchemaFrom(File sqlFile, String sql) {
		for (final String line : Splitter.on(LINE_BREAK).split(sql)) {
			if (line.startsWith(SCHEMA_PREFIX)) {
				return line.substring(SCHEMA_PREFIX.length());
			}
		}
		throw new NoSuchElementException(sqlFile + " does not specify a schema!");
	}

}
