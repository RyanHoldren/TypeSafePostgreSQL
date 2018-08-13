package com.github.ryanholdren.typesafesql;

import com.github.ryanholdren.typesafesql.ResultColumns.Many;
import com.github.ryanholdren.typesafesql.ResultColumns.None;
import com.github.ryanholdren.typesafesql.ResultColumns.One;
import com.github.ryanholdren.typesafesql.ResultColumns.Visitor;
import static com.google.common.base.CharMatcher.whitespace;
import com.google.common.base.Splitter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static java.util.regex.Pattern.compile;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;

@Immutable
@Style(stagedBuilder = true)
public interface JavaClassWriter extends Constants {

	public static final Pattern PRECEDING_WHITESPACE = Pattern.compile("^\\s*");

	public static String escape(String line) {
		return line.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	public static String findPrecedingWhitespaceIn(String line) {
		final Matcher matcher = PRECEDING_WHITESPACE.matcher(line);
		if (matcher.find()) {
			return matcher.group();
		}
		throw new IllegalStateException("Should always match something, even if it's an empty string!");
	}

	public static String capitalize(String word) {
		return Character.toUpperCase(word.charAt(0)) + word.substring(1);
	}

	public static String uncapitalize(String word) {
		return Character.toLowerCase(word.charAt(0)) + word.substring(1);
	}

	String getNamespace();
	String getClassName();
	String getSql();
	Parameters getParameters();
	ResultColumns getResultColumns();

	default void writeTo(BufferedWriter writer) throws IOException {
		writeTo(new AutoIndentingWriter(writer));
	}

	default void writeTo(AutoIndentingWriter writer) throws IOException {
		writeNamespaceTo(writer);
		writeImportsTo(writer);
		writeStartOfClassTo(writer);
		writeSQLConstantTo(writer);
		writeParametersTo(writer);
		writeResultTo(writer);
		writePgAsyncMethodTo(writer);
		writeEndOfClassTo(writer);
	}

	default void writeNamespaceTo(AutoIndentingWriter writer) throws IOException {
		writer.writeLine("package ", getNamespace(), ";");
		writer.writeEmptyLine();
	}

	default void writeImportsTo(AutoIndentingWriter writer) throws IOException {
		final TreeSet<String> imports = new TreeSet<>();
		for (final PostgresField parameter : getParameters()) {
			imports.addAll(parameter.getImports());
		}
		for (final PostgresField resultColumn : getResultColumns()) {
			imports.addAll(resultColumn.getImports());
		}
		imports.add("static java.lang.String.join");
		imports.add("static java.lang.System.lineSeparator");
		imports.add("com.github.pgasync.QueryExecutor");
		imports.add("org.immutables.value.Value.Immutable");
		imports.add("org.immutables.value.Value.Style");
		imports.add("org.immutables.value.Value.Enclosing");
		imports.add("java.util.function.IntFunction");
		imports.add("static rx.RxReactiveStreams.toPublisher");
		imports.add("reactor.core.publisher.Flux");
		imports.add("reactor.core.publisher.Mono");
		for (String classNameOfImport : imports) {
			writer.writeLine("import ", classNameOfImport, ';');
		}
		writer.writeEmptyLine();
	}

	default void writeStartOfClassTo(AutoIndentingWriter writer) throws IOException {
		writer.writeLine("public interface ", getClassName(), " {");
		writer.writeEmptyLine();
	}

	default void writeSQLConstantTo(AutoIndentingWriter writer) throws IOException {
		final Iterator<String> iterator = getLinesOfSQL().iterator();
		if (iterator.hasNext()) {
			if (hasParameters()) {
				writer.writeLine("public static String getSql(IntFunction<String> variable) {");
				writer.writeLine("return join (");
			} else {
				writer.writeLine("public static final String SQL = join(");
			}
			writer.write("lineSeparator()");
			int counter = 0;
			while (true) {
				String line = iterator.next();
				if (line == null) {
					break;
				}
				if (line.startsWith("--") || isEntirelyWhitespace(line)) {
					if (iterator.hasNext()) {
						continue;
					} else {
						break;
					}
				}
				writer.writeLine(',');
				final String precedingWhitespace = findPrecedingWhitespaceIn(line);
				writer.write(precedingWhitespace);
				writer.write('"');
				final String trimmed = line.trim();
				final Matcher parameterMatcher = PARAMETER_PATTERN.matcher(trimmed);
				if (parameterMatcher.find()) {
					int end = 0;
					do {
						final int start = parameterMatcher.start();
						final String before = trimmed.substring(end, start);
						writer.write(escape(before));
						writer.write('"', " + variable.apply(", counter ++, ") + ", '"');
						end = parameterMatcher.end();
					} while (parameterMatcher.find());
					writer.write(escape(trimmed.substring(end, trimmed.length())));
				} else {
					writer.write(escape(trimmed));
				}
				writer.write('"');
				if (iterator.hasNext()) {
					continue;
				} else {
					writer.writeLine();
					break;
				}
			}
			writer.writeLine(");");
			if (hasParameters()) {
				writer.writeLine("}");
			}
			writer.writeEmptyLine();
		}
	}

	public static final Pattern LINE_BREAK = compile("\\r?\\n");

	default Iterable<String> getLinesOfSQL() {
		return Splitter.on(LINE_BREAK).split(getSql());
	}

	default boolean isEntirelyWhitespace(String string) {
		return whitespace().matchesAllOf(string);
	}

	default void writeParametersTo(AutoIndentingWriter writer) throws IOException {
		if (hasParameters()) {
			writer.writeLine("public interface ", getClassName(), "Parameters {");
			writer.writeEmptyLine();
			writeParametersGettersTo(writer);
			writer.writeEmptyLine();
			writeParametersToArrayTo(writer);
			writer.writeEmptyLine();
			writer.writeLine("}");
			writer.writeEmptyLine();
		}
	}

	default void writeParametersGettersTo(AutoIndentingWriter writer) throws IOException {
		for (final PostgresField parameter : getParameters().unique()) {
			writer.writeLine(parameter.getJavaType(), " get" + capitalize(parameter.getName()) + "();");
		}
	}

	default void writeParametersToArrayTo(AutoIndentingWriter writer) throws IOException {
		writer.writeLine("default Object[] toArray() {");
		writer.writeLine("return new Object[] {");
		for (final String name : getParameterNames()) {
			writer.writeLine("get" + capitalize(name) + "(),");
		}
		writer.writeLine("};");
		writer.writeLine("}");
	}

	default boolean hasParameters() {
		return getParameterNames().iterator().hasNext();
	}

	default String getFirstParameterName() {
		for (final String parameterName : getParameterNames()) {
			return parameterName;
		}
		throw new UnsupportedOperationException("This SQL file has no parameters!");
	}

	default Iterable<String> getParameterNames() {
		final Matcher matcher = PARAMETER_PATTERN.matcher(getSql());
		return () -> new Iterator<String>() {

			@Override
			public boolean hasNext() {
				return matcher.find();
			}

			@Override
			public String next() {
				return matcher.group().substring(1);
			}

		};
	}

	default void writeResultTo(AutoIndentingWriter writer) throws IOException {
		getResultColumns().accept(new Visitor() {

			@Override
			public void visit(None columns) {
				return;
			}

			@Override
			public void visit(One columns) {
				return;
			}

			@Override
			public void visit(Many columns) throws IOException {
				writer.writeLine("@Immutable");
				writer.writeLine("@Style(stagedBuilder = true)");
				writer.write("public interface ", getResultClassName());
				final Iterator<String> interfaces = getResultInterfaces();
				if (interfaces.hasNext()) {
					writer.write(" extends ");
					while (true) {
						writer.write(interfaces.next());
						if (interfaces.hasNext()) {
							writer.write(", ");
						} else {
							break;
						}
					}
				}
				writer.writeLine(" {");
				for (final PostgresField column : columns) {
					writer.writeLine(column.getJavaType(), " get" + capitalize(column.getName()) + "();");
				}
				writer.writeLine("}");
				writer.writeEmptyLine();
			}

		});
	}

	public static final String INTERFACE_PREFIX = "-- Implements: ";

	default Iterator<String> getResultInterfaces() {
		final ArrayList<String> interfaces = new ArrayList<>();
		for (final String line : getLinesOfSQL()) {
			if (line.startsWith(INTERFACE_PREFIX)) {
				final String interfaze = line.substring(INTERFACE_PREFIX.length());
				interfaces.add(interfaze);
			}
		}
		return interfaces.iterator();
	}

	default void writePgAsyncMethodTo(AutoIndentingWriter writer) throws IOException {
		final String methodName = uncapitalize(getClassName());
		writer.writeLine("public interface PgAsync extends ", getClassName(), " {");
		writer.writeEmptyLine();
		writer.writeLine("QueryExecutor getQueryExecutor();");
		writer.writeEmptyLine();
		if (hasParameters()) {
			writer.writeLine("public static final String SQL = getSql(index -> \"$\" + index);");
			writer.writeEmptyLine();
			writer.writeLine("@Immutable");
			writer.writeLine("@Style(stagedBuilder = true, init = \"with*\")");
			writer.writeLine("public interface BoundPgAsync", getClassName(), "Parameters extends ", getClassName(), "Parameters {");
			writer.writeEmptyLine();
			writer.writeLine("PgAsync getPgAsync();");
			writer.writeEmptyLine();
			getResultColumns().accept(new Visitor<IOException>() {

					@Override
					public void visit(None columns) throws IOException {
						writer.writeLine("default Mono<Void> execute() {");
					}

					@Override
					public void visit(One column) throws IOException {
						writer.writeLine("default Flux<", column.getColumn().getBoxedJavaType(), "> execute() {");
					}

					@Override
					public void visit(Many columns) throws IOException {
						writer.writeLine("default Flux<", getResultClassName(), "> execute() {");
					}

			});
			writer.writeLine("return getPgAsync().", methodName, "(this);");
			writer.writeLine("}");
			writer.writeEmptyLine();
			writer.writeLine("}");
			writer.writeEmptyLine();
			writer.writeLine("default ImmutableBoundPgAsync", getClassName(), "Parameters.", capitalize(getFirstParameterName()),"BuildStage ", methodName, "() {");
			writer.writeLine("return ImmutableBoundPgAsync", getClassName(), "Parameters.builder().withPgAsync(this);");
			writer.writeLine("}");
			writer.writeEmptyLine();
		}
		getResultColumns().accept(new Visitor<IOException>() {

				@Override
				public void visit(None columns) throws IOException {
					if (hasParameters()) {
						writer.writeLine("default Mono<Void> ", methodName, "(", getClassName(), "Parameters parameters) {");
						writer.writeLine("return Mono.from(toPublisher(getQueryExecutor().querySet(SQL, parameters.toArray()))).then();");
						writer.writeLine("}");
					} else {
						writer.writeLine("default Mono<Void> ", methodName, "() {");
						writer.writeLine("return Mono.from(toPublisher(getQueryExecutor().querySet(SQL))).then();");
						writer.writeLine("}");
					}
				}

				@Override
				public void visit(One column) throws IOException {
					if (hasParameters()) {
						writer.writeLine("default Flux<", column.getColumn().getBoxedJavaType(), "> ", methodName, '(', getClassName(), "Parameters parameters", ") {");
						writer.writeLine("return Flux.from(toPublisher(getQueryExecutor().queryRows(SQL, parameters.toArray()))).map(row -> {");
						writer.writeLine("return null;");
						writer.writeLine("});");
						writer.writeLine("}");
					} else {
						writer.writeLine("default Flux<", column.getColumn().getBoxedJavaType(), "> ", methodName, "() {");
						writer.writeLine("return Flux.from(toPublisher(getQueryExecutor().queryRows(SQL))).map(row -> {");
						writer.writeLine("return null;");
						writer.writeLine("});");
						writer.writeLine("}");
					}
				}

				@Override
				public void visit(Many columns) throws IOException {
					if (hasParameters()) {
						writer.writeLine("default Flux<", getResultClassName(), "> ", methodName, '(', getClassName(), "Parameters parameters", ") {");
						writer.writeLine("return Flux.from(toPublisher(getQueryExecutor().queryRows(SQL, parameters.toArray()))).map(row -> {");
						writer.writeLine("return null;");
						writer.writeLine("});");
						writer.writeLine("}");
					} else {
						writer.writeLine("default Flux<", getResultClassName(), "> ", methodName, "() {");
						writer.writeLine("return Flux.from(toPublisher(getQueryExecutor().queryRows(SQL))).map(row -> {");
						writer.writeLine("return null;");
						writer.writeLine("});");
						writer.writeLine("}");
					}
				}

		});
		writer.writeEmptyLine();
		writer.writeLine("}");
		writer.writeEmptyLine();
	}

	public static final String RESULT_CLASS_PREFIX = "-- Result Class: ";

	default String getResultClassName() {
		for (final String line : getLinesOfSQL()) {
			if (line.startsWith(RESULT_CLASS_PREFIX)) {
				return line.substring(RESULT_CLASS_PREFIX.length());
			}
		}
		return getClassName() + "Result";
	}

	default void writeEndOfClassTo(AutoIndentingWriter writer) throws IOException {
		writer.writeLine("}");
	}

}
