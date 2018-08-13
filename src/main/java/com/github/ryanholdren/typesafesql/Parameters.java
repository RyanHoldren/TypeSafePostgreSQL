package com.github.ryanholdren.typesafesql;

import java.sql.ParameterMetaData;
import static java.sql.ParameterMetaData.parameterNullable;
import java.sql.SQLException;
import static java.util.Arrays.asList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

public class Parameters implements Iterable<PostgresField> {

	public static Parameters from(List<String> names, ParameterMetaData parameters) throws SQLException {
		final int numberOfParameters = parameters.getParameterCount();
		final PostgresField[] fields = new PostgresField[numberOfParameters];
		for (int index = 0; index < numberOfParameters; index ++) {
			final int offset = index + 1;
			final String postgresType = parameters.getParameterTypeName(offset);
			final boolean isNullable = parameters.isNullable(offset) == parameterNullable;
			fields[index] = new PostgresField(names.get(index), PostgresType.from(postgresType, isNullable));
		}
		return new Parameters(fields);
	}

	private final PostgresField[] fields;

	private Parameters(PostgresField[] fields) {
		this.fields = fields;
	}

	public Iterable<PostgresField> unique() {
		return new LinkedHashSet<>(asList(fields));
	}

	@Override
	public Iterator<PostgresField> iterator() {
		return asList(fields).iterator();
	}

}
