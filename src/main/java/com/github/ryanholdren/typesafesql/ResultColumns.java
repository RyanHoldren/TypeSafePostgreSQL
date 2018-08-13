package com.github.ryanholdren.typesafesql;

import static com.github.ryanholdren.typesafesql.ResultColumns.None.NONE;
import java.sql.ResultSetMetaData;
import static java.sql.ResultSetMetaData.columnNullable;
import java.sql.SQLException;
import java.util.Arrays;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.singleton;
import java.util.Iterator;

public interface ResultColumns extends Iterable<PostgresField> {

	public static ResultColumns from(ResultSetMetaData results) throws SQLException {
		if (results == null) {
			return NONE;
		}
		final int numberOfColumns = results.getColumnCount();
		final PostgresField[] columns = new PostgresField[numberOfColumns];
		for (int index = 0; index < numberOfColumns; index ++) {
			final int offset = index + 1;
			final String postgresType = results.getColumnTypeName(offset);
			final boolean isNullable = results.isNullable(offset) == columnNullable;
			columns[index] = new PostgresField(results.getColumnLabel(offset), PostgresType.from(postgresType, isNullable));
		}
		if (numberOfColumns == 1) {
			return new One(columns[0]);
		}
		return new Many(columns);
	}

	interface Visitor<E extends Exception> {
		void visit(None columns) throws E;
		void visit(One columns) throws E;
		void visit(Many columns) throws E;
	}

	<E extends Exception> void accept(Visitor<E> visitor) throws E;

	enum None implements ResultColumns {

		NONE;

		@Override
		public <E extends Exception> void accept(Visitor<E> visitor) throws E {
			visitor.visit(this);
		}

		@Override
		public Iterator<PostgresField> iterator() {
			return emptyIterator();
		}

	}

	static class One implements ResultColumns {

		private final PostgresField column;

		private One(PostgresField column) {
			this.column = column;
		}

		public PostgresField getColumn() {
			return column;
		}

		@Override
		public <E extends Exception> void accept(Visitor<E> visitor) throws E {
			visitor.visit(this);
		}

		@Override
		public Iterator<PostgresField> iterator() {
			return singleton(column).iterator();
		}

	}

	static class Many implements ResultColumns {

		private final PostgresField[] columns;

		private Many(PostgresField[] columns) {
			this.columns = columns;
		}

		@Override
		public <E extends Exception> void accept(Visitor<E> visitor) throws E {
			visitor.visit(this);
		}

		@Override
		public Iterator<PostgresField> iterator() {
			return Arrays.asList(columns).iterator();
		}

	}

}
