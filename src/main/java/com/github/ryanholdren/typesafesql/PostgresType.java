package com.github.ryanholdren.typesafesql;

import com.google.common.collect.ImmutableSet;
import java.sql.SQLException;
import java.util.Set;

public enum PostgresType {

	BIG_DECMIAL("BigDecimal", "java.math.BigDecimal"),
	BOOLEAN("boolean") {
		@Override
		public String getBoxedJavaType() {
			return "Boolean";
		}
	},
	BYTE_ARRAY("byte[]"),
	CHARACTER("char") {
		@Override
		public String getBoxedJavaType() {
			return "Character";
		}
	},
	DOUBLE("double") {
		@Override
		public String getBoxedJavaType() {
			return "Double";
		}
	},
	FLOAT("float") {
		@Override
		public String getBoxedJavaType() {
			return "Float";
		}
	},
	INTEGER("int") {
		@Override
		public String getBoxedJavaType() {
			return "Integer";
		}
	},
	LOCAL_DATE("LocalDate", "java.time.LocalDate"),
	LOCAL_DATE_TIME("LocalDateTime", "java.time.LocalDateTime"),
	LOCAL_TIME("LocalTime", "java.time.LocalTime"),
	LONG("long") {
		@Override
		public String getBoxedJavaType() {
			return "Long";
		}
	},
	OFFSET_DATE_TIME("OffsetDateTime", "java.time.OffsetDateTime"),
	OFFSET_TIME("OffsetTime", "java.time.OffsetTime"),
	OPTIONAL_BIG_DECMIAL("Optional<BigDecimal>", "java.util.Optional", "java.math.BigDecimal"),
	OPTIONAL_BOOLEAN("Optional<Boolean>", "java.util.Optional"),
	OPTIONAL_BYTE_ARRAY("Optional<byte[]>", "java.util.Optional"),
	OPTIONAL_CHARACTER("Optional<Character>", "java.util.Optional"),
	OPTIONAL_DOUBLE("OptionalDouble", "java.util.OptionalDouble"),
	OPTIONAL_FLOAT("Optional<Float>", "java.util.Optional"),
	OPTIONAL_INTEGER("OptionalInt", "java.util.OptionalInt"),
	OPTIONAL_LOCAL_DATE("Optional<LocalDate>", "java.util.Optional", "java.time.LocalDate"),
	OPTIONAL_LOCAL_DATE_TIME("Optional<LocalDateTime>", "java.util.Optional", "java.time.LocalDateTime"),
	OPTIONAL_LOCAL_TIME("Optional<LocalTime>", "java.util.Optional", "java.time.LocalTime"),
	OPTIONAL_LONG("OptionalLong", "java.util.OptionalLong"),
	OPTIONAL_OFFSET_DATE_TIME("Optional<OffsetDateTime>", "java.util.Optional", "java.time.OffsetDateTime"),
	OPTIONAL_OFFSET_TIME("Optional<OffsetTime>", "java.util.Optional", "java.time.OffsetTime"),
	OPTIONAL_SHORT("Optional<Short>", "java.util.Optional"),
	OPTIONAL_STRING("Optional<String>", "java.util.Optional"),
	OPTIONAL_UUID("Optional<UUID>", "java.util.Optional", "java.util.UUID"),
	SHORT("short") {
		@Override
		public String getBoxedJavaType() {
			return "Short";
		}
	},
	STRING("String"),
	UUID("UUID", "java.util.UUID");

	public static PostgresType from(String postgresType, boolean isNullable) throws SQLException {
		switch (postgresType) {
			case "bool":
				if (isNullable) {
					return OPTIONAL_BOOLEAN;
				} else {
					return BOOLEAN;
				}
			case "int2":
				if (isNullable) {
					return OPTIONAL_SHORT;
				} else {
					return SHORT;
				}
			case "int4":
				if (isNullable) {
					return OPTIONAL_INTEGER;
				} else {
					return INTEGER;
				}
			case "int8":
				if (isNullable) {
					return OPTIONAL_LONG;
				} else {
					return LONG;
				}
			case "float4":
				if (isNullable) {
					return OPTIONAL_FLOAT;
				} else {
					return FLOAT;
				}
			case "float8":
				if (isNullable) {
					return OPTIONAL_DOUBLE;
				} else {
					return DOUBLE;
				}
			case "numeric":
				if (isNullable) {
					return OPTIONAL_BIG_DECMIAL;
				} else {
					return BIG_DECMIAL;
				}
			case "char":
				if (isNullable) {
					return OPTIONAL_CHARACTER;
				} else {
					return CHARACTER;
				}
			case "text":
			case "name":
			case "varchar":
				if (isNullable) {
					return OPTIONAL_STRING;
				} else {
					return STRING;
				}
			case "uuid":
				if (isNullable) {
					return OPTIONAL_UUID;
				} else {
					return UUID;
				}
			case "date":
				if (isNullable) {
					return OPTIONAL_LOCAL_DATE;
				} else {
					return LOCAL_DATE;
				}
			case "time":
				if (isNullable) {
					return OPTIONAL_LOCAL_TIME;
				} else {
					return LOCAL_TIME;
				}
			case "timetz":
				if (isNullable) {
					return OPTIONAL_OFFSET_TIME;
				} else {
					return OFFSET_TIME;
				}
			case "timestamp":
				if (isNullable) {
					return OPTIONAL_LOCAL_DATE_TIME;
				} else {
					return LOCAL_DATE_TIME;
				}
			case "timestamptz":
				if (isNullable) {
					return OPTIONAL_OFFSET_DATE_TIME;
				} else {
					return OFFSET_DATE_TIME;
				}
			case "bytea":
				if (isNullable) {
					return OPTIONAL_BYTE_ARRAY;
				} else {
					return BYTE_ARRAY;
				}
		}
		if (isNullable) {
			return OPTIONAL_STRING;
		} else {
			return STRING;
		}
	}

	private final String javaType;
	private final ImmutableSet<String> imports;

	private PostgresType(String javaType, String ... imports) {
		this.javaType = javaType;
		this.imports = ImmutableSet.copyOf(imports);
	}

	public String getJavaType() {
		return javaType;
	}

	public String getBoxedJavaType() {
		return javaType;
	}

	public Set<String> getImports() {
		return imports;
	}

}
