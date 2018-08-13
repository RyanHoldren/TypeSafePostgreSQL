package com.github.ryanholdren.typesafesql;

import java.util.Objects;
import java.util.Set;

public class PostgresField {

	private final String name;
	private final PostgresType type;

	public PostgresField(String name, PostgresType type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public PostgresType getType() {
		return type;
	}

	public String getJavaType() {
		return type. getJavaType();
	}

	public String getBoxedJavaType() {
		return type. getBoxedJavaType();
	}

	public Set<String> getImports() {
		return type. getImports();
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final PostgresField other = (PostgresField) obj;
		if (!Objects.equals(this.name, other.name)) {
			return false;
		}
		return true;
	}

}
