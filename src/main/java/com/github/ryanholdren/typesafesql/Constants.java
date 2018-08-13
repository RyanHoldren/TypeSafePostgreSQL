package com.github.ryanholdren.typesafesql;

import java.util.regex.Pattern;
import static java.util.regex.Pattern.compile;

interface Constants {
	public static final Pattern PARAMETER_PATTERN = compile("(?<=\\(|,|\\s|^):[a-z][a-zA-Z0-9]*");
}
