package io.quantumdb.nemesis.structure;

public class QueryBuilder {

	private final StringBuilder builder;

	public QueryBuilder() {
		this.builder = new StringBuilder();
	}

	public QueryBuilder append(String part) {
		if (builder.length() > 0) {
			builder.append(" ");
		}
		builder.append(part.trim());
		return this;
	}

	@Override
	public String toString() {
		return builder.toString();
	}

}
