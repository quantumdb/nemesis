package io.quantumdb.nemesis.structure;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ColumnDefinition {

	private final String name;
	private final String type;

	private String defaultExpression;
	private boolean nullable = true;
	private boolean identity = false;
	private boolean autoIncrement = false;

	public ColumnDefinition(String name, String type) {
		this.name = name;
		this.type = type;
	}

	public ColumnDefinition setDefaultExpression(String expression) {
		this.defaultExpression = expression;
		return this;
	}

	public ColumnDefinition isNullable(boolean nullable) {
		this.nullable = nullable;
		return this;
	}

	public ColumnDefinition isIdentity(boolean identity) {
		this.identity = identity;
		return this;
	}

	public ColumnDefinition isAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
		return this;
	}

}
