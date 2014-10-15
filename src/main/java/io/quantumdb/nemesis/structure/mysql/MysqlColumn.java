package io.quantumdb.nemesis.structure.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.Column;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Constraint;
import io.quantumdb.nemesis.structure.Index;
import io.quantumdb.nemesis.structure.QueryBuilder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
class MysqlColumn implements Column {

	private final Connection connection;
	private final MysqlTable parent;

	private String name;
	private String defaultExpression;
	private boolean nullable;
	private String type;
	private boolean identity;
	private boolean autoIncrement;

	MysqlColumn(Connection connection, MysqlTable parent, ColumnDefinition column) {
		this(connection, parent, column.getName(), column.getDefaultExpression(), column.isNullable(),
				column.getType(), column.isIdentity(), column.isAutoIncrement());
	}

	MysqlColumn(Connection connection, MysqlTable parent, String name, String defaultExpression,
			boolean nullable, String dataType, boolean identityColumn, boolean autoIncrement) {

		this.connection = connection;
		this.parent = parent;
		this.name = name;
		this.defaultExpression = defaultExpression;
		this.nullable = nullable;
		this.type = dataType;
		this.identity = identityColumn;
		this.autoIncrement = autoIncrement;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void rename(String newName) throws SQLException {
		execute(String.format("ALTER TABLE %s CHANGE COLUMN %s %s", parent.getName(), name,
				getDefinition(newName, type, nullable, autoIncrement, defaultExpression)));
		this.name = newName;
	}

	@Override
	public MysqlTable getParent() {
		return parent;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public void setType(String newType) throws SQLException {
		execute(String.format("ALTER TABLE %s MODIFY COLUMN %s", parent.getName(),
				getDefinition(name, newType, nullable, autoIncrement, defaultExpression)));

		this.type = newType;
	}

	@Override
	public boolean isNullable() {
		return nullable;
	}

	@Override
	public void setNullable(boolean isNullable) throws SQLException {
		execute(String.format("ALTER TABLE %s MODIFY COLUMN %s", parent.getName(),
				getDefinition(name, type, isNullable, autoIncrement, defaultExpression)));

		this.nullable = isNullable;
	}

	@Override
	public String getDefaultExpression() {
		return defaultExpression;
	}

	@Override
	public void setDefaultExpression(String newExpression) throws SQLException {
		if (Strings.isNullOrEmpty(newExpression)) {
			newExpression = null;
		}

		execute(String.format("ALTER TABLE %s MODIFY COLUMN %s", parent.getName(),
				getDefinition(name, type, nullable, autoIncrement, newExpression)));

		this.defaultExpression = newExpression;
	}

	@Override
	public boolean isIdentity() {
		return identity;
	}

	@Override
	public void setIdentity(boolean identity) throws SQLException {
		List<String> identityColumns = getParent().listColumns().stream()
				.filter(c -> c.isIdentity())
				.map(c -> c.getName())
				.collect(Collectors.toList());

		if (identity) {
			identityColumns.add(name);
		}

		execute(String.format("ALTER TABLE %s DROP PRIMARY KEY, ADD PRIMARY KEY(%s);", getParent().getName(),
				Joiner.on(',').join(identityColumns)));

		this.identity = identity;
	}

	@Override
	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP COLUMN %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}

	private String getDefinition(String name, String type, boolean nullable, boolean autoIncrement,
			String defaultExpression) {

		QueryBuilder queryBuilder = new QueryBuilder();

		queryBuilder.append(name + " " + type);

		if (!nullable) {
			queryBuilder.append(" NOT NULL");
		}

		if (autoIncrement) {
			queryBuilder.append(" AUTO_INCREMENT");
		}
		else if (!Strings.isNullOrEmpty(defaultExpression)) {
			if (!defaultExpression.endsWith(")") && !defaultExpression.endsWith("'")) {
				defaultExpression = "'" + defaultExpression + "'";
			}

			queryBuilder.append(" DEFAULT " + defaultExpression);
		}

		return queryBuilder.toString();
	}

}
