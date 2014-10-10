package io.quantumdb.nemesis.structure.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.quantumdb.nemesis.structure.Column;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Constraint;
import io.quantumdb.nemesis.structure.Index;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class PostgresColumn implements Column {

	private final Connection connection;
	private final PostgresTable parent;

	private String name;
	private String defaultExpression;
	private boolean nullable;
	private String type;
	private boolean identity;
	private boolean autoIncrement;

	PostgresColumn(Connection connection, PostgresTable parent, ColumnDefinition column) {
		this(connection, parent, column.getName(), column.getDefaultExpression(), column.isNullable(),
				column.getType(), column.isIdentity(), column.isAutoIncrement());
	}

	PostgresColumn(Connection connection, PostgresTable parent, String name, String defaultExpression,
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
		execute(String.format("ALTER TABLE %s RENAME %s TO %s", parent.getName(), name, newName));
		this.name = newName;
	}

	@Override
	public PostgresTable getParent() {
		return parent;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public void setType(String newType) throws SQLException {
		execute(String.format("ALTER TABLE %s ALTER %s TYPE %s", parent.getName(), name, newType));
		this.type = newType;
	}

	@Override
	public boolean isNullable() {
		return nullable;
	}

	@Override
	public void setNullable(boolean isNullable) throws SQLException {
		String action = isNullable ? "DROP" : "SET";
		execute(String.format("ALTER TABLE %s ALTER %s %s NOT NULL", parent.getName(), name, action));
		this.nullable = isNullable;
	}

	@Override
	public String getDefaultExpression() {
		return defaultExpression;
	}

	@Override
	public void setDefaultExpression(String newExpression) throws SQLException {
		if (Strings.isNullOrEmpty(newExpression)) {
			execute(String.format("ALTER TABLE %s ALTER %s DROP DEFAULT", parent.getName(), name));
		}
		else {
			execute(String.format("ALTER TABLE %s ALTER %s SET DEFAULT %s", parent.getName(), name, newExpression));
		}
		this.defaultExpression = newExpression;
	}

	@Override
	public boolean isIdentity() {
		return identity;
	}

	@Override
	public void setIdentity(boolean identity) throws SQLException {
		if (identity) {
			List<String> identityColumns = getParent().listColumns().stream()
					.filter(c -> c.isIdentity())
					.map(c -> c.getName())
					.collect(Collectors.toList());

			Optional<Constraint> currentPrimaryKeyConstraint = getParent().listConstraints().stream()
					.filter(c -> c.getType().equals("PRIMARY KEY"))
					.findFirst();

			if (currentPrimaryKeyConstraint.isPresent()) {
				currentPrimaryKeyConstraint.get().drop();
			}

			Optional<Index> currentPrimaryKeyIndex = getParent().listIndices().stream()
					.filter(i -> i.isPrimary())
					.findFirst();

			if (currentPrimaryKeyIndex.isPresent()) {
				currentPrimaryKeyIndex.get().drop();
			}

			identityColumns.add(name);
			execute(String.format("ALTER TABLE %s ADD PRIMARY KEY (%s)", getParent().getName(),
					Joiner.on(',').join(identityColumns)));
		}
		else {
			throw new UnsupportedOperationException();
		}
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

}
