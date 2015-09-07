package io.quantumdb.nemesis.structure.mysql55;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.Column;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Constraint;
import io.quantumdb.nemesis.structure.ForeignKey;
import io.quantumdb.nemesis.structure.Index;
import io.quantumdb.nemesis.structure.QueryBuilder;
import io.quantumdb.nemesis.structure.Table;
import io.quantumdb.nemesis.structure.Trigger;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
class MysqlTable implements Table {

	private final Connection connection;
	private final MysqlDatabase parent;
	private final String name;

	MysqlTable(Connection connection, MysqlDatabase parent, String name) {
		this.connection = connection;
		this.parent = parent;
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void rename(String newName) throws SQLException {
		execute(String.format("ALTER TABLE %s RENAME TO %s", name, newName));
	}

	@Override
	public MysqlDatabase getParent() {
		return parent;
	}

	@Override
	public List<Column> listColumns() throws SQLException {
		String query = "SHOW COLUMNS FROM " + name;

		List<Column> columns = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			log.debug(query);
			ResultSet resultSet = statement.executeQuery(query);

			while (resultSet.next()) {
				String columnName = resultSet.getString("Field");
				String expression = resultSet.getString("Default");
				boolean nullable = "YES".equalsIgnoreCase(resultSet.getString("Null"));
				String type = resultSet.getString("Type");
				boolean identity = resultSet.getString("Key").equals("PRI");
				boolean autoIncrement = resultSet.getString("Extra").contains("auto_increment");

				columns.add(new MysqlColumn(connection, this, columnName, expression, nullable, type,
						identity, autoIncrement));
			}
		}

		return columns;
	}

	@Override
	public Column addColumn(ColumnDefinition column) throws SQLException {
		if (column.isAutoIncrement() && listColumns().stream().filter(Column::isIdentity).count() > 0) {
			throw new UnsupportedOperationException("Mysql 5.5 does not support auto increment on non primary key.");
		}

		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("ALTER TABLE " + name);
		queryBuilder.append(" ADD " + column.getName() + " " + column.getType());

		if (!column.isNullable()) {
			queryBuilder.append(" NOT NULL");
		}

		if (column.isAutoIncrement()) {
			queryBuilder.append(" AUTO_INCREMENT");
		}
		else if (!Strings.isNullOrEmpty(column.getDefaultExpression())) {
			queryBuilder.append(" DEFAULT " + column.getDefaultExpression());
		}

		execute(queryBuilder.toString());

		MysqlColumn created = new MysqlColumn(connection, this, column);
		if (column.isIdentity()) {
			created.setIdentity(true);
		}

		return created;
	}

	@Override
	public List<Index> listIndices() throws SQLException {
		String query = "SHOW INDEXES FROM " + name;

		List<Index> indices = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {

			log.debug(query);
			ResultSet resultSet = statement.executeQuery(query);

			while (resultSet.next()) {
				String indexName = resultSet.getString("Key_name");
				boolean isUnique = !resultSet.getBoolean("Non_unique");
				boolean isPrimary = indexName.equals("PRIMARY");
				indices.add(new MysqlIndex(this, indexName, isUnique, isPrimary));
			}
		}

		return indices;
	}

	@Override
	public Index createIndex(String name, boolean unique, String... columnNames) throws SQLException {
		String columns = Joiner.on(',').join(columnNames);
		if (unique) {
			execute(String.format("CREATE UNIQUE INDEX %s ON %s (%s)", name, this.name, columns));
		}
		else {
			execute(String.format("CREATE INDEX %s ON %s (%s)", name, this.name, columns));
		}
		return new MysqlIndex(this, name, unique, false);
	}

	@Override
	public List<Constraint> listConstraints() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT column_name, constraint_name ")
				.append("FROM information_schema.key_column_usage ")
				.append("WHERE table_name = ?")
				.toString();

		List<Constraint> constraints = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, name);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String constraintName = resultSet.getString("constraint_name");
				String columnName = resultSet.getString("column_name");
				String constraintType = "CHECK";

				if (constraintName.equals("PRIMARY")) {
					constraintType = "PRIMARY";
				}

				constraints.add(new MysqlConstraint(this, constraintName, constraintType, columnName));
			}
		}
		return constraints;
	}

	@Override
	public Constraint createConstraint(String name, String type, String expression) throws SQLException {
		throw new UnsupportedOperationException("MySQL does not support constraints? WTF!");
	}

	@Override
	public List<ForeignKey> listForeignKeys() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT * ")
				.append("FROM information_schema.key_column_usage ")
				.append("WHERE table_schema = SCHEMA() AND table_name = ? AND referenced_table_name IS NOT NULL")
				.toString();

		List<ForeignKey> foreignKeys = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, name);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String constraintName = resultSet.getString("constraint_name");
				foreignKeys.add(new MysqlForeignKey(this, constraintName));
			}
		}
		return foreignKeys;
	}

	@Override
	public List<Trigger> listTriggers() throws SQLException {
		// TODO: Add support for listing triggers.
		return Lists.newArrayList();
	}

	@Override
	public ForeignKey addForeignKey(String constraint, String[] columns, String referencedTable, String[] referencedColumns)
			throws SQLException {

		execute(String.format("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", name, constraint,
				Joiner.on(',').join(columns), referencedTable, Joiner.on(',').join(referencedColumns)));

		return new MysqlForeignKey(this, constraint);
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("DROP TABLE %s", this.name));
	}

	private void execute(String query) throws SQLException {
		getParent().execute(query);
	}

}
