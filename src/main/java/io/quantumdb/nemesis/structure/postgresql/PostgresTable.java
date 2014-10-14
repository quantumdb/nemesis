package io.quantumdb.nemesis.structure.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.Column;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Constraint;
import io.quantumdb.nemesis.structure.ForeignKey;
import io.quantumdb.nemesis.structure.Index;
import io.quantumdb.nemesis.structure.QueryBuilder;
import io.quantumdb.nemesis.structure.Sequence;
import io.quantumdb.nemesis.structure.Table;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
class PostgresTable implements Table {

	private final Connection connection;
	private final PostgresDatabase parent;
	private final String name;

	PostgresTable(Connection connection, PostgresDatabase parent, String name) {
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
	public PostgresDatabase getParent() {
		return parent;
	}

	private List<String> listPrimaryKeyColumns() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT ")
				.append("  pg_attribute.attname AS name ")
				.append("FROM pg_index, pg_class, pg_attribute, pg_namespace ")
				.append("WHERE ")
				.append("  nspname = 'public' AND ")
				.append("  pg_class.oid = '" + name + "'::regclass AND ")
				.append("  indrelid = pg_class.oid AND ")
				.append("  pg_class.relnamespace = pg_namespace.oid AND ")
				.append("  pg_attribute.attrelid = pg_class.oid AND ")
				.append("  pg_attribute.attnum = any(pg_index.indkey) ")
				.append(" AND indisprimary")
				.toString();

		List<String> primaryKeyColumns = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {

			log.debug(query);
			ResultSet resultSet = statement.executeQuery(query);

			while (resultSet.next()) {
				String name = resultSet.getString("name");
				primaryKeyColumns.add(name);
			}
		}
		catch (SQLException e) {
			// Do nothing...
		}

		return primaryKeyColumns;
	}

	@Override
	public List<Column> listColumns() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT * ")
				.append("FROM information_schema.columns ")
				.append("WHERE table_schema = ? AND table_name = ? ")
				.append("ORDER BY ordinal_position ASC")
				.toString();

		List<String> primaryKeyColumns = listPrimaryKeyColumns();
		List<Sequence> sequences = parent.listSequences();
		List<String> sequenceNames = sequences
				.stream()
				.map(seq -> seq.getName())
				.collect(Collectors.toList());

		List<Column> columns = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, "public");
			statement.setString(2, name);

			log.debug(query);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String columnName = resultSet.getString("column_name");
				String expression = resultSet.getString("column_default");
				boolean nullable = "yes".equalsIgnoreCase(resultSet.getString("is_nullable"));
				String type = resultSet.getString("data_type");
				boolean identity = primaryKeyColumns.contains(columnName);
				boolean autoIncrement = sequenceNames.contains(name + "_" + columnName + "_seq");

				columns.add(new PostgresColumn(connection, this, columnName, expression, nullable, type,
						identity, autoIncrement));
			}
		}

		return columns;
	}

	@Override
	public Column addColumn(ColumnDefinition column) throws SQLException {
		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("ALTER TABLE " + name);
		queryBuilder.append(" ADD " + column.getName() + " " + column.getType());

		if (!column.isNullable()) {
			queryBuilder.append(" NOT NULL");
		}

		if (column.isAutoIncrement()) {
			queryBuilder.append(" DEFAULT NEXTVAL('" + name + "_" + column.getName() + "_seq')");
			execute("CREATE SEQUENCE " + name + "_" + column.getName() + "_seq;");
		}
		else if (!Strings.isNullOrEmpty(column.getDefaultExpression())) {
			queryBuilder.append(" DEFAULT " + column.getDefaultExpression());
		}

		execute(queryBuilder.toString());

		if (column.isIdentity()) {
			getColumn(column.getName()).setIdentity(true);
		}

		return new PostgresColumn(connection, this, column);
	}

	@Override
	public List<Index> listIndices() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT ")
				.append("  c.relname as \"name\", ")
				.append("  i.indisunique as \"is_unique\", ")
				.append("  i.indisprimary as \"is_primary\" ")
				.append("FROM pg_catalog.pg_class c ")
				.append("     JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid ")
				.append("     JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid ")
				.append("     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ")
				.append("WHERE c.relkind IN ('i', 'p') ")
				.append("      AND n.nspname NOT IN ('pg_catalog', 'pg_toast') ")
				.append("      AND pg_catalog.pg_table_is_visible(c.oid) ")
				.append("      AND c2.relname = ?")
				.append("ORDER BY c.relname ASC;")
				.toString();

		List<Index> indices = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, name);

			log.debug(query);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String indexName = resultSet.getString("name");
				boolean isUnique = resultSet.getBoolean("is_unique");
				boolean isPrimary = resultSet.getBoolean("is_primary");
				indices.add(new PostgresIndex(this, indexName, isUnique, isPrimary));
			}
		}

		return indices;
	}

	@Override
	public Index createIndex(String name, boolean unique, String... columnNames) throws SQLException {
		String columns = Joiner.on(',').join(columnNames);
		if (unique) {
			execute(String.format("CREATE UNIQUE INDEX CONCURRENTLY %s ON %s (%s)", name, this.name, columns));
		}
		else {
			execute(String.format("CREATE INDEX CONCURRENTLY %s ON %s (%s)", name, this.name, columns));
		}
		return new PostgresIndex(this, name, unique, false);
	}

	@Override
	public List<Constraint> listConstraints() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT tc.constraint_name, tc.constraint_type, kc.column_name ")
				.append("FROM information_schema.table_constraints tc ")
				.append("LEFT JOIN information_schema.key_column_usage kc ")
				.append("    ON kc.table_name = tc.table_name AND kc.table_schema = tc.table_schema ")
				.append("WHERE tc.table_schema = ? AND tc.table_name = ?")
				.toString();

		List<Constraint> constraints = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, "public");
			statement.setString(2, name);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String constraintName = resultSet.getString("constraint_name");
				String constraintType = resultSet.getString("constraint_type");
				String columnName = resultSet.getString("column_name");

				constraints.add(new PostgresConstraint(this, constraintName, constraintType, columnName));
			}
		}
		return constraints;
	}

	@Override
	public Constraint createConstraint(String name, String type, String expression) throws SQLException {
		String query = String.format("ALTER TABLE %s ADD CONSTRAINT %s %s %s", this.name, name, type, expression);
		getParent().execute(query);
		return new PostgresConstraint(this, name, type, expression);
	}

	@Override
	public List<ForeignKey> listForeignKeys() throws SQLException {
		String query = new QueryBuilder()
				.append("SELECT ")
				.append("    tc.constraint_name ")
				.append("FROM ")
				.append("    information_schema.table_constraints AS tc ")
				.append("    JOIN information_schema.key_column_usage AS kcu ")
				.append("      ON tc.constraint_name = kcu.constraint_name ")
				.append("    JOIN information_schema.constraint_column_usage AS ccu ")
				.append("      ON ccu.constraint_name = tc.constraint_name ")
				.append("WHERE constraint_type = 'FOREIGN KEY' AND tc.table_schema = ? AND tc.table_name = ?;")
				.toString();

		List<ForeignKey> foreignKeys = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, "public");
			statement.setString(2, name);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String constraintName = resultSet.getString("constraint_name");
				foreignKeys.add(new PostgresForeignKey(this, constraintName));
			}
		}
		return foreignKeys;
	}

	@Override
	public PostgresForeignKey addForeignKey(String constraint, String[] columns, String referencedTable, String[] referencedColumns)
			throws SQLException {

		execute(String.format("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", name, constraint,
				Joiner.on(',').join(columns), referencedTable, Joiner.on(',').join(referencedColumns)));

		return new PostgresForeignKey(this, constraint);
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("DROP TABLE %s", this.name));
		for (Column column : listColumns()) {
			if (column.isAutoIncrement()) {
				execute("DROP SEQUENCE IF EXISTS " + name + "_" + column.getName() + "_seq");
			}
		}
	}

	private void execute(String query) throws SQLException {
		getParent().execute(query);
	}

}
