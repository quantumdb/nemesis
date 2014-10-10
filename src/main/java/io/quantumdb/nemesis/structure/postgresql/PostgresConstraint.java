package io.quantumdb.nemesis.structure.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.Constraint;
import io.quantumdb.nemesis.structure.QueryBuilder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class PostgresConstraint implements Constraint {

	static Constraint createConstraint(PostgresTable table, String name, String type, String expression)
			throws SQLException {

		String query = String.format("ALTER TABLE %s ADD CONSTRAINT %s %s %s", table.getName(), name, type, expression);
		table.getParent().execute(query);
		return new PostgresConstraint(table, name, type, expression);
	}

	static List<Constraint> listConstraints(Connection connection, PostgresTable table) throws SQLException {
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
			statement.setString(2, table.getName());
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String constraintName = resultSet.getString("constraint_name");
				String constraintType = resultSet.getString("constraint_type");
				String columnName = resultSet.getString("column_name");

				constraints.add(new PostgresConstraint(table, constraintName, constraintType, columnName));
			}
		}
		return constraints;
	}

	private final PostgresTable parent;
	private final String name;
	private final String type;
	private final String expression;

	PostgresConstraint(PostgresTable parent, String name, String type, String expression) {
		this.parent = parent;
		this.name = name;
		this.type = type;
		this.expression = expression;
	}

	@Override
	public String getName() {
		return name;
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
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP CONSTRAINT %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}

}
