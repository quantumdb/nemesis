package io.quantumdb.nemesis.structure.postgresql;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Constraint;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class PostgresConstraint implements Constraint {

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
