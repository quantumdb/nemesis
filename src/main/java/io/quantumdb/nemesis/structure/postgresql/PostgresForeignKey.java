package io.quantumdb.nemesis.structure.postgresql;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.ForeignKey;

class PostgresForeignKey implements ForeignKey {

	private final PostgresTable parent;
	private final String name;

	PostgresForeignKey(PostgresTable parent, String name) {
		this.parent = parent;
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP CONSTRAINT %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		parent.getParent().execute(query);
	}

}
