package io.quantumdb.nemesis.structure.mysql55;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.ForeignKey;

class MysqlForeignKey implements ForeignKey {

	private final MysqlTable parent;
	private final String name;

	MysqlForeignKey(MysqlTable parent, String name) {
		this.parent = parent;
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP FOREIGN KEY %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		parent.getParent().execute(query);
	}

}
