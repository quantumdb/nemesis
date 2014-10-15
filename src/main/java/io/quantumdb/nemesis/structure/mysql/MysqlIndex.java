package io.quantumdb.nemesis.structure.mysql;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Index;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
class MysqlIndex implements Index {

	private final MysqlTable parent;
	private final String name;

	private final boolean unique;
	private final boolean primary;

	MysqlIndex(MysqlTable parent, String name, boolean unique, boolean primary) {
		this.parent = parent;
		this.name = name;
		this.unique = unique;
		this.primary = primary;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public MysqlTable getParent() {
		return parent;
	}

	@Override
	public boolean isUnique() {
		return unique;
	}

	@Override
	public boolean isPrimary() {
		return primary;
	}

	@Override
	public void rename(String name) throws SQLException {
		throw new UnsupportedOperationException("Mysql 5.5 does not support renaming indices.");
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP INDEX %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}

}
