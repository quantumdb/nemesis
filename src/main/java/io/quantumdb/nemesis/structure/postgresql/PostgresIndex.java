package io.quantumdb.nemesis.structure.postgresql;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Index;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class PostgresIndex implements Index {

	private final PostgresTable parent;
	private final String name;

	private final boolean unique;
	private final boolean primary;

	PostgresIndex(PostgresTable parent, String name, boolean unique, boolean primary) {
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
	public PostgresTable getParent() {
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
		execute(String.format("ALTER INDEX %s RENAME TO %s", this.name, name));
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("DROP INDEX CONCURRENTLY %s", name));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}

}
