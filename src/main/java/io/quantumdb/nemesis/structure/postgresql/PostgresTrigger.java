package io.quantumdb.nemesis.structure.postgresql;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Trigger;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
class PostgresTrigger implements Trigger {

	private final PostgresTable parent;
	private final String name;

	PostgresTrigger(PostgresTable parent, String name) {
		this.parent = parent;
		this.name = name;
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
	public void drop() throws SQLException {
		execute(String.format("DROP TRIGGER %s ON %s", name, parent.getName()));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}

}
