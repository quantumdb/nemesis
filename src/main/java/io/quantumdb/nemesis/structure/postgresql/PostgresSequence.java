package io.quantumdb.nemesis.structure.postgresql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.Sequence;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class PostgresSequence implements Sequence {

	static List<Sequence> listSequences(Connection connection, PostgresDatabase parent) throws SQLException {
		List<Sequence> sequences = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT c.relname AS name FROM pg_class c WHERE c.relkind = 'S';");

			while (resultSet.next()) {
				String name = resultSet.getString("name");
				sequences.add(new PostgresSequence(parent, name));
			}
		}
		return sequences;
	}

	private final PostgresDatabase parent;
	private final String name;

	PostgresSequence(PostgresDatabase parent, String name) {
		this.parent = parent;
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public PostgresDatabase getParent() {
		return parent;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("DROP SEQUENCE %s", name));
	}

	private void execute(String query) throws SQLException {
		getParent().execute(query);
	}

}
