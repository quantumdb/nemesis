package io.quantumdb.nemesis.backends;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DatabaseConnector {

	public static Connection connect(DatabaseBackend.Type type, DatabaseCredentials credentials)
			throws SQLException, ClassNotFoundException {

		Class.forName(type.getDriver());
		return DriverManager.getConnection(credentials.getUrl(), credentials.getUsername(), credentials.getPassword());
	}
}
