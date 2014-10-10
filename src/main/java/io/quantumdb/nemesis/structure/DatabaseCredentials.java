package io.quantumdb.nemesis.structure;

import lombok.Data;

/**
 * This data class described the information that is needed to connect to a SQL database.
 */
@Data
public class DatabaseCredentials {

	private final String url;
	private final String username;
	private final String password;

}
