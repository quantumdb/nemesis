package io.quantumdb.nemesis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import io.quantumdb.nemesis.profiler.DatabaseStructure;
import io.quantumdb.nemesis.profiler.Profiler;
import io.quantumdb.nemesis.profiler.ProfilerConfig;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Launcher {

	private static final int ROWS = 100_000_000;
	private static final int STARTUP_TIMEOUT = 5000;
	private static final int TEARDOWN_TIMEOUT = 5000;

	@SneakyThrows
	public static void main(String[] args) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		System.out.println("QuantumDB - Nemesis\n");
		selectDatabaseType(reader);
	}

	private static void selectDatabaseType(BufferedReader reader) throws InterruptedException, IOException {
		while (true) {
			System.out.println("\nType of database?\n");
			System.out.println("  1. PostgreSQL.");
			System.out.println("  2. MySQL.");
			System.out.println("  3. Exit.");
			System.out.println("");
			System.out.print("Option: ");

			try {
				int option = Integer.parseInt(reader.readLine().trim());
				System.out.println("");

				switch (option) {
					case 1:
						setCredentials(reader, Database.Type.POSTGRESQL);
						break;
					case 2:
						setCredentials(reader, Database.Type.MYSQL);
						break;
					case 3:
						return;
					default:
						System.err.println("You must choose an option in range [1..3]");
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			catch (NumberFormatException e) {
				System.err.println("You must choose an option in range [1..3]");
			}
		}
	}

	private static void setCredentials(BufferedReader reader, Database.Type type)
			throws InterruptedException, IOException {

		String url      = ask("JDBC url: ", reader);
		String username = ask("Username: ", reader);
		String password = ask("Password: ", reader);

		DatabaseCredentials credentials = new DatabaseCredentials(url, username, password);
		prepareProfiling(reader, type, credentials);
	}

	private static void prepareProfiling(BufferedReader reader, Database.Type type,
			DatabaseCredentials credentials) throws InterruptedException {

		while (true) {
			System.out.println("\nWhat do you want to do?\n");
			System.out.println("  1. Prepare the SQL database for Nemesis.");
			System.out.println("  2. Run Nemesis on the SQL database.");
			System.out.println("  3. Exit.");
			System.out.println("");
			System.out.print("Option: ");

			try {
				int option = Integer.parseInt(reader.readLine().trim());
				System.out.println("");

				switch (option) {
					case 1:
						DatabaseStructure preparer = new DatabaseStructure(type, credentials);
						preparer.prepareStructureAndRows(ROWS);
						break;
					case 2:
						int readers = askWorkerQuantity("READER", reader);
						int inserts = askWorkerQuantity("INSERT", reader);
						int deletes = askWorkerQuantity("DELETE", reader);
						int updates = askWorkerQuantity("UPDATE", reader);

						ProfilerConfig config = new ProfilerConfig(readers, updates, inserts, deletes);

						Profiler profiler = new Profiler(config, type, credentials, STARTUP_TIMEOUT, TEARDOWN_TIMEOUT);
						profiler.profile();
						break;
					case 3:
						return;
					default:
						System.err.println("You must choose an option in range [1..3]");
				}
			}
			catch (NumberFormatException e) {
				System.err.println("You must choose an option in range [1..3]");
			}
			catch (Throwable e) {
				log.error(e.getMessage(), e);
			}
			Thread.sleep(100);
		}
	}

	@SneakyThrows
	private static int askWorkerQuantity(String type, BufferedReader reader) {
		while (true) {
			try {
				int option = Integer.parseInt(ask(String.format("How many %s workers: ", type), reader));
				if (option >= 0) {
					return option;
				}
			}
			catch (Throwable e) {
				// Do nothing...
			}
			System.err.println("You must choose an option in range [0..]");
			Thread.sleep(100);
		}
	}

	@SneakyThrows
	private static String ask(String question, BufferedReader reader) {
		System.out.print(question);
		return reader.readLine().trim();
	}

}
