package io.quantumdb.nemesis.profiler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import io.quantumdb.nemesis.structure.TableDefinition;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DatabaseStructure {

	private static final int BATCH_SIZE = 10_000;
	private static final DecimalFormat FORMAT = new DecimalFormat("##0");
	private static final String QUERY = "INSERT INTO users (name) VALUES (?);";

	private final Database.Type type;
	private final DatabaseCredentials credentials;

	public DatabaseStructure(Database.Type type, DatabaseCredentials credentials) {
		this.type = type;
		this.credentials = credentials;
	}

	public void prepareStructureAndRows(int rows) throws SQLException, InterruptedException {
		prepareStructure();
		prepareRows(rows);
	}

	public void prepareStructure() throws SQLException {
		log.info("Creating table...");

		TableDefinition table = new TableDefinition("users")
				.withColumn(new ColumnDefinition("id", "bigint")
						.setNullable(false)
						.setAutoIncrement(true)
						.setIdentity(true))
				.withColumn(new ColumnDefinition("name", "varchar(255)")
						.setNullable(false));

		Database backend = type.createBackend();
		backend.connect(credentials);
		backend.createTable(table);
		backend.close();

		log.info("Table created");
	}

	public void dropStructure() throws SQLException {
		log.info("Dropping table...");

		Database backend = type.createBackend();
		backend.connect(credentials);
		backend.dropContents();
		backend.close();

		log.info("Table dropped");
	}
	
	public void prepareRows(int rows) throws InterruptedException {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);

		try {
			log.info("Filling table...");

			AtomicLong lastTimestamp = new AtomicLong(System.currentTimeMillis());
			AtomicInteger counter = new AtomicInteger();

			int numberOfExecutors = (int) Math.min(Math.ceil(rows / (double) BATCH_SIZE), executor.getCorePoolSize());

			List<Future<?>> futures = Lists.newArrayList();
			for (int i = 0; i < numberOfExecutors; i++) {
				futures.add(executor.submit(() -> {
					Connection connection;
					PreparedStatement statement;
					try {
						Database backend = type.createBackend();
						backend.connect(credentials);
						connection = backend.getConnection();
						connection.setAutoCommit(false);
						statement = connection.prepareStatement(QUERY);
					}
					catch (SQLException e1) {
						log.error(e1.getMessage(), e1);
						return;
					}

					int myCounter = 0;
					while (counter.incrementAndGet() <= rows) {
						try {
							statement.setString(1, RandomNameGenerator.generate());
							statement.addBatch();
							myCounter++;

							if (myCounter % BATCH_SIZE == 0) {
								statement.executeBatch();
								statement.close();
								connection.commit();

								statement = connection.prepareStatement(QUERY);
								myCounter = 0;
							}
						}
						catch (SQLException e1) {
							log.error(e1.getMessage(), e1);
							return;
						}
					}

					try {
						if (myCounter > 0) {
							statement.executeBatch();
							statement.close();
							connection.commit();
						}

						connection.close();
					}
					catch (SQLException e1) {
						log.error(e1.getMessage(), e1);
					}
				}));
			}

			long lastCounter = 0;
			String lastPrinted = "";
			while (counter.get() < rows) {
				sleep(100);

				String print = FORMAT.format((double) counter.get() / rows * 100d) + "%";
				print = Strings.padStart(print, 7, ' ');

				if (!lastPrinted.equals(print)) {
					long now = System.currentTimeMillis();
					double newInserts = counter.get() - lastCounter;
					double timeDiff = ((double) (now - lastTimestamp.get())) / 1000.0;
					long speed = (long) (newInserts / timeDiff);

					log.info(print + " - " + speed + " inserts/sec");
					lastPrinted = print;
					lastTimestamp.set(now);
					lastCounter = counter.get();
				}
			}

			while (!futures.isEmpty()) {
				try {
					futures.remove(0).get();
				}
				catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}

			log.info("Table filled");
		}
		finally {
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
		}
	}

	private void sleep(int millis) {
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.error(e.getMessage(), e);
		}
	}
}
