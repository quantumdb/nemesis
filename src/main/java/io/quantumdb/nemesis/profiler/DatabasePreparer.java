package io.quantumdb.nemesis.profiler;

import static io.quantumdb.nemesis.schema.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.nemesis.schema.Column.Hint.NOT_NULL;
import static io.quantumdb.nemesis.schema.Column.Hint.PRIMARY_KEY;
import static io.quantumdb.nemesis.schema.types.GenericColumnTypes.int8;
import static io.quantumdb.nemesis.schema.types.GenericColumnTypes.varchar;

import java.io.IOException;
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
import io.quantumdb.nemesis.backends.DatabaseBackend;
import io.quantumdb.nemesis.backends.DatabaseConnector;
import io.quantumdb.nemesis.backends.DatabaseCredentials;
import io.quantumdb.nemesis.backends.RandomNameGenerator;
import io.quantumdb.nemesis.schema.Column;
import io.quantumdb.nemesis.schema.Table;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DatabasePreparer {

	private static final int ROWS = 100_000_000;
	private static final int BATCH_SIZE = 10_000;

	private static final DecimalFormat FORMAT = new DecimalFormat("##0");
	private static final String QUERY = "INSERT INTO users (name) VALUES (?);";
	private static final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);

	private final DatabaseBackend.Type type;
	private final DatabaseCredentials credentials;

	public DatabasePreparer(DatabaseBackend.Type type, DatabaseCredentials credentials) {
		this.type = type;
		this.credentials = credentials;
	}

	public void run() throws ClassNotFoundException, InterruptedException {
		try {
			createTable();
			fillTable();
		}
		catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		finally {
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
		}
	}

	private Connection createConnection(boolean autoCommit) throws SQLException, ClassNotFoundException {
		Connection c = DatabaseConnector.connect(type, credentials);
		c.setAutoCommit(autoCommit);
		return c;
	}

	private void createTable() throws SQLException, ClassNotFoundException {
		log.info("Creating table...");

		Table table = new Table("users")
				.addColumn(new Column("id", int8(), NOT_NULL, AUTO_INCREMENT, PRIMARY_KEY))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		DatabaseBackend backend = DatabaseBackend.Factory.create(type);
		backend.connect(credentials);
		backend.createTable(table);
		backend.close();
		
		log.info("Table created");
	}
	
	private void fillTable() {
		log.info("Filling table...");
		
		AtomicLong lastTimestamp = new AtomicLong(System.currentTimeMillis());
		AtomicInteger counter = new AtomicInteger();
		
		List<Future<?>> futures = Lists.newArrayList();
		for (int i = 0; i < executor.getCorePoolSize(); i++) {
			futures.add(executor.submit(() -> {
				Connection connection;
				PreparedStatement statement;
				try {
					connection = createConnection(false);
					statement = connection.prepareStatement(QUERY);
				} catch (Exception e1) {
					e1.printStackTrace();
					return;
				}
				
				int myCounter = 0;
				while (counter.incrementAndGet() <= ROWS) {
					try {
						statement.setString(1, RandomNameGenerator.generate());
						statement.addBatch();
						myCounter++;
						
						if (myCounter % BATCH_SIZE == 0) {
							statement.executeBatch();
							statement.close();
							connection.commit();

							statement = connection.prepareStatement(QUERY);
						}
					} 
					catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				try {
					statement.executeBatch();
					statement.close();

					connection.commit();
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}));
		}
		
		long lastCounter = 0;
		String lastPrinted = "";
		while (counter.get() < ROWS) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			String print = FORMAT.format((double) counter.get() / ROWS * 100d) + "%";
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
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		
		log.info("Table filled");
	}
	
}
