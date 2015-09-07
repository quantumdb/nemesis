package io.quantumdb.nemesis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class Splitter {

	public static void main(String[] args) throws IOException {
		File folder = new File(args[0]);
		File[] scenarios = folder.listFiles(file -> file.isDirectory() && !file.getName().startsWith(".") && !file.getName().startsWith("_") && !file.getName().equals("graphs"));
		for (File scenario : scenarios) {
			AtomicLong start = new AtomicLong();
			AtomicLong end = new AtomicLong();

			File operation = new File(scenario, "OPERATION.log");
			parse(operation, line -> {
				start.set(getQueryStart(line));
				end.set(getQueryEnd(line));
			});

			long middle = (end.get() - start.get()) / 2 + start.get();
			Range<Long> preRange = Range.closed(1000L, 51000L);
			Range<Long> duringRange = Range.closed(middle - 25000L, middle + 25000L);
			Range<Long> postRange = Range.closed(end.get() + 1000L, end.get() + 51000L);

			File[] logFiles = scenario.listFiles((dir, name) -> name.endsWith(".log") && !name.contains("OPERATION"));
			for (File file : logFiles) {
				log.info("Processing: {}", file.getAbsoluteFile());

				File pre = new File(file.getAbsolutePath() + ".pre");
				File during = new File(file.getAbsolutePath() + ".during");
				File post = new File(file.getAbsolutePath() + ".post");

				FileWriter preWriter = new FileWriter(pre);
				FileWriter duringWriter = new FileWriter(during);
				FileWriter postWriter = new FileWriter(post);

				parse(file, line -> {
					long queryStart = getQueryStart(line);
					if (preRange.contains(queryStart)) {
						preWriter.write(line + "\n");
					}
					else if (duringRange.contains(queryStart)) {
						duringWriter.write(line + "\n");
					}
					else if (postRange.contains(queryStart)) {
						postWriter.write(line + "\n");
					}
				});

				preWriter.flush();
				duringWriter.flush();
				postWriter.flush();

				preWriter.close();
				duringWriter.close();
				postWriter.close();
			}
		}
	}
	
	private static void parse(File file, Parser parser) throws IOException {
		try (RandomAccessFile accessor = new RandomAccessFile(file, "r")) {
			boolean read = true;
			do {
				String line = accessor.readLine();
				if (line == null) {
					read = false;
				}
				else {
					parser.parse(line);
				}
			}
			while (read);
		}
	}
	
	private static String getWorkerType(String line) {
		return line.substring(0, line.indexOf('\t'));
	}
	
	private static long getQueryStart(String line) {
		int first = line.indexOf('\t');
		return Long.parseLong(line.substring(first + 1, line.indexOf('\t', first + 1)));
	}
	
	private static long getQueryEnd(String line) {
		int first = line.indexOf('\t');
		int second = line.indexOf('\t', first + 1);
		return Long.parseLong(line.substring(second + 1, line.indexOf('\t', second + 1)));
	}
	
	@FunctionalInterface
	private interface Parser {
		void parse(String line) throws IOException;
	}
	
}
