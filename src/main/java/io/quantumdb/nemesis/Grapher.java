package io.quantumdb.nemesis;

import javax.imageio.ImageIO;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class Grapher {

	private static final int SKIP_UNTIL = 45_000;
	private static final int PADDING = 5;
	private static final int WIDTH = 901;
	private static final int HEIGHT = 60;
	private static final int SCALE = 30;  // Pixels per second

//	private static final int SKIP_UNTIL = 0;
//	private static final int PADDING = 20;
//	private static final int WIDTH = 1000;
//	private static final int HEIGHT = 150;
//	private static final int SCALE = 30;  // Pixels per second

	private static final int RESOLUTION = (1000 / SCALE);
	private static final int LIMIT = WIDTH * RESOLUTION + SKIP_UNTIL;

	public static void main(String[] args) throws IOException {
		File dir = new File(args[0]);
		File[] scenarios = dir.listFiles(file -> file.isDirectory() && !file.getName().startsWith(".") && !file.getName().startsWith("_"));
		for (File scenario : scenarios) {
			new Grapher().graphResponseTimes(scenario);
			log.info("Graphed: " + scenario.getAbsolutePath());
		}
	}
	
	public void graphResponseTimes(File folder) throws IOException {
		drawSplit(folder);
	}

	private void drawSplit(File folder) throws IOException {
		BufferedImage image = new BufferedImage(
				WIDTH,
				HEIGHT + PADDING,
				BufferedImage.TYPE_USHORT_555_RGB);

		Graphics graphics = image.getGraphics();

		graphics.setColor(Color.WHITE);
		graphics.fillRect(0, 0, image.getWidth(), image.getHeight());

		File[] logFiles = folder.listFiles((dir, name) -> name.endsWith(".log"));
		ArrayList<File> files = Lists.newArrayList(logFiles);
		Collections.sort(files);

		AtomicReference<String> type = new AtomicReference<>();
		for (File file : files) {
			parse(file, line -> {
				long x = getQueryStart(line);
				if (x < SKIP_UNTIL) {
					return true;
				}
				if (x > LIMIT + 1000) {
					return false;
				}

				long queryEnd = getQueryEnd(line);
				int queryDuration = (int) (queryEnd - x);
				String queryType = getWorkerType(line);

				int y = queryDuration;
				if (queryType.equals("Operation")) {
					graphics.setColor(new Color(0f, 0f, 0f, 0.2f));
					graphics.fillRect(toX(x), 0, Math.min(WIDTH - toX(x), toX(queryDuration + SKIP_UNTIL)), toY(image.getHeight()) - PADDING + 1);
				}

				if (!queryType.equals(type.get())) {
					type.set(queryType);
					log.info("Drawing: {}", type.get());
				}

				switch (queryType) {
					case "InsertWorker":
						graphics.setColor(new Color(0, 255, 0, 80));
						break;
					case "DeleteWorker":
						graphics.setColor(new Color(255, 0, 0, 80));
						break;
					case "UpdateWorker":
						graphics.setColor(new Color(0, 0, 255, 80));
						break;
					case "SelectWorker":
						graphics.setColor(new Color(255, 200, 0, 80));
						break;
					case "Operation":
						graphics.setColor(new Color(0, 0, 0));
						break;
					default:
						throw new IllegalArgumentException("Wrong argument: " + queryType);
				}

				graphics.drawLine(toX(x), toY(Math.max(0, image.getHeight() - y)) - PADDING, toX(x), toY(image.getHeight()) - PADDING);
				return true;
			});
		}

		graphics.setColor(Color.BLACK);
		graphics.drawLine(1, HEIGHT, WIDTH, HEIGHT);
//		graphics.drawLine(1, PADDING, 1, HEIGHT);

		for (int i = 0; i <= WIDTH; i += SCALE) {
			graphics.drawLine(i, HEIGHT, i, HEIGHT + 4);
		}

		ImageIO.write(image, "png", new File(folder, folder.getName() + ".png"));
		ImageIO.write(image, "png", new File(new File(folder.getParent(), "graphs"), folder.getName() + ".png"));
	}

	private int toX(long x) {
		return (int) ((double) (x - SKIP_UNTIL) / 1000.0 * SCALE);
	}

	private int toY(int y) {
		return y;
	}
	
	private void parse(File file, Parser parser) throws IOException {
		try (RandomAccessFile accessor = new RandomAccessFile(file, "r")) {
			boolean read = true;
			do {
				String line = accessor.readLine();
				if (line == null || !parser.parse(line)) {
					read = false;
				}
			}
			while (read);
		}
	}
	
	private String getWorkerType(String line) {
		return line.substring(0, line.indexOf('\t'));
	}
	
	private long getQueryStart(String line) {
		int first = line.indexOf('\t');
		return Long.parseLong(line.substring(first + 1, line.indexOf('\t', first + 1)));
	}
	
	private long getQueryEnd(String line) {
		int first = line.indexOf('\t');
		int second = line.indexOf('\t', first + 1);
		return Long.parseLong(line.substring(second + 1, line.indexOf('\t', second + 1)));
	}
	
	@FunctionalInterface
	private interface Parser {
		boolean parse(String line);
	}
	
}
