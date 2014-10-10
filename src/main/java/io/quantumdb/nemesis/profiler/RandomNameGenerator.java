package io.quantumdb.nemesis.profiler;

import java.util.Random;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RandomNameGenerator {

	private static final Random RANDOM = new Random();

	private static final String[] FIRST_NAMES = {
			"Walter", "Skyler", "Jesse", "Hank", "Marie",
			"Saul", "Steven", "Mike", "Gustavo", "Ted",
			"Lydia", "Gale", "Leonel", "Marco", "Tuco"
	};

	private static final String[] LAST_NAMES = {
			"White", "Pinkman", "Schrader", "Goodman",
			"Gomez", "Ehrmantraut", "Fring", "Beneke",
			"Rodarte-Quayle", "Boetticher", "Salamanca"
	};

	public static String generate() {
		String firstName = FIRST_NAMES[RANDOM.nextInt(FIRST_NAMES.length)];
		String lastName = LAST_NAMES[RANDOM.nextInt(LAST_NAMES.length)];
		return firstName + " " + lastName;
	}

}
