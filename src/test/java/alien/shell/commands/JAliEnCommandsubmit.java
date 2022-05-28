package alien.shell.commands;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JAliEnCommandsubmitTest {
	@Test
	static void testJavaArraySplicing() {
		String[] arr = { "Hello", "Bright", "World" };
		List<String> list = Arrays.asList("Hello", "Dark", "World");

		Assertions.assertEquals(3, arr.length);
		Assertions.assertEquals(arr.length, list.size());

		arr = list.subList(1, list.size()).toArray(new String[0]);

		Assertions.assertEquals(2, arr.length);
		Assertions.assertEquals("Dark", arr[0]);
		Assertions.assertEquals("World", arr[1]);
	}

	static void checkArgs(String[] target, String[] args) {
		JAliEnCommandsubmit cmd = new JAliEnCommandsubmit(null, Arrays.asList(args));
		Assertions.assertArrayEquals(target, cmd.getArgs());
	}

	@Test
	static void testCommandSubmit() {
		String args[] = null;
		String target[] = null;

		args = new String[] { "zero", "one", "two" };
		target = new String[] { "one", "two" };
		checkArgs(target, args);

		args = new String[] { "zero" };
		checkArgs(null, args);
	}
}
