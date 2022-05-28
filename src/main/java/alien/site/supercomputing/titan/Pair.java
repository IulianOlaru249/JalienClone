package alien.site.supercomputing.titan;

/**
 * @author psvirin
 *
 * @param <F>
 * @param <S>
 */
public class Pair<F, S> {
	private final F first;
	private final S second;

	/**
	 * @param _first
	 * @param _second
	 */
	public Pair(final F _first, final S _second) {
		first = _first;
		second = _second;
	}

	/**
	 * @return first
	 */
	public F getFirst() {
		return first;
	}

	/**
	 * @return second
	 */
	public S getSecond() {
		return second;
	}
}
