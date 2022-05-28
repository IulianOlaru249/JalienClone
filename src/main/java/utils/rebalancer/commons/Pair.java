package utils.rebalancer.commons;

/**
 * Pair class
 * @param <U>
 * @param <V>
 */
public class Pair<U, V>
{
    /**
     * Pair elements
     */
    public final U first;
    public final V second;

    /**
     * Constructs a new pair with specified values
     * @param first
     * @param second
     */
    private Pair(U first, V second)
    {
        this.first = first;
        this.second = second;
    }

    /**
     * Checks specified object is "equal to" the current object or not
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (!first.equals(pair.first)) {
            return false;
        }
        return second.equals(pair.second);
    }

    /**
     * Computes hash code for an object to support hash tables
     */
    @Override
    public int hashCode()
    {
        // use hash codes of the underlying objects
        return 31 * first.hashCode() + second.hashCode();
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }

    /**
     * Factory method for creating a typed Pair immutable instance
     * @param a
     * @param b
     * @param <U>
     * @param <V>
     * @return
     */
    public static <U, V> Pair <U, V> of(U a, V b)
    {
        return new Pair<>(a, b);
    }
}