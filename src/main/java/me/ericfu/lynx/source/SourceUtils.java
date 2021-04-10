package me.ericfu.lynx.source;

public abstract class SourceUtils {

    /**
     * Partition number S into N ranges such that difference between the smallest and the largest part is minimum
     * <p>
     * For example, given S = 10 and N = 3, the output is [3, 6, 10]
     *
     * @param s total number S
     * @param n number of parts N
     * @return a array of the best split results, while number in index i as end of this range
     */
    public static long[] quantiles(long s, int n) {
        // split
        long[] result = new long[n];
        long r = n - (s % n);
        long d = s / n;
        for (int i = 0; i < n; i++) {
            result[i] = i >= r ? d + 1 : d;
        }
        // accumulate
        for (int i = 1; i < result.length; i++) {
            result[i] += result[i - 1];
        }
        return result;
    }
}
