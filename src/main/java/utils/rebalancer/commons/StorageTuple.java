package utils.rebalancer.commons;

import alien.config.ConfigUtils;
import utils.lfncrawler.LFNCrawler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Storage Tuple
 * a collection of storage elements with common files
 *
 * @author -
 * @since -
 */
public class StorageTuple implements Comparable{
    /**
     * Counters for the common LFN pool and for the number of SEs
     */
    private int sesNo = 0;
    private int commonLFNNo = 0;

    /**
     * Lists common LFN pool and SEs
     */
    private List<StorageTupleMember> ses = new ArrayList<>();
    private List<String> LFNs = new ArrayList<>();

    /**
     * Number of common LFNs in tuple.
     * @return
     */
    public int getCommonLFNNo() {
        return commonLFNNo;
    }

    /**
     * Add a new storage element to the tuple
     *
     * @param newMember A new storage element object
     */
    public void addSEMember(StorageTupleMember newMember) {
        this.ses.add(newMember);
        this.sesNo ++;
    }

    /**
     *
     * @param newLfn
     */
    public void addCommonLfn(String newLfn) {
        this.LFNs.add(newLfn);
        this.commonLFNNo++;
    }

    /**
     * The replication factor is equal to the number of SEs in a tuple
     */
    public int getReplicationFactor() {
        return this.ses.size();
    }

    /**
     * Generate all permutations of a list.
     * @param original
     * @param <E>
     * @return
     */
    public static <E> List<List<E>> generatePermutations(List<E> original) {
        if(original.isEmpty()) {
            List<List<E>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }

        E firstElement = original.remove(0);
        List<List<E>> returnValue = new ArrayList<>();
        List<List<E>> permutations = generatePermutations(original);
        for (List<E> smallerPermutated : permutations) {
            for (int index = 0; index <= smallerPermutated.size(); index++) {
                List<E> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    /**
     * Function for zipping two lists.
     * @param as
     * @param bs
     * @param <A>
     * @param <B>
     * @return
     */
    public static <A, B> List<Pair<A, B>> zipJava8(List<A> as, List<B> bs) {
        return IntStream.range(0, Math.min(as.size(), bs.size()))
                .mapToObj(i -> Pair.of(as.get(i), bs.get(i)))
                .collect(Collectors.toList());
    }

    /**
     * Get all possible ways of moving common files (LFNs) from one tuple to another.
     * @param firstTupleSEs
     * @param secondTupleSEs
     * @return A list of lists of SE pairs.
     */
    public List<List<Pair<StorageTupleMember, StorageTupleMember>>> getAllCombinations(List<StorageTupleMember> firstTupleSEs,
                                                                                       List<StorageTupleMember> secondTupleSEs) {
        List<List<StorageTupleMember>> result = generatePermutations(firstTupleSEs);
        return result.stream().map(x -> zipJava8(x, secondTupleSEs)).collect(Collectors.toList());
    }

    /**
     * The name of a tuple is the concatenation of all members' names
     */
    public StringBuilder getSeMemberNames() {
        StringBuilder tupleName = new StringBuilder("( ");

        for(StorageTupleMember member : this.ses) {
            tupleName.append(member.originalName + " ");
        }
        tupleName.append(")");

        return tupleName;
    }

    /**
     * Get all the members of this class
     */
    public List<StorageTupleMember> getSeMembers() {
        return this.ses;
    }


    /**
     * Compute the cost of transferring LFNs between Tuples
     *
     */
//    public Double getOPSCost( StorageTupleMember targetSE, Map<Pair<String, String>, Double> distances,
//                              int transferedLFNNo, boolean withWriteDemotion,
//                              int readDemotionWeight, int distanceWeight, int writeDemotionWeight) {
//        /**
//         * The number of LFNs that will be moved is determined in the strategy.
//         * Examples:
//         *  (A,B) --2--> (B, D) ==(possible ways to transfer between SE members)==> (AB, BD), (AD, BB)
//         *  (A, B, C) --9--> (D, E, F) ====> (AD, BE, CF), (AF, BE, CD), ...
//         */
//        return (double)0;
//    }


    public Pair<Double, List<Pair<StorageTupleMember, StorageTupleMember>>> getOPSCost( StorageTuple seTuple, Map<Pair<String, String>, Double> distances,
                              int transferedLFNNo, boolean withWriteDemotion,
                              int readDemotionWeight, int distanceWeight, int writeDemotionWeight) {
        /**
         * The number of LFNs that will be moved is determined in the strategy.
         * Examples:
         *  (A,B) --2--> (B, D) ==(possible ways to transfer between SE members)==> (AB, BD), (AD, BB)
         *  (A, B, C) --9--> (D, E, F) ====> (AD, BE, CF), (AF, BE, CD), ...
         */
        //TODO: IMPORTANT: Maybe use Operation class instead of Pair<StorageTupleMember, StorageTupleMember>
        List<List<Pair<StorageTupleMember, StorageTupleMember>>> allCombinations = getAllCombinations(getSeMembers(), seTuple.getSeMembers());
        double minCost = (double)Integer.MAX_VALUE;
        List<Pair<StorageTupleMember, StorageTupleMember>> bestMatch = null;

        for(List<Pair<StorageTupleMember, StorageTupleMember>> pairs : allCombinations) {
            double costTotal = 0;
            for(Pair<StorageTupleMember, StorageTupleMember> pair : pairs) {
                StorageTupleMember source = pair.first;
                StorageTupleMember destination = pair.second;
                costTotal += source.getOPSCost(destination, distances, transferedLFNNo, withWriteDemotion,
                                                readDemotionWeight, distanceWeight, writeDemotionWeight);
            }
            if(minCost > costTotal) {
                minCost = costTotal;
                bestMatch = pairs;
            }
        }
        return Pair.of(minCost, bestMatch);
    }

    /**
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
