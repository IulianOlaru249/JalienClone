package utils.rebalancer.src;

import utils.rebalancer.commons.Operation;
import utils.rebalancer.commons.Pair;
import utils.rebalancer.commons.StorageTuple;

import java.util.List;
import java.util.Map;

/**
 * Common interface for all strategies.
 *
 * @
 */
public interface AlgoStrategy {
    /**
     * Methods that will be used by all Strategy Subclasses
     */

    /**
     * Compute tuple reunion.
     * @param seTuples - List of tuples
     * @return
     */
//    default StorageTuple getTupleReunion(List<StorageTuple> seTuples) {
//
//    }


    /**
     * Return all tuple combinations possible.
     * @param tupleReunion
     * @param replicationFactor
     * @return
     */
//    default List<StorageTuple> getAllCombinations(StorageTuple tupleReunion, int replicationFactor) {
//
//    }


    /**
     * Return all pairs of tuples.
     */
//    default void getAllTuplePairs(List<StorageTuple> tupleList, int pairDimension) {
//
//    }

    /**
     * Compute average number of common LFNs.
     * @param tupleList
     * @return
     */
    default double getMeanLFNNo(List<StorageTuple> tupleList) {
        double mean = 0;

        for (StorageTuple st : tupleList) {
            mean += st.getCommonLFNNo();
        }

        return mean / tupleList.size();
    }


    /**
     * The abstract method that will be implemented by
     * Strategy subclasses.
     */
    List<Operation> doAlgorithm(Map<Pair<String, String>, Double> distances, List<StorageTuple> tupleList, int threshold);

}
