package utils.rebalancer.src;

import utils.rebalancer.commons.Operation;
import utils.rebalancer.commons.Pair;
import utils.rebalancer.commons.StorageTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GreedyStrategy implements AlgoStrategy {
    @Override
    public List<Operation> doAlgorithm(Map<Pair<String, String>, Double> distances, List<StorageTuple> tupleList, int threshold) {

        List<Operation> opsList = new ArrayList<>();
        int replicationFactor = tupleList.get(0).getReplicationFactor();

        Collections.sort(tupleList);

        /* Keep reiterating as long as not all tuples have been balanced */
        while( tupleList.size() > 1) {
            double meanLFNNo = this.getMeanLFNNo(tupleList);

            /**
             *  Separate the storage tuples in sources and destinations basted on the
             *   number of LFNs. The destinations are the tuples which will attempt to receive
             *   files and the sources will serve as attempt to send files.
             */
            List<StorageTuple> destinations = tupleList
                    .stream()
                    .filter(st -> st.getCommonLFNNo() > meanLFNNo)
                    .collect(Collectors.toList());

            List<StorageTuple> sources = tupleList
                    .stream()
                    .filter(c -> c.getCommonLFNNo() <= meanLFNNo)
                    .collect(Collectors.toList());

            int i = 0;
            for (StorageTuple source : sources) {
                while( source.getCommonLFNNo() != 0 && i < destinations.size()) {
                    /**/
                    int requiredLFNs = Math.min(source.getCommonLFNNo(), Math.abs(destinations.get(i).getCommonLFNNo() - threshold));

                    /**/
                    List<Operation> bestOPS = source.getOPS(destinations.get(i), distances, requiredLFNs, true, 1, 8, 1);
                    opsList.addAll(bestOPS);

                    source.transferLFNs(destinations.get(i), bestOPS);

                    /**/
                    if (source.getCommonLFNNo() == 0) {
                        tupleList.remove(source);
                    }

                    if (destinations.get(i).getCommonLFNNo() >= threshold) {
                        tupleList.remove(destinations.get(i));
                        i++;
                    }
                }
            }
        }

        return opsList;
    }
}
