package utils.rebalancer.src;

import utils.rebalancer.commons.InputParser;
import utils.rebalancer.commons.Operation;
import utils.rebalancer.commons.Pair;
import utils.rebalancer.commons.StorageTuple;

import java.util.*;

public class MainTest {
    public static void main(String[] args) {
        utils.rebalancer.commons.InputParser inputParser = new utils.rebalancer.commons.InputParser();

        Map<Pair<String, String>, Double> distances = inputParser.getDistances("/home/ghostpants/Documents/CERN/jalien/src/main/java/utils/rebalancer/mock_dataset/seDistance.csv");
        List<StorageTuple> seTuples = InputParser.getStorageElements("/home/ghostpants/Documents/CERN/jalien/src/main/java/utils/rebalancer/mock_dataset/whereis_cern2.csv",
                "/home/ghostpants/Documents/CERN/jalien/src/main/java/utils/rebalancer/mock_dataset/seList_cern.csv");

//        for (Pair<String, String> pair : distances.keySet()) {
//            String firstSE = pair.first;
//            String secSE = pair.second;
//            String value = distances.get(pair).toString();
//            System.out.println("[" + firstSE + " --> " + secSE + "] = "  + value);
//        }
//        for(StorageTuple seTuple : seTuples) {
//            System.out.println(seTuple.getSeMemberNames());
//        }

        AlgoStrategy algoStrategy = new GreedyStrategy();
        List<Operation> greedyOps = algoStrategy.doAlgorithm(distances, seTuples, 200);

        for (Operation ops : greedyOps) {
            System.out.println(ops);
        }

    }
}

