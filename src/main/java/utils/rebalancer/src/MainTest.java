package utils.rebalancer.src;

import utils.rebalancer.commons.InputParser;
import utils.rebalancer.commons.Pair;
import utils.rebalancer.commons.StorageTuple;

import java.util.*;

public class MainTest {
    public static void main(String[] args) {
        //List<StorageTuple> seTuples = inputParser.
        Map<Pair<String, String>, Double> distances = InputParser.getDistances("/home/nicu/Documents/Facultate/Licenta/jalien/src/main/java/utils/rebalancer/mock_dataset/seDistance.csv");
        List<StorageTuple> seTuples = InputParser.getStorageElements("/home/nicu/Documents/Facultate/Licenta/jalien/src/main/java/utils/rebalancer/mock_dataset/whereis_cern2.csv",
                                                                        "/home/nicu/Documents/Facultate/Licenta/jalien/src/main/java/utils/rebalancer/mock_dataset/seList_cern.csv");

        for(StorageTuple seTuple : seTuples) {
            System.out.println(seTuple.getSeMemberNames());
        }
        //System.out.println("There are: " + count + " tuples");

//        for (Pair<String, String> pair : distances.keySet()) {
//            String firstSE = pair.first;
//            String secSE = pair.second;
//            String value = distances.get(pair).toString();
//            System.out.println("[" + firstSE + " --> " + secSE + "] = "  + value);
//        }


    }
}
