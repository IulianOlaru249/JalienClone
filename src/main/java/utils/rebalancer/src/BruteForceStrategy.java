package utils.rebalancer.src;

import utils.rebalancer.commons.Operation;
import utils.rebalancer.commons.Pair;
import utils.rebalancer.commons.StorageTuple;
import utils.rebalancer.commons.StorageTupleMember;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BruteForceStrategy implements AlgoStrategy{
    @Override
    public List<Operation> doAlgorithm(Map<Pair<String, String>, Double> distances, List<StorageTuple> tupleList, int threshold) {
        int replicationFactor = tupleList.get(0).getReplicationFactor();

        List<Operation> operationList = new ArrayList<>();

        for(StorageTuple targetSeTuple : tupleList) {

            /**
            If the number of LFNs on tuple is already over threshold, continue.
            It may have already surpassed the threshold via a previous operation.
             */
            if(targetSeTuple.getCommonLFNNo() >= threshold)
                continue;

            double cheapestOpsCost = Integer.MAX_VALUE;

            /**
             * Cheapest sequence of operations (moves) between 2 StorageTuple elements.
             */
            List<Pair<StorageTupleMember, StorageTupleMember>> cheapestOps = null;

            int transferredLFNs = 0;
            StorageTuple destination = null;
            StorageTuple source = null;

            /**
             * This should indicate whether there are any possible ops to execute or not.
             */
            boolean opsPossible = false;
            for(StorageTuple seTuple : tupleList) {
                /**
                 * If it's the same tuple then continue to the next.
                 */
                if(seTuple.getSeMemberNames().equals(targetSeTuple.getSeMemberNames()))
                    continue;

                /**
                 * How many files are necessary to reach the threshold.
                 */
                int requiredLFNNo = Math.abs(threshold - targetSeTuple.getCommonLFNNo());
                if(seTuple.getCommonLFNNo() >= threshold) {
                    /**
                    Either get all files as long as the source is not going under threshold or get
                    * as many files as needed to get the destination over threshold*/
                    requiredLFNNo = Math.min(Math.abs(threshold - seTuple.getCommonLFNNo()), requiredLFNNo);
                }
                else {
                    requiredLFNNo = Math.min(requiredLFNNo, seTuple.getCommonLFNNo());
                }

                /**
                 * Common LFN number is always divisible with replication factor
                 * Compute cost based on how many files per SE we transfer
                 */
                Pair<Double, List<Pair<StorageTupleMember, StorageTupleMember>>> opsEliminate = targetSeTuple.getOPSCost(seTuple,
                        distances, targetSeTuple.getCommonLFNNo(), true, 1, 8, 1);
                Pair<Double, List<Pair<StorageTupleMember, StorageTupleMember>>> opsAdd = seTuple.getOPSCost(targetSeTuple, distances, requiredLFNNo,
                        true, 1, 8, 1);

                /**
                 * Cost of chosen operations
                 */
                double costOpsEliminate = opsEliminate.first;
                double costOpsAdd = opsAdd.first;

                /**
                 * If number of common LFNs on seTuple reached the threshold you can't move any files from it.
                 */
                if(seTuple.getCommonLFNNo() == threshold)
                    costOpsAdd = Integer.MAX_VALUE;

                /**
                 * Not possible to add files from seTuple to the target Tuple because
                 * it's not enough to surpass the threshold.
                 */
                if(requiredLFNNo + targetSeTuple.getCommonLFNNo() < threshold)
                    costOpsAdd = Integer.MAX_VALUE;

                /**
                 * Don't move all the files from one source tuple to a destination tuple if it doesn't move the
                 * destination over threshold.
                 */
                if(targetSeTuple.getCommonLFNNo() + seTuple.getCommonLFNNo() < threshold)
                    costOpsEliminate = Integer.MAX_VALUE;

                /**
                 * If there are no common LFNs left on the tuple, then make
                 * the cost of elimination Infinite.
                 */
                if(targetSeTuple.getCommonLFNNo() == 0)
                    costOpsEliminate = Integer.MAX_VALUE;

                if(cheapestOpsCost > Math.min(costOpsEliminate, costOpsAdd)) {
                    opsPossible = true;
                    if(costOpsEliminate < costOpsAdd) {
                        transferredLFNs = targetSeTuple.getCommonLFNNo();
                        source = targetSeTuple;
                        destination = seTuple;
                        cheapestOps = opsEliminate.second;
                        cheapestOpsCost = costOpsEliminate;
                    } else {
                        transferredLFNs = requiredLFNNo;
                        source = seTuple;
                        destination = targetSeTuple;
                        cheapestOps = opsAdd.second;
                        cheapestOpsCost = costOpsAdd;
                    }
                }
            }
            if(opsPossible) {
                //TODO: Append operations to the list.
                //operationList.add()

                System.out.println(source.getSeMemberNames() + " --" + (transferredLFNs * replicationFactor) + "-->" + destination.getSeMemberNames());
                for(Pair<StorageTupleMember, StorageTupleMember> ops : cheapestOps) {
                    System.out.println("       " + ops.first.getName() + " --" + transferredLFNs + "-->" + ops.second.getName());
                }

                //source.transferLFNs(destination, cheapest_ops, transferred_lfns)
            }
        }

        return operationList;
    }
}
