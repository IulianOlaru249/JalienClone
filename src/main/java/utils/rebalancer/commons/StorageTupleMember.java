package utils.rebalancer.commons;

import alien.se.SE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Storage Tuple Member
 * Extends the SE class to be able to compute the theoretical cost of transfering files between them
 *
 * @author
 * @since
 */
public class StorageTupleMember extends SE {

    /**
     * The PFN pool for this SE
     */
    private int numberOfPFNs = 0;
    private List<String> PFNs = new ArrayList<>();

    /**
     * Build an arbitrary SE from the corresponding fields
     *
     * @param seName
     * @param seNumber
     * @param qos
     * @param seStoragePath
     * @param seioDaemons
     */
    public StorageTupleMember(String seName, int seNumber, String qos, String seStoragePath, String seioDaemons) {
        super(seName, seNumber, qos, seStoragePath, seioDaemons);
    }

    /**
     *
     * @return
     */
    public List<String> getPFNs() {
        return this.PFNs;
    }

    /**
     *
     * @param newPFN
     */
    public void addPFN(String newPFN) {
        this.PFNs.add(newPFN);
        this.numberOfPFNs ++;
    }


    /**
     * TODO!
     * @param targetSE
     */
    public void transferPFNs(StorageTupleMember targetSE) {
        this.numberOfPFNs --;
        targetSE.numberOfPFNs ++;
    }

    /**
     * This is used to compute the cost of transfering @transferedLFNNo between this SE and @targetSE
     *
     * @param targetSE
     * @param distances
     * @param transferedLFNNo
     * @param withWriteDemotion
     * @param readDemotionWeight
     * @param distanceWeight
     * @param writeDemotionWeight
     *
     * @return the cost of the operation
     */
    public Double getOPSCost( StorageTupleMember targetSE, Map<Pair<String, String>, Double> distances,
            int transferedLFNNo, boolean withWriteDemotion,
            int readDemotionWeight, int distanceWeight, int writeDemotionWeight) {

        /**
         * When this is false, the write demotion is not taken into account.
         * This is done in order to avoid returning a negative cost, since some
         * write demotion calues can be negative.
         *
         * Allows user whether to take into account the negative WD or not.
         * Example usecase:
         *  I.  User wants to compare the cost of two ops. Negative WD should be taken into account.
         *  II. User wants to compare the "real" cost of a series of ops. Negative WD should not be
         *      taken into account since the negative cost they can produce does not make sense
         *      in this context.
         */
        if (!withWriteDemotion) {
            distanceWeight += writeDemotionWeight;
            writeDemotionWeight = 0;
        }

        /**
         * "Loopback" condition
         * Does not cost anything to keep files locally
         */
        if (this.getName().equals(targetSE.getName())) {
            return (double) 0;
        }

        Double distance = distances.get(Pair.of(this.getName(), targetSE.getName()));

        return transferedLFNNo *
                ( (readDemotionWeight * this.demoteRead + distanceWeight * distance + writeDemotionWeight * targetSE.demoteWrite)
                / (readDemotionWeight + distanceWeight + writeDemotionWeight) );
    }
}
