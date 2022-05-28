package utils.rebalancer.commons;

import alien.config.ConfigUtils;
import utils.lfncrawler.LFNCrawler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Storage Tuple
 * a collection of storage elements with common files
 *
 * @author -
 * @since -
 */
public class StorageTuple {
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
     * Add a new storage element to the tuple
     *
     * @param newMember A new storage element object
     */
    public void addSEMember(StorageTupleMember newMember) {
        this.ses.add(newMember);
        this.sesNo ++;
    }

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
     * @param targetSE
     * @param distances
     * @param transferedLFNNo
     * @param withWriteDemotion
     * @param readDemotionWeight
     * @param distanceWeight
     * @param writeDemotionWeight
     * @return
     */
    public Double getOPSCost( StorageTupleMember targetSE, Map<Pair<String, String>, Double> distances,
                              int transferedLFNNo, boolean withWriteDemotion,
                              int readDemotionWeight, int distanceWeight, int writeDemotionWeight) {
        //TODO: get all combinations
        return (double)0;
    }
}
