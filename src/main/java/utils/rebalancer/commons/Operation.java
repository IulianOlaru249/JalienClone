package utils.rebalancer.commons;

public class Operation {

    private StorageTupleMember source;
    private StorageTupleMember destination;
    private double cost;
    private int transferedLFNs;

    public Operation(StorageTupleMember source, StorageTupleMember destination, double cost, int transferedLFNs) {
        this.source = source;
        this.destination = destination;
        this.cost = cost;
        this.transferedLFNs = transferedLFNs;
    }

    public String toString() {
        return this.source.getName() + "--" + this.transferedLFNs
                + "-->" + this.destination.getName() + ": " + this.cost;
    }
}
