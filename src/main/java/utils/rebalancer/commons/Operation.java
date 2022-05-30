package utils.rebalancer.commons;

public class Operation {

    private StorageTuple sourceParentTuple;
    private StorageTupleMember source;

    private StorageTuple destinationParentTuple;
    private StorageTupleMember destination;

    private double cost;
    private int transferedLFNs;

    public Operation(StorageTuple sourceParentTuple, StorageTuple destinationParentTuple, StorageTupleMember source, StorageTupleMember destination, double cost, int transferedLFNs) {
        this.sourceParentTuple = sourceParentTuple;
        this.destinationParentTuple = destinationParentTuple;
        this.source = source;
        this.destination = destination;
        this.cost = cost;
        this.transferedLFNs = transferedLFNs;
    }

    public StorageTuple getSourceParentTuple() {
        return sourceParentTuple;
    }

    public StorageTupleMember getSource() {
        return source;
    }

    public StorageTuple getDestinationParentTuple() {
        return destinationParentTuple;
    }

    public StorageTupleMember getDestination() {
        return destination;
    }

    public double getCost() {
        return cost;
    }

    public int getTransferedLFNs() {
        return transferedLFNs;
    }

    public String toString() {
        return this.source.getName() + "--" + this.transferedLFNs
                + "-->" + this.destination.getName() + ": " + this.cost;
    }
}
