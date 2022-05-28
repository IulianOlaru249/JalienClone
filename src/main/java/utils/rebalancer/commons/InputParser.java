package utils.rebalancer.commons;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * InputParser class.
 */
public class InputParser {

    public InputParser() {
    }

    /**
     *
     * Return a map of distances between SEs.
     */
    public static Map<Pair<String, String>, Double> getDistances(String distancesFile) {
        Map<Pair<String, String>, Double> distances = new HashMap<>();

        Map<Pair<String, String>, Double> sitesToStorages = new HashMap<>();

        Map<String, List<String>> aliases = new HashMap<>();

        List<String> allSEs = new ArrayList<>();

        String line = "";
        String splitBy = ",";

        try {
            BufferedReader br = new BufferedReader(new FileReader(distancesFile));
            while ((line = br.readLine()) != null)
            {
                String[] row = line.split(splitBy);

                sitesToStorages.put(Pair.of(row[0], row[1]), Double.parseDouble(row[2]));

                if(!allSEs.contains(row[1])) {
                    allSEs.add(row[1]);
                }

                String seSite = row[1].split("::")[1];
                List<String> aliasValues;
                if(row[0].contains(seSite)) {
                    if(aliases.containsKey(seSite)) {
                        aliasValues = aliases.get(seSite);
                        aliasValues.add(row[0]);
                        aliases.put(seSite, aliasValues);
                    } else {
                        aliases.put(seSite, new ArrayList<>(Arrays.asList(row[0])));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String firstSE : allSEs) {
            for(String secondSE : allSEs) {
                String firstSeSite = firstSE.split("::")[1];

                if(firstSE.equals(secondSE))
                    continue;

                if(!aliases.containsKey(firstSeSite))
                    continue;

                List<String> siteAliases = aliases.get(firstSeSite);

                for(String siteAlias : siteAliases) {
                    if(sitesToStorages.containsKey(Pair.of(siteAlias, secondSE)))
                        firstSeSite = siteAlias;
                }

                distances.put(Pair.of(firstSE.toUpperCase(), secondSE.toUpperCase()),
                                        sitesToStorages.get(Pair.of(firstSeSite, secondSE)));
            }
        }

        return distances;
    }

    /**
     * Return a list of StorageTuple elements with their corresponding LFNs.
     * @param whereisFile
     * @param seListFile
     * @return
     */
    public static List<StorageTuple> getStorageElements(String whereisFile, String seListFile) {
        List<StorageTuple> tuples = new ArrayList<>();
        Map<String, Pair<Double, Double>> sesMetadata = new HashMap<>();
        Map<String, StorageTupleMember> sesNamesToData = new HashMap<>();
        Map<String, StorageTuple> sesNamesToTuple = new HashMap<>();

        /*TODO 1: Open seListFile and read from it*/
        String line = "";
        String splitBy = ",";

        try {
            BufferedReader br = new BufferedReader(new FileReader(seListFile));
            while ((line = br.readLine()) != null)
            {
                String[] row = line.split(splitBy);
                String seName = row[0].toUpperCase();
                Double writeDemotion = Double.parseDouble(row[2]);
                Double readDemotion = Double.parseDouble(row[3]);
                sesMetadata.put(seName, Pair.of(writeDemotion, readDemotion));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*TODO 2: Open whereis File and read from it*/
        try {
            BufferedReader br = new BufferedReader(new FileReader(whereisFile));
            while ((line = br.readLine()) != null)
            {
                String[] row = line.split(splitBy);
                List<String> sesList = null;
                String[] sesArray;
                String lfn = row[1];

                if(row[2].contains(";")) {
                    sesArray = row[2].split(";");
                    Arrays.sort(sesArray);
                    sesList = Arrays.asList(sesArray);
                } else {
                    sesList.add(row[2]);
                }

                //Convert sesList, which is a list of storage elements -> to a tuple format:
                String sesTupleJoined = String.join(",", sesList);

                if(sesNamesToTuple.containsKey(sesTupleJoined)) {
                    sesNamesToTuple.get(sesTupleJoined).addCommonLfn(lfn);
                    for(String seName : sesList)
                        sesNamesToData.get(seName).addPFN(seName);
                    continue;
                } else {
                    sesNamesToTuple.put(sesTupleJoined, new StorageTuple());
                    sesNamesToTuple.get(sesTupleJoined).addCommonLfn(lfn);
                }

                /*Go through each storage where the file is (since a file can have replicas) */
                for(String seName : sesList) {
                    /*If already initiated, just add it to the lfn pool*/
                    if(sesNamesToData.containsKey(seName)) {
                        sesNamesToData.get(seName).addPFN(lfn);
                        sesNamesToTuple.get(sesTupleJoined).addSEMember(sesNamesToData.get(seName));
                    }
                    /*Else create new*/
                    else {

                        //TODO: Refactor this
                        StorageTupleMember newSe = new StorageTupleMember(seName, 0, null, null, null);
                        newSe.addPFN(lfn);

                        //Keep track of every se
                        sesNamesToData.put(seName, newSe);

                        //Add new storage to tuple
                        sesNamesToTuple.get(sesTupleJoined).addSEMember(newSe);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(Map.Entry<String, StorageTuple> entry : sesNamesToTuple.entrySet()) {
            tuples.add(entry.getValue());
        }

        return tuples;
    }
}
