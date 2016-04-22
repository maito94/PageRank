//package rank.page;
//
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//public class GraphUtils {
//    public static final String EDGES = "/Users/mag94/Desktop/hadoop/out_dir/";
////    public static final String EDGES = "/Users/mag94/Desktop/hadoop/edges.txt";
//
//    public static final String BLOCKS = "res/blocks.txt";
//
//    public static double MIN = 0.00;
//    public static double MAX = 1.00;
//
//    public static void main(String[] args) {
//        System.out.println("----- Executing -----");
//        printEdgeInterval();
//    }
//
//    /**
//     * Will read the edges.txt file line-by-line and print only those
//     * lines which fall within the inclusive interval [MIN,MAX].
//     */
//    public static void printEdgeInterval() {
//
//
//        File dir = new File(EDGES);
//        File[] directoryListing = dir.listFiles();
//        if (directoryListing != null) {
//            for (File file : directoryListing) {
//                if (file.exists() && file.isFile()) {
//                    try {
//                        BufferedReader br = getFileReader(file.getPath());
//                        String line;
//                        while ((line = br.readLine()) != null) {
//                            boolean withinInterval = edgeFilter(line);
////                    if (withinInterval)
////                        System.out.println(line);
//                        }
//                    }
//                    catch (IOException ex) {
//                        Logger.getLogger(GraphUtils.class.getName()).log(Level.SEVERE, null, ex);
//                    }
//                }
//                else {
//                    System.out.println("File does not exist.");
//                }
//            }
//        } else {
//            // Handle the case where dir is not really a directory.
//            // Checking dir.isDirectory() above would not be sufficient
//            // to avoid race conditions with another process that deletes
//            // directories.
//        }
//
//
////        File file = new File(EDGES);
//
//    }
//
//    /**
//     * Given a nodeID, will return the blockID of the corresponding block.
//     *
//     * NOTE: Will return -1 if no such block exists.
//     * @param nodeID
//     * @return
//     */
//    public static long blockIDofNode(long nodeID) {
//        try (BufferedReader br = getFileReader(BLOCKS)) {
//            int blockID = 0;
//            String line;
//            while ((line = br.readLine().trim()) != null) {
//                long parsedBlockSize = Long.parseLong(line);
//                if (nodeID < parsedBlockSize)
//                    return blockID;
//                nodeID -= parsedBlockSize;
//                blockID++;
//            }
//        } catch (IOException ex) {
//            Logger.getLogger(GraphUtils.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return -1;
//    }
//
//    private static boolean edgeFilter(String line) {
//        if (line == null)
//            return false;
//
////        Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+)\\s+(\\d+\\.\\d+)$");
//
////        Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+\\.\\d+)((\\s+\\d+)*)$");
////        Matcher matcher = pattern.matcher(line);
////
////        System.out.println("GroupCount: " + matcher.groupCount());
//
////        if (matcher.find() && matcher.groupCount() > 0) {
////            System.out.println(matcher.group(3));
//////            double parsedDouble = Double.parseDouble(matcher.group(3));
//////
//////            if (MIN <= parsedDouble && parsedDouble <= MAX)
//////                return true;
////        }
//
//
//
//
//
//
//        Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+\\.\\d+)\\s+((\\d+\\s*)*)$");
//        Matcher matcher = pattern.matcher(line.toString());
//
//        if (matcher.find() && matcher.groupCount() > 0) {
//            // get source node
//            String source = matcher.group(1);
//
//            // get list of destination nodes
////            destinations.set(matcher.group(3));
////            String[] destinations_arr = destinations.toString().split("\\s+");
//            System.out.println(matcher.group(3));
//            String destinations = matcher.group(3);
//            String[] destinations_arr = destinations.split("\\s+");
//
//            // get pagerank of current node and setup source_pagerank value
//            String pagerank = matcher.group(2);
////            source_pagerank_degree.set(valueForDestination(source.toString(), pagerank, destinations_arr.length));
//
//            System.out.println("ARRAY_LENGTH: " + destinations_arr.length);
//
//            for (String dest : destinations_arr) {
//                String destination = ( dest );
////                context.write(destination, source_pagerank_degree);
//
//                System.out.println("-----------------------------");
//
//                System.out.println("Destination: " + dest);
//
////                destination.set( valueForSource(dest) );
////                context.write(source, destination);
//
//                System.out.println("Source: " + source + ", Destination: " + destination);
//
//                System.out.println("-----------------------------");
//
//            }
//        }
//        return true;
//    }
//
//    private static BufferedReader getFileReader(String filepath) {
//        File file = new File(filepath);
//        if (file.exists() && file.isFile()) {
//            try {
//                FileReader fr = new FileReader(filepath);
//                BufferedReader br = new BufferedReader(fr);
//                return br;
//            }
//            catch (FileNotFoundException ex) {
//                Logger.getLogger(GraphUtils.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
//        return null;
//    }
//}
