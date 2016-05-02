package rank.page;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

    public enum PageRankEnums {
        AGGREGATE_RESIDUALS,
        AGGREGATE_ITERATION_PAGERANKS_RESIDUALS,
        AGGREGATE_BLOCK_ITERATIONS
    }

    public static final String ACCESS_KEY_ID = "AKIAIAL2EQ2C75T5TOTQ";
    public static final String SECRET_ACCESS_KEY = "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";

    // compute filter parameters for netid mag399
    public static double fromNetID = 0.993; // 993 is 399 reversed

    public static double rejectMIN = 0.9 * fromNetID;
    public static double rejectLIMIT = rejectMIN + 0.01;

    public static double DAMPING_FACTOR = 0.85;
    public static double EPSILON = 0.0001;

    public static final Integer SOURCE_INDEX = 1;
    public static final Integer PAGE_RANK_INDEX = 2;
    public static final Integer DESTINATIONS_INDEX = 3;

    public static int EXPECTED_NODES;
    public static int MAX_ITERATIONS;

    public static String BUCKET_NAME;
    public static String inputFile;
    public static String outputDirectory;
    public static String outputFile;

    //    public static Integer BASE_ACCURACY;
    public static Integer ADDED_ACCURACY = 1000000;

    public static Integer[] BLOCKID_BOUNDARIES = {
            10328,  20373,  30629,  40645,  50462,  60841,
            70591,  80118,  90497, 100501, 110567, 120945,
            130999, 140574, 150953, 161332, 171154, 181514,
            191625, 202004, 212383, 222762, 232593, 242878,
            252938, 263149, 273210, 283473, 293255, 303043,
            313370, 323522, 333883, 343663, 353645, 363929,
            374236, 384554, 394929, 404712, 414617, 424747,
            434707, 444489, 454285, 464398, 474196, 484050,
            493968, 503752, 514131, 524510, 534709, 545088,
            555467, 565846, 576225, 586604, 596585, 606367,
            616148, 626448, 636240, 646022, 655804, 665666,
            675448, 685230
    };


    public static Integer[] BLOCK_SIZES = {
            10328, 10045, 10256, 10016, 9817, 10379,
            9750, 9527, 10379, 10004, 10066, 10378,
            10054, 9575, 10379, 10379, 9822, 10360,
            10111, 10379, 10379, 10379, 9831, 10285,
            10060, 10211, 10061, 10263, 9782, 9788,
            10327, 10152, 10361, 9780, 9982, 10284,
            10307, 10318, 10375, 9783, 9905, 10130,
            9960, 9782, 9796, 10113, 9798, 9854,
            9918, 9784, 10379, 10379, 10199, 10379,
            10379, 10379, 10379, 10379, 9981, 9782,
            9781, 10300, 9792, 9782, 9782, 9862,
            9782, 9782
    };


    private static Integer getMinPower10(Double pr) {
        return (pr < 1.0) ? 1 + getMinPower10(pr * 10.0) : 0;
    }


    private static BufferedReader getFileReader(String filepath) {
        File file = new File(filepath);
        if (file.exists() && file.isFile()) {
            try {
                FileReader fr = new FileReader(filepath);
                BufferedReader br = new BufferedReader(fr);
                return br;
            }
            catch (FileNotFoundException ex) {
                Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }


//    /**
//     * Constructs an ArrayList of block id boundaries
//     *
//     * @param filepath  path to file containing block sizes
//     * @return ArrayList containing block id boundaries
//     */
//    private static ArrayList<Integer> getBlockIDBoundariesFromFile(String filepath) {
//        ArrayList<Integer> block_boundaries = new ArrayList<>();
//        try (BufferedReader br = getFileReader(filepath)) {
//            int currentBlockIDBoundary = 0;
//
//            String line;
//            while (br != null && (line = br.readLine()) != null) {
//                line = line.trim();
//                Integer parsedBlockSize = Integer.parseInt(line);
//
//                block_boundaries.add(currentBlockIDBoundary + parsedBlockSize);
//                currentBlockIDBoundary += parsedBlockSize;
//
//                BLOCK_SIZES.add(parsedBlockSize);
//            }
//        } catch (IOException ex) {
//            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//        return block_boundaries;
//    }


    /**
     * Retrieves the index of the node within its block
     *
     * @param nodeID    node to look up index for
     * @param blockID   blockID of node
     * @return Index of node within block
     */
    public static Integer getNodeBlockIndex(Integer nodeID, Integer blockID) {
        Integer index_node = nodeID;

        if (0 < blockID) {
            index_node -= BLOCKID_BOUNDARIES[ blockID - 1 ];
        }

        return index_node;
    }


    /**
     * Retrieves the node id given the index into a block
     *
     * @param nodeIndex index of node within block
     * @param blockID   blockID of node
     * @return node id
     */
    public static Integer getNodeIDFromIndex(Integer nodeIndex, Integer blockID) {
        Integer nodeid = nodeIndex;

        if (0 < blockID) {
            nodeid += BLOCKID_BOUNDARIES[ blockID - 1 ];
        }

        return nodeid;
    }


    /**
     * Retrieves the block id associated with the node
     *
     * @param nodeID     node to look up block id for
     * @return Block id corresponding to node
     */
    public static Integer getBlockID(Integer nodeID) {
        Integer blockID = 0;

        for (Integer blockBoundary : BLOCKID_BOUNDARIES) {
            if (nodeID < blockBoundary) {
                break;
            }
            blockID++;
        }

        return blockID;
    }


    public static class FileMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text source = new Text();
        private Text destination = new Text();

        private String empty_string = "";

        public boolean selectInputLine(double x) {
            return ( (rejectMIN <= x && x < rejectLIMIT) ? false : true );
        }

        /**
         * Map the original file to source and destination pairs.
         * Input format:
         *      Source  Destination Randomization
         *
         * Output format:
         *      Source  Destination if Randomization not filtered
         *      Source  EMPTY       if Randomization is filtered
         *
         *      Source BACKUP       if Destinations is dangling node
         *
         * @param key     Current line offset to file
         * @param value   Text at line offset in file
         */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {

            Pattern pattern = Pattern.compile("^ *(\\d+) +(\\d+) +(\\d+\\.\\d+)$");
            Matcher matcher = pattern.matcher(value.toString());

            if (matcher.find() && matcher.groupCount() >= 3) {
                source.set(matcher.group(1));
                double parsedDouble = Double.parseDouble(matcher.group(3));

                // only select lines that are not within reject boundaries
                if (selectInputLine(parsedDouble)) {
                    destination.set(matcher.group(2));
                    context.write(source, destination);
                }

                // still need to add the node with empty string to avoid completely filtering out the node during
                // page rank calculations
                else {
                    destination.set(empty_string);
                    context.write(source, destination);
                }

                source.set(matcher.group(2));
                destination.set("BACKUP");
                context.write(source, destination);
            }
        }
    }


    public static class FileReducer
            extends Reducer<Text, Text, Text, Text>
    {

        private Text result = new Text();

        /**
         * Reduce inputs to create a modified format of the original file.
         * Input format:
         *      Source  Destinations
         *      A       [ B, C ]
         *      B       [ D ]
         *      C       [ A, B ]
         *      D       [ B, C ]
         *
         * Output format:
         *      Source  PageRank    Destinations
         *      A       0.25        B C
         *
         *      B       0.25
         *
         *      B       0.25        D
         *      C       0.25        A B
         *      D       0.25        B C
         * @param key       Source node
         * @param values    List of outgoing links from source node
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            List<String> destinations = new ArrayList<>();

            for (Text d : values)
            {
                // add destination to list only if it is not the empty string
                if (!d.toString().isEmpty() && !d.toString().equals("BACKUP")) {
                    destinations.add(d.toString());
                }
            }

            String[] dest_array = destinations.toArray(new String[0]);
            String dest_result = StringUtils.join(" ", dest_array);

            Configuration conf = context.getConfiguration();

            Double page_rank = 1.0 / conf.getInt("EXPECTED_NODES", 1);
            String pr_string = String.valueOf(page_rank);

            result.set(pr_string + " " + dest_result);
            context.write(key, result);
        }
    }


    /**
     * Create a map-reduce job whose purpose is to format the original input file
     */
    public static Job createInitializationJob(String inputDirectory, String outputDirectory)
            throws IOException
    {
        Configuration conf = new Configuration();
        conf.setInt("EXPECTED_NODES", EXPECTED_NODES);

        Job init_job = Job.getInstance(conf, "pagerank_init");
        init_job.setJarByClass(PageRank.class);

        init_job.setMapperClass(FileMapper.class);
        init_job.setMapOutputKeyClass(Text.class);
        init_job.setMapOutputValueClass(Text.class);

        init_job.setReducerClass(FileReducer.class);
        init_job.setOutputKeyClass(Text.class);
        init_job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(init_job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(init_job, new Path(outputDirectory));

        return init_job;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("----- Executing -----");

        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Boolean block_implementation = false;

        if (remainingArgs.length == 6 || remainingArgs.length == 7) {
            EXPECTED_NODES = Integer.parseInt(remainingArgs[0]);

//            BASE_ACCURACY = (int)Math.pow(10, getMinPower10(1.0 / EXPECTED_NODES));
//            System.out.println("BASE_ACCURACY: " + BASE_ACCURACY);

            MAX_ITERATIONS = Integer.parseInt(remainingArgs[1]);

            BUCKET_NAME = remainingArgs[2];

            inputFile = "s3n://" + BUCKET_NAME + "/" + remainingArgs[3];
//            inputFile = remainingArgs[3];

            outputDirectory = "s3n://" + BUCKET_NAME + "/" + remainingArgs[4];
//            outputDirectory = remainingArgs[4];

            outputFile = remainingArgs[5];

            if (remainingArgs.length == 7) {
                block_implementation = true;
//                BLOCKID_BOUNDARIES = getBlockIDBoundariesFromFile( remainingArgs[6] );
            }
        }
        else {
            System.err.println("Usage:");
            System.err.println("\tpagerank <n> <i> <bucket name> <in file> <out directory> <out file> ");
            System.err.println("\tpagerank <n> <i> <bucket name> <in file> <out directory> <out file> blocks");
            System.err.println("Where:");
            System.err.println("\t<n> is the number of nodes.");
            System.err.println("\t<i> is the number of maximum iterations to run.");
            System.err.println("\t<bucket name> is the name of the bucket where the files are located.");
            System.err.println("\t<in file> path to file with the list of edges located within the bucket.");
            System.err.println("\t<out directory> is the output directory to which Hadoop files are written to within the bucket.");
            System.err.println("\t<out file> is the output file to which the average residuals will be written to within the bucket.");
            System.err.println("\tblocks - string literal to indicate block computation.");
            System.err.println("\t\tIf this is provided, then the blocks version implementation will be run.");
            System.exit(2);
        }

        // add one more level to outputDirectory so Hadoop output files are in directory rather then on the same level
        // as the directory
        outputDirectory += "/out";

        // initialize the file to the desired format for our map reduce
        Job initJob = createInitializationJob(inputFile, outputDirectory);
        initJob.waitForCompletion(true);

        if (block_implementation) {
            RunUpdatePageRankJobs.runBlockUpdatePageRankJobs(MAX_ITERATIONS, outputDirectory, outputDirectory);
        }
        else {
            // start running page rank iterations
            RunUpdatePageRankJobs.runSingleUpdatePageRankJobs(MAX_ITERATIONS, outputDirectory, outputDirectory);
        }

        System.out.println("----- Completed -----");
        System.exit(0);
    }
}
