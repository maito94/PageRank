package rank.page;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;


public class PageRank {

    public enum PageRankEnums {
        AGGREGATE_RESIDUALS
    }

    public static double MIN = 0.00;
    public static double MAX = 1.00;
    public static int EXPECTED_NODES = 4;
    public static double DAMPING_FACTOR = 0.85;
    public static double EPSILON = 0.0001;

    public static final Integer SOURCE_INDEX = 1;
    public static final Integer PAGE_RANK_INDEX = 2;
    public static final Integer DESTINATIONS_INDEX = 3;

    public static String inputDirectory = "/Users/mag94/Desktop/hadoop/edges.txt";
    public static String outputDirectory = "/Users/mag94/Desktop/hadoop/out/out_dir";

    public static String residualsOutputFile = "/Users/mag94/Desktop/hadoop/residuals.txt";


    public static class FileMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text source = new Text();
        private Text destination = new Text();

        /**
         * Map the original file to source and destination pairs.
         * Input format:
         *      Source  Destination Randomization
         *
         * Output format:
         *      Source  Destination if Randomization not filtered
         *      Source  EMPTY       if Randomization is filtered    TODO: NEED TO IMPLEMENT THIS
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

                double parsedDouble = Double.parseDouble(matcher.group(3));
                //TODO: should be !(MIN <= parsedDouble && parsedDouble <= MAX)
                if (MIN <= parsedDouble && parsedDouble <= MAX)
                    source.set(matcher.group(1));
                    destination.set(matcher.group(2));
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
         *      B       0.25        D
         *      C       0.25        A B
         *      D       0.25        B C
         * @param key       Source node
         * @param values    List of outgoing links from source node
         * TODO: After map todo is implemented, need to check whether value is empty string or not
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            List<String> destinations = new ArrayList<>();

            for (Text d : values)
            {
                destinations.add(d.toString());
            }

            String[] dest_array = destinations.toArray(new String[0]);
            String dest_result = StringUtils.join(" ", dest_array);

            Double page_rank = 1.0 / EXPECTED_NODES;
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
//        Job init_job = new Job(new Configuration(), "pagerank_init");
        Configuration conf = new Configuration();
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

//        init_job.setInputFormatClass(KeyValueTextInputFormat.class);
        return init_job;
    }

    public static void main(String[] args) throws Exception {

        System.out.println("----- Executing -----");

        Job initJob = createInitializationJob(inputDirectory, outputDirectory);
        initJob.waitForCompletion(true);

        RunUpdatePageRankJobs.runUpdatePageRankJobs(10, outputDirectory, outputDirectory);


        System.out.println("----- Completed -----");

    }
}
