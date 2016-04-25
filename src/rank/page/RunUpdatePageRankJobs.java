package rank.page;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunUpdatePageRankJobs {

    /**
     * Create a single page rank map-reduce job to update the current page ranks.
     * @param jobId Some arbitrary number so that Hadoop can create a directory "<outputDirectory>/<jobname>_<jobId>
     *              for storage of intermediate files.
     * @param inputDirectory The input directory specified by the user through PageRank after the initial file
     *                       has been modified.
     * @param outputDirectory The output directory for which to write job results, specified by the user through
     *                        PageRank.
     */
    public static Job createSingleUpdatePageRankJob(int jobId, String inputDirectory, String outputDirectory)
            throws IOException
    {
//        Job init_job = new Job(new Configuration(), "pagerank_update");
        Configuration conf = new Configuration();
        Job update_job = Job.getInstance(conf, "pagerank_update");

        update_job.setJarByClass(RunUpdatePageRankJobs.class);

        update_job.setMapperClass(SingleUpdatePageRankMapper.class);
        update_job.setMapOutputKeyClass(Text.class);
        update_job.setMapOutputValueClass(Text.class);

        update_job.setReducerClass(SingleUpdatePageRankReducer.class);
        update_job.setOutputKeyClass(Text.class);
        update_job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(update_job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(update_job, new Path(outputDirectory + "_" + jobId));

//        init_job.setInputFormatClass(KeyValueTextInputFormat.class);
        return update_job;
    }


    /**
     * Run single pagerank map-reduce jobs until the average page rank residuals are less than the epsilon specified in
     * PageRank.
     * @param maxIterations   The maximum number of updates to execute before the program is stopped.
     * @param inputDirectory  The path to the directory from which to read the file of edges
     * @param outputDirectory The path to the directory to which to put Hadoop output files
     * @return The number of iterations that were executed.
     */
    public static int runSingleUpdatePageRankJobs(int maxIterations, String inputDirectory, String outputDirectory)
            throws IOException, InterruptedException, ClassNotFoundException
    {
//    	Job job = createUpdateJob((int)System.currentTimeMillis(), inputDirectory, outputDirectory);
        int iteration = 0;

        StringBuilder sb = new StringBuilder();
        while (iteration < maxIterations) {

//            Job job = createUpdatePageRankJob((int)System.currentTimeMillis(), inputDirectory, outputDirectory);
            Job job = createSingleUpdatePageRankJob(iteration, inputDirectory, outputDirectory);
            job.waitForCompletion(true);

            // need to use createUpdatePageRankJob output directory as new directory for chaining results
            inputDirectory = outputDirectory + "_" + iteration;

            // calculate average residual by getting counter and dividing by number of nodes
            Counters jobCounters = job.getCounters();
            Long long_aggregate_residuals = jobCounters.findCounter(PageRank.PageRankEnums.AGGREGATE_RESIDUALS).getValue();
            Double aggregate_residuals = Double.longBitsToDouble( long_aggregate_residuals );
            Double average_residual = aggregate_residuals / PageRank.EXPECTED_NODES;

            System.out.println("Average Residual: " + average_residual);
            sb.append("Iteration " + iteration + " avg error " + average_residual + "\n");

            if (average_residual < PageRank.EPSILON) {
                break;
            }

            iteration++;
        }

        // Write out the average residuals calculated for each iteration
        try {
            Files.write(Paths.get(PageRank.outputFile), sb.toString().getBytes());
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }

        return iteration;
    }


    /**
     * Create a blocked page rank map-reduce job to update the current page ranks.
     * @param jobId Some arbitrary number so that Hadoop can create a directory "<outputDirectory>/<jobname>_<jobId>
     *              for storage of intermediate files.
     * @param inputDirectory The input directory specified by the user through PageRank after the initial file
     *                       has been modified.
     * @param outputDirectory The output directory for which to write job results, specified by the user through
     *                        PageRank.
     */
    public static Job createBlockUpdatePageRankJob(int jobId, String inputDirectory, String outputDirectory)
            throws IOException
    {
//        Job init_job = new Job(new Configuration(), "pagerank_update");
        Configuration conf = new Configuration();
        Job update_job = Job.getInstance(conf, "pagerank_update");

        update_job.setJarByClass(RunUpdatePageRankJobs.class);

        update_job.setMapperClass(BlockUpdatePageRankeMapper.class);
        update_job.setMapOutputKeyClass(Text.class);
        update_job.setMapOutputValueClass(Text.class);

        update_job.setReducerClass(BlockUpdatePageRankReducer.class);
        update_job.setOutputKeyClass(Text.class);
        update_job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(update_job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(update_job, new Path(outputDirectory + "_" + jobId));

//        init_job.setInputFormatClass(KeyValueTextInputFormat.class);
        return update_job;
    }


    /**
     * Run blocked pagerank map-reduce jobs until the average page rank residuals are less than the epsilon specified
     * in PageRank.
     * @param maxIterations   The maximum number of updates to execute before the program is stopped.
     * @param inputDirectory  The path to the directory from which to read the file of edges
     * @param outputDirectory The path to the directory to which to put Hadoop output files
     * @return The number of iterations that were executed.
     */
    public static int runBlockUpdatePageRankJobs(int maxIterations, String inputDirectory, String outputDirectory)
            throws IOException, InterruptedException, ClassNotFoundException
    {
//    	Job job = createUpdateJob((int)System.currentTimeMillis(), inputDirectory, outputDirectory);
        int iteration = 0;

        StringBuilder sb = new StringBuilder();
        while (iteration < maxIterations) {

//            Job job = createUpdatePageRankJob((int)System.currentTimeMillis(), inputDirectory, outputDirectory);
            Job job = createBlockUpdatePageRankJob(iteration, inputDirectory, outputDirectory);
            job.waitForCompletion(true);

            // need to use createUpdatePageRankJob output directory as new directory for chaining results
            inputDirectory = outputDirectory + "_" + iteration;

            // calculate average residual by getting counter and dividing by number of nodes
            Counters jobCounters = job.getCounters();
            Long long_aggregate_pagerank_residuals = jobCounters.findCounter(PageRank.PageRankEnums.AGGREGATE_ITERATION_PAGERANKS_RESIDUALS).getValue();
            Double aggregate_pagerank_residuals = Double.longBitsToDouble( long_aggregate_pagerank_residuals );

            Double average_residual = aggregate_pagerank_residuals / PageRank.EXPECTED_NODES;
            System.out.println("Average Residual: " + average_residual);
            sb.append("Iteration " + iteration + " avg error " + average_residual + "\n");

            Long aggragate_iterations = jobCounters.findCounter(PageRank.PageRankEnums.AGGREGATE_BLOCK_ITERATIONS).getValue();
            Double average_iterations = Double.valueOf(aggragate_iterations) / PageRank.BLOCKID_BOUNDARIES.size();
            sb.append("Iteration " + iteration + " avg block iterations " + average_iterations + "\n");

            if (aggregate_pagerank_residuals < PageRank.EXPECTED_NODES * PageRank.EPSILON) {
                break;
            }

            iteration++;
        }

        // Write out the average residuals calculated for each iteration
        try {
            Files.write(Paths.get(PageRank.outputFile), sb.toString().getBytes());
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }

        return iteration;
    }
}
