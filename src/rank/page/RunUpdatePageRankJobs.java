package rank.page;

import java.io.*;
import java.nio.charset.StandardCharsets;

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
        Configuration conf = new Configuration();
        conf.setInt("EXPECTED_NODES", PageRank.EXPECTED_NODES);
//        conf.setInt("BASE_ACCURACY", PageRank.BASE_ACCURACY);

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
        int iteration = 0;

        StringBuilder sb = new StringBuilder();
        while (iteration < maxIterations) {
            Job job = createSingleUpdatePageRankJob(iteration, inputDirectory, outputDirectory);
            job.waitForCompletion(true);

            // need to use createUpdatePageRankJob output directory as new directory for chaining results
            inputDirectory = outputDirectory + "_" + iteration;

            // calculate average residual by getting counter and dividing by number of nodes
            Counters jobCounters = job.getCounters();

            // covert long to double by dividing by base and added accuracy during reduce phase
            Long long_aggregate_residuals = jobCounters.findCounter(PageRank.PageRankEnums.AGGREGATE_RESIDUALS).getValue();

            Double aggregate_residuals = (double)long_aggregate_residuals / (double)(PageRank.ADDED_ACCURACY);//(double)(PageRank.BASE_ACCURACY * PageRank.ADDED_ACCURACY);
            Double average_residual = aggregate_residuals / PageRank.EXPECTED_NODES;

            System.out.println("Iteration " + iteration + " avg error " + average_residual);
            sb.append("Iteration " + iteration + " avg error " + average_residual);

            if (average_residual < PageRank.EPSILON) {
                break;
            }

            iteration++;
        }

        // Write out the average residuals calculated for each iteration
        try {
            System.out.println("PageRank.outputFile: " + PageRank.outputFile);

            File outputFile = new File(PageRank.outputFile);
            FileOutputStream fos = new FileOutputStream(outputFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            ByteArrayInputStream bais = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));

            int bytesRead;
            byte[] buffer = new byte[1024];
            while ((bytesRead = bais.read(buffer)) != -1)
                bos.write(buffer, 0, bytesRead);

            bos.flush();
            bos.close();
            bais.close();

            S3Wrapper.uploadFile(PageRank.BUCKET_NAME, new File(PageRank.outputFile));
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
            System.out.println("RunUpdatePageRankJobs: ERROR writing results");
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
        Configuration conf = new Configuration();
        conf.setInt("EXPECTED_NODES", PageRank.EXPECTED_NODES);
//        conf.setInt("BASE_ACCURACY", PageRank.BASE_ACCURACY);

        Job update_job = Job.getInstance(conf, "pagerank_update");
        update_job.setJarByClass(RunUpdatePageRankJobs.class);

        update_job.setMapperClass(BlockUpdatePageRankMapper.class);
        update_job.setMapOutputKeyClass(Text.class);
        update_job.setMapOutputValueClass(Text.class);

        update_job.setReducerClass(BlockUpdatePageRankReducer.class);
        update_job.setOutputKeyClass(Text.class);
        update_job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(update_job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(update_job, new Path(outputDirectory + "_" + jobId));

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
        int iteration = 0;

        StringBuilder sb = new StringBuilder();
        while (iteration < maxIterations) {
            Job job = createBlockUpdatePageRankJob(iteration, inputDirectory, outputDirectory);
            job.waitForCompletion(true);

            // need to use createUpdatePageRankJob output directory as new directory for chaining results
            inputDirectory = outputDirectory + "_" + iteration;

            // calculate average residual by getting counter and dividing by number of nodes
            Counters jobCounters = job.getCounters();

            // covert long to double by dividing by base and added accuracy during reduce phase
            Long long_aggregate_pagerank_residuals = jobCounters.findCounter(PageRank.PageRankEnums.AGGREGATE_ITERATION_PAGERANKS_RESIDUALS).getValue();
            Double aggregate_pagerank_residuals = (double)long_aggregate_pagerank_residuals / (double)(PageRank.ADDED_ACCURACY);//(double)(PageRank.BASE_ACCURACY * PageRank.ADDED_ACCURACY);

            Double average_residual = aggregate_pagerank_residuals / PageRank.EXPECTED_NODES;
            sb.append("Iteration " + iteration + " avg error " + average_residual + "\n");

            Long aggragate_iterations = jobCounters.findCounter(PageRank.PageRankEnums.AGGREGATE_BLOCK_ITERATIONS).getValue();
            Double average_iterations = (double)aggragate_iterations / (double)PageRank.BLOCKID_BOUNDARIES.length;
            sb.append("Iteration " + iteration + " avg block iterations " + average_iterations + "\n");

            System.out.println("Iteration " + iteration + " avg error " + average_residual);
            System.out.println("Iteration " + iteration + " avg block iterations " + average_iterations);

            if (aggregate_pagerank_residuals < PageRank.EXPECTED_NODES * PageRank.EPSILON) {
                break;
            }

            iteration++;
        }

        // Write out the average residuals calculated for each iteration
        try {
            File outputFile = new File(PageRank.outputFile);
            FileOutputStream fos = new FileOutputStream(outputFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            ByteArrayInputStream bais = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));

            int bytesRead;
            byte[] buffer = new byte[1024];
            while ((bytesRead = bais.read(buffer)) != -1)
                bos.write(buffer, 0, bytesRead);

            bos.flush();
            bos.close();
            bais.close();

            S3Wrapper.uploadFile(PageRank.BUCKET_NAME, new File(PageRank.outputFile));
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }

        return iteration;
    }
}
