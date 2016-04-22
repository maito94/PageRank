package rank.page;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunUpdatePageRankJobs {


    private static BufferedReader getFileReader(String filepath) {
        File file = new File(filepath);
        if (file.exists() && file.isFile()) {
            try {
                FileReader fr = new FileReader(filepath);
                BufferedReader br = new BufferedReader(fr);
                return br;
            }
            catch (FileNotFoundException ex) {
                Logger.getLogger(RunUpdatePageRankJobs.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }


    public static ArrayList<Double> getPageRanks(String inputDirectory) {
        ArrayList<Double> pageranks = new ArrayList<>();


        File dir = new File(inputDirectory);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File file : directoryListing) {
                // Do something with child
                if (file.exists() && file.isFile()) {
                    BufferedReader br = getFileReader(file.getPath());
                    String line;
                    try {
                        while ((line = br.readLine()) != null) {
                            Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+\\.\\d+)\\s+((\\d+\\s*)*)$");
                            Matcher matcher = pattern.matcher(line);

                            if (matcher.find() && matcher.groupCount() > 0) {
                                // get pagerank of current node and setup source_pagerank value
                                String pagerank = matcher.group(PageRank.PAGE_RANK_INDEX);
                                pageranks.add( Double.valueOf(pagerank) );
                            }
                        }
                    }
                    catch (IOException ex) {
                        Logger.getLogger(RunUpdatePageRankJobs.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                else {
                    System.out.println("File does not exist.");
                }
            }
        } else {
            // Handle the case where dir is not really a directory.
            // Checking dir.isDirectory() above would not be sufficient
            // to avoid race conditions with another process that deletes
            // directories.
            System.out.println("File does not exist.");

        }


        return pageranks;
    }


    /**
     * Create a map-reduce job to update the current page ranks.
     * @param jobId Some arbitrary number so that Hadoop can create a directory "<outputDirectory>/<jobname>_<jobId>
     *              for storage of intermediate files.
     * @param inputDirectory The input directory specified by the user through PageRank after the initial file
     *                       has been modified.
     * @param outputDirectory The output directory for which to write job results, specified by the user through
     *                        PageRank.
     */
    public static Job createUpdatePageRankJob(int jobId, String inputDirectory, String outputDirectory)
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
     * Run the jobs until the page rank residuals are less than the epsilon specified in PageRank.
     * @param maxIterations   The maximum number of updates to execute before the program is stopped.
     * @param inputDirectory  The path to the directory from which to read the file of edges
     * @param outputDirectory The path to the directory to which to put Hadoop output files
     * @return The number of iterations that were executed.
     * TODO: currently residuals are calculated through iteration in this file, should be done using counters
     */
    public static int runUpdatePageRankJobs(int maxIterations, String inputDirectory, String outputDirectory)
            throws IOException, InterruptedException, ClassNotFoundException
    {
//    	Job job = createUpdateJob((int)System.currentTimeMillis(), inputDirectory, outputDirectory);
        int iteration = 0;

        while (iteration < maxIterations) {
            iteration++;
            boolean done = true;
            ArrayList<Double> old_pageranks = getPageRanks(inputDirectory);

            System.out.println("SIZE: " + old_pageranks);

//            Job job = createUpdatePageRankJob((int)System.currentTimeMillis(), inputDirectory, outputDirectory);
            Job job = createUpdatePageRankJob(iteration, inputDirectory, outputDirectory);
            job.waitForCompletion(true);

            inputDirectory = outputDirectory + "_" + iteration;
            ArrayList<Double> new_pageranks = getPageRanks(inputDirectory);


            for (int i=0; i<old_pageranks.size(); i++) {
                Double old_pagerank = old_pageranks.get(i);
                Double new_pagerank = new_pageranks.get(i);

                Double residual = Math.abs(old_pagerank - new_pagerank) / new_pagerank;

                System.out.println(i + ": " + residual);

                if (residual > PageRank.EPSILON) {
                    done = false;
                }
            }
            if (done == true) {
                break;
            }

        }

        return iteration;
    }
}
