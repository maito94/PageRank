package rank.page;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SingleUpdatePageRankMapper
        extends Mapper<Object, Text, Text, Text>
{
    private Text source = new Text();
    private Text source_pagerank_degree = new Text();
    private Text destination = new Text();


    // produces string of form: "Source=source PageRank=pagerank"
    private String valueForDestination(String src, String pagerank, Integer degree) {
        String src_pr = "";
        src_pr += "Source=" + src + " PageRank=" + pagerank + " Degree=" + String.valueOf(degree);
        return src_pr;
    }


    // produces string of form: "Destination=dest"
    private String valueForSource(String dest) {
        String d = "";
        d += "Destination=" + dest;
        return d;
    }


    /**
     * Input format:
     *      Source  PageRank    Destinations
     *      A       0.25        B C
     *      B       0.25        D
     *      C       0.25        A B
     *      D       0.25        B C
     *
     * In order to have enough information during reducer phase, need to output:
     *      1. Destination-source pairs with page rank from source
     *      2. Source to destination pairs to be able to reconstruct file format
     *  Output Format:
     *      Key     Value (string)
     *  A:
     *      B       Source=A PageRank=0.25 Degree=2
     *      C       Source=A PageRank=0.25 Degree=2
     *      A       Destination=B
     *      A       Destination=C
     *  B:
     *      D       Source=B PageRank=0.25 Degree=1
     *      B       Destination=D
     *  C:
     *      A       Source=C PageRank=0.25 Degree=2
     *      B       Source=C PageRank=0.25 Degree=2
     *      C       Destination=A
     *      C       Destination=B
     *  D:
     *      B       Source=D PageRank=0.25 Degree=2
     *      C       Source=D PageRank=0.25 Degree=2
     *      D       Destination=B
     *      D       Destination=C
     */
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException
    {

        Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+\\.\\d+)\\s+((\\d+\\s*)*)$");
//        Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+\\.\\d+)((\\s+\\d+)*)$");
        Matcher matcher = pattern.matcher(value.toString());

        if (matcher.find() && matcher.groupCount() > 0) {
            // get source node
            source.set(matcher.group(PageRank.SOURCE_INDEX));

            // get list of destination nodes
            String destinations = matcher.group(PageRank.DESTINATIONS_INDEX);
            String[] destinations_arr = destinations.split("\\s+");

            // get pagerank of current node and setup source_pagerank value
            String pagerank = matcher.group(PageRank.PAGE_RANK_INDEX);
            source_pagerank_degree.set( valueForDestination(source.toString(), pagerank, destinations_arr.length) );

            for (String dest : destinations_arr) {
                destination.set( dest );
                context.write(destination, source_pagerank_degree);

//                System.out.println("-----------------------------");
//                System.out.println("Destination: " + dest + ", Info: " + source_pagerank_degree.toString());

                destination.set( valueForSource(dest) );
                context.write(source, destination);

//                System.out.println("Source: " + source.toString() + ", Destination: " + destination.toString());
//                System.out.println("-----------------------------");
            }
        }
    }
}
