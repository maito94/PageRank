package rank.page;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlockUpdatePageRankMapper
        extends Mapper<Object, Text, Text, Text>
{
    private Text block = new Text();

    private Text node_boundarycondition = new Text();
    private Text node_pagerank = new Text();
    private Text node_destinations = new Text();
    private Text node_degree = new Text();

    // produces string of form: "Key=E Source=A SourceBlock=Block PageRank=0.20 Degree=3"
    private String stringValueForBoundaries(String key, String source, String sourceBlock, String sourcePageRank, String sourceDegree) {
        String bc = "";
        bc += "Key=" + key + " Source=" + source + " SourceBlock=" + sourceBlock + " PageRank=" + sourcePageRank + " Degree=" + sourceDegree;
        return bc;
    }

    // produces string of form: "Destinations=dest1,dest2"
    private String stringValueForDestinations(String key, String[] dests) {
        String d = "";
        d += "Key=" + key + " Destinations=" + StringUtils.join(",", dests);
        return d;
    }

    // produces string of form: "Key=key Degree=dest"
    private String stringValueForDegree(String key, String degree) {
        String d = "";
        d += "Key=" + key + " Degree=" + degree;
        return d;
    }

    // produces string of form: "Key=key PageRank=pagerank"
    private String stringValueForPagRank(String key, String pagerank) {
        String pr = "";
        pr += "Key=" + key + " PageRank=" + pagerank;
        return pr;
    }

    /**
     * Input format:
     *      Source  PageRank    Destinations
     *
     * Block:
     *      A       0.20        B C E
     *      B       0.20        D
     *      C       0.20        A B
     *      D       0.20        B C
     *
     * Block':
     *      E       0.20        A
     *
     *  Output Format:
     *      Key     Value (string)
     *  A:
     *      Block   "Key=A PageRank=0.20"
     *      Block   "Key=A Destinations=B,C,E"
     *      Block   "Key=A Degree=3"
     *      Block'  "Key=E Source=A SourceBlock=Block PageRank=0.20 Degree=3"
     *  B:
     *      Block   "Key=B PageRank=0.20"
     *      Block   "Key=B Destinations=D"
     *      Block   "Key=B Degree=1"
     *  C:
     *      Block   "Key=C PageRank=0.20"
     *      Block   "Key=C Destinations=A,B"
     *      Block   "Key=C Degree=2"
     *  D:
     *      Block   "Key=D PageRank=0.20"
     *      Block   "Key=D Destinations=B,C"
     *      Block   "Key=D Degree=2"
     *  E
     *      Block'  "Key=E PageRank=0.20"
     *      Block'  "Key=E Destinations=A"
     *      Block'  "Key=E Degree=1"
     *      Block   "Key=A Source=E SourceBlock=Block' PageRank=0.20 Degree=1"
     */
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException
    {
//        Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+\\.\\d+)\\s+((\\d+\\s*)*)$");
        Pattern pattern = Pattern.compile("^ *(\\d+)\\s+(\\d+\\.\\d+(?:E-\\d+)?+)\\s+((\\d+\\s*)*)$");
        Matcher matcher = pattern.matcher(value.toString());

        if (matcher.find() && matcher.groupCount() > 0) {
            // get source node and set the block
            String source = matcher.group(PageRank.SOURCE_INDEX);
            Integer blockID = PageRank.getBlockID( Integer.valueOf(source) );
            block.set( String.valueOf(blockID) );

            // get list of destination nodes
            String destinations = matcher.group(PageRank.DESTINATIONS_INDEX);
            String[] destinations_arr = destinations.split("\\s+");

            // get pagerank of current node to setup current_pagerank value and source_pagerank value
            String pagerank = matcher.group(PageRank.PAGE_RANK_INDEX);

            // outputs: (Block, Key=A PageRank=0.20) key value pair
            node_pagerank.set( stringValueForPagRank(source, pagerank) );
            context.write(block, node_pagerank);

            // outputs: (Block, Key=A Degree=3) key value pair
            String degrees = String.valueOf( destinations_arr.length );
            node_degree.set( stringValueForDegree(source, degrees) );
            context.write(block, node_degree);

            // outputs: (Block, Key=A Destinations=B,C,E) key value pair
            node_destinations.set( stringValueForDestinations(source, destinations_arr) );
            context.write(block, node_destinations);


            for (String dest : destinations_arr) {
                if (!dest.isEmpty()) {
                    Integer dest_blockID = PageRank.getBlockID(Integer.valueOf(dest));
                    if (blockID != dest_blockID) {
                        // outputs: (Block', Key=E Source=A SourceBlock=Block PageRank=0.20 Degree=3) key value pair
                        block.set(String.valueOf(dest_blockID));
                        node_boundarycondition.set(stringValueForBoundaries(dest, source, String.valueOf(blockID), pagerank, degrees));
                        context.write(block, node_boundarycondition);
                    }


//                 System.out.println("-----------------------------");
//                 System.out.println("BC: " + node_boundarycondition.toString());
//                 System.out.println("-----------------------------");
                }
            }
        }

    }
}
