package rank.page;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SingleUpdatePageRankReducer
    extends Reducer<Text, Text, Text, Text>
{
    private Text result = new Text();


    /**
     * Input format:
     *      Key     Values (list of strings)
     *      A       [ "PageRank=0.25", "Destination=B", "Destination=C", "Source=C PageRank=0.25 Degree=2" ]
     *      B       [ "PageRank=0.25", "Destination=D", "Source=A PageRank=0.25 Degree=2", "Source=C PageRank=0.25 Degree=2", "Source=D PageRank=0.25 Degree=2" ]
     *      C       [ "PageRank=0.25", "Destination=A", "Destination=B", "Source=A PageRank=0.25 Degree=2", "Source=D PageRank=0.25 Degree=2" ]
     *      D       [ "PageRank=0.25", "Destination=B", "Destination=C", "Source=B PageRank=0.25 Degree=1" ]
     *  With the given input values, reconstruct the same structure from the original file with updated page rank values
     *  Output format:
     *      Source  PageRank    Destinations
     *      A       0.125       B C
     *      B       0.375       D
     *      C       0.25        A B
     *      D       0.25        B C
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        List<String> destinations = new ArrayList<>();
        Double old_pagerank = 0.0;
        Double updated_pagerank = 0.0;

        // info either contains:
        //      1. "Destination=X"
        //      2. "PageRank=Y"
        //      3. "Source=Y PageRank=m.mm Degree=n"
        for (Text info : values) {
            String info_str = info.toString();
            String[] info_arr = info_str.split("\\s+");

            // info contains "Destination=X" or "PageRank=Y"
            if (info_arr.length == 1) {

                if (info_arr[0].contains("PageRank")) {
                    int index_old_pagerank = info_str.indexOf('=');
                    old_pagerank = Double.valueOf( info_str.substring(index_old_pagerank + 1) );
                }
                else {
                    int index_destination = info_str.indexOf('=');
                    destinations.add( info_str.substring(index_destination + 1) );
                }
            }
            // info contains "Source=Y PageRank=m.mm Degree=n"
            else {
                int index_source = info_arr[0].indexOf('=');
                String source = info_arr[0].substring(index_source + 1);

                int index_pagerank = info_arr[1].indexOf('=');
                String pagerank = info_arr[1].substring(index_pagerank + 1);

                int index_degree = info_arr[2].indexOf('=');
                String degree = info_arr[2].substring(index_degree + 1);

                Double source_pagerank = Double.valueOf(pagerank);
                Double source_degree = Double.valueOf(degree);
                updated_pagerank += source_pagerank/source_degree;

            }
        }

        // apply damping effect
        // (1 - d)/N + d(updated_pagerank)
        updated_pagerank = (1.0 - PageRank.DAMPING_FACTOR)/PageRank.EXPECTED_NODES + PageRank.DAMPING_FACTOR*updated_pagerank;
        String pagerank_str = String.valueOf(updated_pagerank);

        // calculate residual for current node
        Double residual = Math.abs(old_pagerank - updated_pagerank) / updated_pagerank;

        // need to get running residual and convert it to double to increment value
        Long long_running_residual = context.getCounter(PageRank.PageRankEnums.AGGREGATE_RESIDUALS).getValue();
        Double running_residual = Double.longBitsToDouble(long_running_residual);

        // overwrite counter for running residual
        Long long_residual = Double.doubleToLongBits( residual + running_residual );
        context.getCounter(PageRank.PageRankEnums.AGGREGATE_RESIDUALS).setValue(long_residual);

        // get list of outgoing links to recreate file format
        String[] dest_array = destinations.toArray(new String[0]);
        String dest_result = StringUtils.join(" ", dest_array);

        // outputs: (Node, PageRank ListOfOutgoingLinks) key value pair
        result.set(pagerank_str + " " + dest_result);
        context.write(key, result);


//        System.out.println("---------------------------------------");
//        System.out.println("old_pagerank: " + old_pagerank);
//        System.out.println("updated_pagerank: " + updated_pagerank);
//        System.out.println(key.toString() + " residual: " + residual);
//        System.out.println("---------------------------------------");
    }
}
