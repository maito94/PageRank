package rank.page;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class BlockUpdatePageRankReducer
        extends Reducer<Text, Text, Text, Text>
{

    private Text node = new Text();
    private Text result = new Text();


    /**
     *
     *  for( v ∈ B ) { NPR[v] = 0; }
     *  for( v ∈ B ) {
     *      for( u where <u, v> ∈ BE ) {
     *          NPR[v] += PR[u] / deg(u);
     *      }
     *      for( u, R where <u,v,R> ∈ BC ) {
     *          NPR[v] += R;
     *      }
     *      NPR[v] = d*NPR[v] + (1-d)/N;
     *  }
     *  for( v ∈ B ) { PR[v] = NPR[v]; }
     */
    public static NTuple.Pair<Double[], Double[]> iterateBlockOnce(Double[] PR, Integer[] deg, Integer blockID,
                                                                   ArrayList< NTuple.Pair<Integer, Integer> > BE,
                                                                   ArrayList< NTuple.Triple<Integer, Integer, Double> > BC,
                                                                   Context context)
    {

        Double[] NPR = new Double[ PR.length ];
        Arrays.fill(NPR, 0.0);

        Double[] residuals = new Double[ PR.length ];
        Arrays.fill(residuals, 0.0);

        for (NTuple.Pair<Integer, Integer> u_v : BE) {
            Integer u = u_v.getFirst();
            Integer v = u_v.getSecond();

            // normalize u and v to indices within own block
            Integer u_index = PageRank.getNodeBlockIndex(u, blockID);
            Integer v_index = PageRank.getNodeBlockIndex(v, blockID);

            NPR[v_index] += PR[u_index] / deg[u_index];
        }

        for (NTuple.Triple<Integer, Integer, Double> u_v_R : BC) {
            Integer u = u_v_R.getFirst();
            Integer v = u_v_R.getSecond();
            Double R = u_v_R.getThird();

            // normalize v to index within own block
            Integer v_index = PageRank.getNodeBlockIndex(v, blockID);
            NPR[v_index] += R;
        }

        int pr_length = PR.length;
        for (int i = 0; i < pr_length; i++) {
            NPR[i] = PageRank.DAMPING_FACTOR*NPR[i] + (1-PageRank.DAMPING_FACTOR)/context.getConfiguration().getInt("EXPECTED_NODES", 1);

            // calculate residual for current node
            residuals[i] = Math.abs(PR[i] - NPR[i]) / NPR[i];
        }

        NTuple.Pair<Double[], Double[]> npr_residuals = new NTuple().new Pair<>(NPR, residuals);
        return npr_residuals;
    }

    /**
     *
     * Input format:
     *      Key     Values (list of strings)
     *
     *      probably want "Key=A Destinations=B,C" and in for loop only add to BE if destination is in Block, this will
     *      allow us to recreate format
     *
     *                                        B, C ∈ Block, E ∉ Block                       E ∈ Block'
     *      Block   [ "Key=A PageRank=0.20", "Key=A Destinations=B,C,E", "Key=A Degree=2", "Key=A Source=E SourceBlock=Block' PageRank=0.20 Degree=1" ]
     *
     *                                        D ∈ Block
     *      Block   [ "Key=B PageRank=0.20", "Key=B Destinations=D", "Key=B Degree=1" ]
     *
     *                                        A, B ∈ Block
     *      Block   [ "Key=C PageRank=0.20", "Key=C Destinations=A,B", "Key=C Degree=2" ]
     *
     *                                        B, C ∈ Block
     *      Block   [ "Key=D PageRank=0.20", "Key=D Destinations=B,C", "Key=D Degree=2" ]
     *
     *
     *  With the given input values, reconstruct the same structure from the original file with updated page rank values
     *  Output format:
     *      Source  PageRank    Destinations
     *      A       0.125       B C
     *      B       0.375       D
     *      C       0.25        A B
     *      D       0.25        B C
     *
     *
     *
     *  PR[v] = current PageRank value of Node v for v ∈ B
     *  BE = { <u, v> | u ∈ B ∧ u → v } = the Edges from Nodes in Block B
     *  BC = { <u, v, R> | u ∉ B ∧ v ∈ B ∧ u → v ∧ R = PR(u)/deg(u) } = the Boundary Conditions
     *
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        Integer blockID = Integer.valueOf(key.toString());
        Integer block_size = PageRank.BLOCK_SIZES.get( blockID );

        Double[] oldPageRanksInBlock = new Double[block_size]; // PR
        Integer[] degreesOfNode = new Integer[block_size];

        String[] outgoingLinks = new String[block_size];

        ArrayList< NTuple.Pair<Integer, Integer> > edgesInBlock = new ArrayList<>(); // BE
        ArrayList< NTuple.Triple<Integer, Integer, Double> > boundaryConditinsInBlock = new ArrayList<>(); // BC

        for (Text info : values) {
            String info_str = info.toString();
            String[] info_arr = info_str.split("\\s+");

            // construct PR array
            if (info_arr[1].contains("PageRank")) {
                // info_str = "Key=A PageRank=0.20"
                // info_arr = ["Key=A", "PageRank=0.20"]
                int index_u = info_arr[0].indexOf('=');
                Integer u = Integer.valueOf( info_arr[0].substring(index_u + 1) );

                int index_pagerank = info_arr[1].indexOf('=');
                Double pagerank = Double.valueOf( info_arr[1].substring(index_pagerank + 1) );

                // normalize u to index within own block
                oldPageRanksInBlock[ PageRank.getNodeBlockIndex(u, blockID) ] = pagerank;
            }
            // construct BE array
            else if (info_arr[1].contains("Destinations")) {
                // info_str = "Key=A Destinations=B,C,E"
                // info_arr = ["Key=A", "Destinations=B,C,E"]
                int index_u = info_arr[0].indexOf('=');
                Integer u = Integer.valueOf( info_arr[0].substring(index_u + 1) );

                int index_destinations = info_arr[1].indexOf('=');
                String[] destinations = info_arr[1].substring(index_destinations + 1).split(",");

                for (String d : destinations) {
                    if (!d.isEmpty()) {
                        Integer v = Integer.valueOf(d);

                        if (PageRank.getBlockID(v) == blockID) {
                            NTuple.Pair<Integer, Integer> u_v = new NTuple().new Pair<>(u, v);
                            edgesInBlock.add(u_v);
                        }
                    }
                }

                outgoingLinks[ PageRank.getNodeBlockIndex(u, blockID) ] = StringUtils.join(" ", destinations);
            }
            // construct Degree array
            else if (info_arr[1].contains("Degree")) {
                // info_str = "Key=A Degree=3"
                // info_arr = ["Key=A", "Degree=3"]
                int index_u = info_arr[0].indexOf('=');
                Integer u = Integer.valueOf( info_arr[0].substring(index_u + 1) );

                int index_degree = info_arr[1].indexOf('=');
                Integer degree = Integer.valueOf( info_arr[1].substring(index_degree + 1) );

                // normalize u to index within own block
                degreesOfNode[ PageRank.getNodeBlockIndex(u, blockID) ] = degree;
            }
            // construct BC array
            else {
                // info_str = "Key=A Source=E SourceBlock=Block' PageRank=0.20 Degree=1"
                // info_arr = ["Key=A", "Source=E", "SourceBlock=Block'", "PageRank=0.20", "Degree=1"]
                int index_v = info_arr[0].indexOf('=');
                Integer v = Integer.valueOf( info_arr[0].substring(index_v + 1) );

                int index_u = info_arr[1].indexOf('=');
                Integer u = Integer.valueOf( info_arr[1].substring(index_u + 1) );

                int index_pagerank = info_arr[3].indexOf('=');
                Double pagerank = Double.valueOf( info_arr[3].substring(index_pagerank + 1) );

                int index_degree = info_arr[4].indexOf('=');
                Integer degree = Integer.valueOf( info_arr[4].substring(index_degree + 1) );

                Double R = pagerank / degree;

                NTuple.Triple<Integer, Integer, Double> u_v_R = new NTuple().new Triple<>(u, v, R);
                boundaryConditinsInBlock.add(u_v_R);
            }
        }

        Double[] newPageRanksInBlock;
        Double[] inblock_residuals;

        Double[] pagerank_start = new Double[block_size];
        // store page ranks before iterations begin (used for outer-block residual calculation)
        for (int i = 0; i < block_size; i++) {
            pagerank_start[i] = oldPageRanksInBlock[i];
        }


        Double average_inblock_residual = 10.0;
        while (average_inblock_residual > PageRank.EPSILON) {
//      while (iterations < max_iterations) {

            context.getCounter(PageRank.PageRankEnums.AGGREGATE_BLOCK_ITERATIONS).increment(1);

            NTuple.Pair<Double[], Double[]> pr_residuals=
                    iterateBlockOnce(oldPageRanksInBlock, degreesOfNode, blockID, edgesInBlock, boundaryConditinsInBlock, context);

            // get new page rank values and residuals
            newPageRanksInBlock = pr_residuals.getFirst();
            inblock_residuals = pr_residuals.getSecond();

            // sum up value of residuals and calculate average
            Double aggregate_inblock_residuals = 0.0;
            for (Double d : inblock_residuals) {
                aggregate_inblock_residuals += d;
            }
            average_inblock_residual = aggregate_inblock_residuals / block_size;

            // update page ranks to new values
            oldPageRanksInBlock = newPageRanksInBlock;

            if (average_inblock_residual < PageRank.EPSILON) {
                break;
            }
        }


        for (int i = 0; i < block_size; i++) {
            String str_result = "";
            Integer nodeID = PageRank.getNodeIDFromIndex(i, blockID);
            str_result += oldPageRanksInBlock[i] + " " + outgoingLinks[i];

            result.set(str_result);
            node.set( String.valueOf(nodeID) );

            context.write(node, result);

            // calculate page rank residual after iterations end (used for outer-block residual calculation)
            Double pagerank_residual = Math.abs(pagerank_start[i] - oldPageRanksInBlock[i]) / oldPageRanksInBlock[i];

            // covert double to long and keep an accuracy of at least six decimal places
            Long long_residual = (long)(pagerank_residual * PageRank.ADDED_ACCURACY);
            context.getCounter(PageRank.PageRankEnums.AGGREGATE_ITERATION_PAGERANKS_RESIDUALS).increment(long_residual);
        }
    }
}
