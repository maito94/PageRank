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
    public static void iterateBlockOnce(Double[] PR, Integer[] deg, Integer blockID,
                                        ArrayList< NTuple.Pair<Integer, Integer> > BE,
                                        ArrayList< NTuple.Triple<Integer, Integer, Double> > BC)
    {
        Double[] NPR = new Double[ PR.length ];
        Arrays.fill(NPR, 0.0);

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
            PR[i] = PageRank.DAMPING_FACTOR*NPR[i] + (1-PageRank.DAMPING_FACTOR)/PageRank.EXPECTED_NODES;

//            System.out.println("-----------------------");
//            System.out.println("PR[" + i + "]: " + PR[i]);
//            System.out.println("-----------------------");

        }
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
        System.out.println("-----------------------");
        System.out.println("REDUCING");
        System.out.println("-----------------------");

        Integer blockID = Integer.valueOf(key.toString());
        Integer block_size = PageRank.BLOCK_SIZES.get( blockID );
        Double[] pageranksInBlock = new Double[block_size]; // PR
        Integer[] degreesOfNode = new Integer[block_size];

        String[] outgoingLinks = new String[block_size];

        ArrayList< NTuple.Pair<Integer, Integer> > edgesInBlock = new ArrayList<>(); // BE
        ArrayList< NTuple.Triple<Integer, Integer, Double> > boundaryConditinsInBlock = new ArrayList<>(); // BC


        for (Text info : values) {
            String info_str = info.toString();
            String[] info_arr = info_str.split("\\s+");


            System.out.println("INFO_STR: " + info_str);


            // construct PR array
            if (info_arr[1].contains("PageRank")) {
                // info_str = "Key=A PageRank=0.20"
                // info_arr = ["Key=A", "PageRank=0.20"]
                int index_u = info_arr[0].indexOf('=');
                Integer u = Integer.valueOf( info_arr[0].substring(index_u + 1) );

                int index_pagerank = info_arr[1].indexOf('=');
                Double pagerank = Double.valueOf( info_arr[1].substring(index_pagerank + 1) );

                // normalize u to index within own block
                pageranksInBlock[ PageRank.getNodeBlockIndex(u, blockID) ] = pagerank;
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
                    Integer v = Integer.valueOf( d );

                    if (PageRank.getBlockID( v ) == blockID) {
                        NTuple.Pair<Integer, Integer> u_v = new NTuple().new Pair<>(u, v);
                        edgesInBlock.add(u_v);
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

        iterateBlockOnce(pageranksInBlock, degreesOfNode, blockID, edgesInBlock, boundaryConditinsInBlock);


        for (int i = 0; i < block_size; i++) {
            String str_result = "";
            Integer nodeID = PageRank.getNodeIDFromIndex(i, blockID);
            str_result += pageranksInBlock[i] + " " + outgoingLinks[i];

            result.set(str_result);
            node.set( String.valueOf(nodeID) );

            context.write(node, result);
        }

    }
}
