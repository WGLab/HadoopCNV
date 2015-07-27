package edu.usc;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.List;
import org.apache.hadoop.io.Text;

/**
 * This is the class that runs a Viterbi scoring CNV calling algorithm on
 * intensity signals over a Chromosome length region. It is intended to be
 * called by the Hadoop framework, but can be easily unit tested by calling the
 * main function.
 */
public class Hmm {

    private String refName;
    private final int states = 4;
    private int markers;
    private int[] ranges;
    private float[] data;
    private float[] lrr_mat;
    private float[] mse_mat;
    private float[] baf_mat;
    private float[] loss_mat;
    private float[] scaled_depth;
    private float mean_coverage;
    private final float[] mu = {-.5f, 0, 0, .5f};
    private final int[] cn_arr = {1, 2, 2, 3};
    private int[] best_state;
    private float alpha = Constants.alpha;
    private float lambda1 = Constants.abberration_penalty;
    private float lambda2 = Constants.transition_penalty;

//    private static final int STATE_SINGLE_DEL = 0;
//    private static final int STATE_CN_LOH = 1;
//    private static final int STATE_NORMAL = 2;
//    private static final int STATE_SINGLE_DUP = 3;
    public Hmm(int markers) {
        this.markers = markers;
    }

    public Hmm() {
    }

    /**
     * Set the mixing proportion alpha.
     *
     * @param alpha
     * @see Constants
     */
    public void setAlpha(float alpha) {
        this.alpha = alpha;
    }

    /**
     * Set the copy number 2 penalty
     *
     * @param lambda1
     * @see Constants
     */
    public void setLambda1(float lambda1) {
        this.lambda1 = lambda1;
    }

    /**
     * Set the transition penalty
     *
     * @param lambda2
     * @see Constants
     */
    public void setLambda2(float lambda2) {
        this.lambda2 = lambda2;
    }

    private void init_matrices() {
        lrr_mat = new float[markers * states];
        baf_mat = new float[markers * states];
        loss_mat = new float[markers * states];
        best_state = new int[markers];
        scaled_depth = new float[markers];
        scale_depth();
        float sd = get_sd();
        System.err.println("Standard deviation is " + sd);
        if (this.lambda1 < 0) {
            this.lambda1 = sd;
        }
        if (this.lambda2 < 0) {
            this.lambda2 = 2 * sd * (float) Math.sqrt(Math.log(markers));
        }
        System.err.println("Chr " + refName + " lambda 1 and 2 are " + lambda1 + " and " + lambda2);
    }

    private float get_sd() {
        if (markers < 2) {
            return 0;
        }
        float mean = 0;
        for (int marker = 0; marker < markers; ++marker) {
            mean += scaled_depth[marker];
        }
        mean /= markers;
        float sd = 0;
        for (int marker = 0; marker < markers; ++marker) {
            float dev = (scaled_depth[marker] - mean);
            sd += dev * dev;
        }
        sd = (float) Math.sqrt(sd / (markers - 1));
        return sd;
    }

    private void scale_depth() {
        for (int i = 0; i < markers; ++i) {
            scaled_depth[i] = (data[i * 2] - mean_coverage) / mean_coverage;
            if (scaled_depth[i] < -.5) {
                scaled_depth[i] = -.5f;
            }
            if (scaled_depth[i] > .5) {
                scaled_depth[i] = .5f;
            }
        }

    }

//    private void rescale(float mat[], int row) {
//        float sum = 0f;
//        for (int state = 0; state < states; ++state) {
//            sum += mat[row * states + state];
//        }
//        for (int state = 0; state < states; ++state) {
//            mat[row * states + state] /= sum;
//        }
//    }
//
    private void compute_emission() {
        if (markers < 2) {
            return;
        }
        boolean debug = false;
        for (int i = 0; i < markers; ++i) {
            if (debug) {
                System.err.print("\n" + i + "\tLRR:" + scaled_depth[i] + "\tBAF:" + data[i * 2 + 1]);
            }
            for (int state = 0; state < states; ++state) {
                float dev = scaled_depth[i] - mu[state];
                lrr_mat[i * states + state] = dev * dev;
                baf_mat[i * states + state] = mse_mat[i * states + state];

                if (debug) {
                    System.err.print("\t" + lrr_mat[i * states + state] + "," + baf_mat[i * states + state]);
                }
            }

            if (debug) {
                System.err.println();
            }
        }
    }

    /**
     * Execute the Viterbi scoring conditional on the observed intensities, and
     * lambda1 and lambda2 penalties.
     */
    private void do_viterbi() {
        int[] traceback = new int[markers * states];
        float min = Float.MAX_VALUE;
        for (int state = 0; state < states; ++state) {
            loss_mat[state] = (1 - alpha) * lrr_mat[state] + alpha * baf_mat[state] + lambda1 * Math.abs(mu[state]);
            if (loss_mat[state] < min) {
                min = loss_mat[state];
            }
        }
        for (int marker = 1; marker < markers; ++marker) {
            float dist = 1;
            for (int current = 0; current < states; ++current) {
                min = Float.MAX_VALUE;
                float loss; // = 0;
                for (int prev = 0; prev < states; ++prev) {
                    loss = loss_mat[(marker - 1) * states + prev] + (1 - alpha) * lrr_mat[marker * states
                            + current] + alpha * baf_mat[marker * states + current] + lambda1
                            * Math.abs(mu[current]) + lambda2 * (dist) * Math.abs(mu[current] - mu[prev]);
                    if (loss < min) {
                        min = loss;
                        loss_mat[marker * states + current] = min;
                        traceback[marker * states + current] = prev;
                    }
                }
                if (min == 1e10) {
                    System.err.println("A Failed to find minimum:");
                    for (int prev = 0; prev < states; ++prev) {
                        loss = loss_mat[(marker - 1) * states + prev] + (1 - alpha) * lrr_mat[marker * states
                                + current] + alpha * baf_mat[marker * states + current] + lambda1
                                * Math.abs(mu[current]) + lambda2 * (dist) * Math.abs(mu[current] - mu[prev]);
                        System.err.println(" " + prev + "," + loss);
                    }
                    System.err.println();
                    System.exit(1);
                }
            }
        }

        // now do the traceback
        int min_index = 1;
        min = Float.MAX_VALUE;
        for (int current = 0; current < states; ++current) {
            if (loss_mat[(markers - 1) * states + current] < min) {
                min = loss_mat[(markers - 1) * states + current];
                min_index = current;
            }
        }

        if (min == Float.MAX_VALUE) {
            System.err.println("B Failed to find minimum!");
            System.exit(1);
        }

        best_state[markers - 1] = cn_arr[min_index];
        for (int marker = markers - 1; marker > 0; --marker) {
            best_state[marker - 1] = traceback[marker * states + min_index];
        }
    }

    // returns -1 if small CNVs found, 1 if big CNVs found, 0 else
    private int good_cnv_size() {
        boolean do_search = false;
        if (do_search) {
            // these are user preferences for CNV distributions
            // int abb_count_threshold_min = 100;
            int length_threshold_min = (int) (markers * .01);
            int length_threshold_max = (int) (markers * .1);
            System.err.println("Good CNV range: " + length_threshold_min + " to " + length_threshold_max);

            int last_cn = 2;
            int abb_start = -1;
            int abb_end;    // = -1;
            int size_flag = 0;
            int total_abb_len = 0;

            for (int marker = 0; marker < markers; ++marker) {
                int cur_cn = cn_arr[best_state[marker]];
                if (last_cn == 2 && cur_cn != 2) {
                    abb_start = marker;
                    //System.err.println("Abberation start at marker "+marker);
                } else if (last_cn != 2 && cur_cn == 2) {
                    abb_end = marker - 1;
                    //System.err.println("Abberation end at marker "+abb_end);
                    int len = abb_end - abb_start + 1;
                    total_abb_len += len;
                    //if (len>length_threshold_max) ++over_max_abberation;
                    //if (len<length_threshold_min) ++under_min_abberation;

                }
                last_cn = cur_cn;
            }

            // nothing was found, too strong a penalty
            System.err.println("Abberation len " + total_abb_len);
            if (total_abb_len > length_threshold_max) {
                System.err.println("Abberation len too big, aborting");
                size_flag = -1;
            } else if (total_abb_len < length_threshold_min) {
                System.err.println("Abberation len too small, aborting");
                size_flag = 1;
            }
            return size_flag;
        } else { // skip search
            return 0;
        }
    }

    /**
     * Called by Hadoop CnvReducer to get the results after Viterbi scoring is
     * completed
     *
     * @see Reducers
     * @return an Iterator of Strings where each String is a line for a region
     * bin
     */
    public Iterator<String> getResults() {
        return new ResultIterator();
    }

    /**
     * Can be called by a unit tested to print out results after Viterbi scoring
     */
    public void print_output() {
        System.out.println("start\tend\tscaled_depth\tCN\tstate");
        Iterator<String> it_result = getResults();
        while (it_result.hasNext()) {
            System.out.println(it_result.next());
        }
    }

    private void search(float lambda2_min, float lambda2_max) {
        float mid = (lambda2_max - lambda2_min) / 2;
        this.lambda2 = lambda2_min + mid;
        System.err.println("Searching range " + lambda2_min + " to " + lambda2_max + " lambda2 is " + lambda2);
        if (mid < .01) {
            System.err.println("Too small a range, aborting");
            return;
        }
        do_viterbi();
        int size_flag = good_cnv_size();

        if (size_flag < 0) {
            search(lambda2, lambda2_max);
        } else if (size_flag > 0) {
            search(lambda2_min, lambda2);
        }
    }

    public void run() {
        compute_emission();
        search(0, lambda2 * 2);
    }

    /**
     * Entrypoint from Hadoop reduce task
     *
     * @see Reducers
     * @param refName The Chromosome name
     * @param it_text The various bin regions, and their associated intensities
     */
    public void init(String refName, Iterator<Text> it_text) {
        this.refName = refName;
        List<Integer> start = new ArrayList<>();
        List<Integer> end = new ArrayList<>();
        List<Float> depth = new ArrayList<>();
        List<List<Float>> baf = new ArrayList<>();
        int sites = 0;
        mean_coverage = 0;

        while (it_text.hasNext()) {
            String reduceValue = it_text.next().toString();
            //System.err.println("CNV REDUCER VALUE: "+reduceValue);
            String[] parts = reduceValue.split("\t");
            int part = 0;
            start.add(Integer.decode(parts[part++]));
            end.add(Integer.decode(parts[part++]));
            depth.add(Float.valueOf(parts[part++]));
            List<Float> vec = new ArrayList<>();

            for (int state = 0; state < states; ++state) {
                Float newFloatVal = Float.valueOf(parts[part++]);

                vec.add(newFloatVal);
            }
            baf.add(vec);
            mean_coverage += (Float.valueOf(parts[part++]));
            sites += (Integer.valueOf(parts[part++]));
        }

        mean_coverage /= sites;
        this.markers = start.size();
        mse_mat = new float[markers * states];
        //System.err.println("TOTAL MARKERS: "+markers);
        System.err.println("Chr " + refName + " mean coverage of " + markers + " markers is " + mean_coverage);
        ranges = new int[markers * 2];
        data = new float[markers * 2];
        for (int i = 0; i < markers; ++i) {
            ranges[i * 2] = start.get(i);
            ranges[i * 2 + 1] = end.get(i);
            data[i * 2] = depth.get(i);

            for (int state = 0; state < states; ++state) {
                List<Float> fVal = baf.get(i);
                mse_mat[i * states + state] = fVal.get(state);
            }
        }
        init_matrices();
    }

    /**
     * This is intended for calling by the main function
     *
     * @param refName The Chromosome name
     * @param ranges The start and end points of each bin
     * @param data The intensity values
     * @param mean_coverage The average intensity for the Chromosome
     */
    public void init(String refName, int[] ranges, float[] data, float mean_coverage) {
        this.mean_coverage = mean_coverage;
        this.refName = refName;
        this.ranges = ranges;
        this.data = data;
        init_matrices();
    }

    public static void main(String args[]) {
        try {
            //for(int i=0;i<args.length;++i) System.err.println(args[i]);
            //System.exit(1);
            String filename = args[0];
            List<String> lines = Files.readAllLines(Paths.get(filename), Charset.defaultCharset());
            int markers = lines.size();
            System.err.println("There are " + lines.size() + " lines in " + filename);
            Hmm hmm = new Hmm(markers);
            int[] ranges = new int[markers * 2];
            float[] data = new float[markers * 2];
            Iterator<String> lines_it = lines.iterator();
            int marker = 0;

            String refName = "";
            float mean_coverage = 0;
            int sites = 0;
            while (lines_it.hasNext()) {
                String[] parts = lines_it.next().split("\t");
                int partindex = 0;
                refName = parts[partindex++];
                ++partindex;  // bin
                ranges[marker * 2] = Integer.parseInt(parts[partindex++]);
                ranges[marker * 2 + 1] = Integer.parseInt(parts[partindex++]);
                data[marker * 2] = Float.parseFloat(parts[partindex++]);
                data[marker * 2 + 1] = Float.parseFloat(parts[partindex++]);
                //int bac = Integer.parseInt(parts[partindex++]);
                float totalDepth = Float.parseFloat(parts[partindex++]);
                mean_coverage += totalDepth;
                int binMarkers = Integer.parseInt(parts[partindex++]);
                sites += binMarkers;
                //data[marker*2+1] = (float)bac/binMarkers;
                //System.err.println("marker "+ranges[marker*2]+" with depth "+data[marker*2]+" and baf "+data[marker*2+1]);
                ++marker;
            }
            mean_coverage /= sites;
            System.err.println("Chr " + refName + " mean coverage of " + markers + " markers is " + mean_coverage);
            hmm.init(refName, ranges, data, mean_coverage);
            hmm.run();
            hmm.print_output();
        } catch (IOException | NumberFormatException ex) {
            System.err.println("Error in main, caused by " + ex.toString());
            System.exit(1);
        }

    }

    /**
     * This inner class handles returning of the results to the Hadoop framework
     */
    class ResultIterator implements Iterator<String> {

        private int marker = 0;

        public ResultIterator() {
        }

        @Override
        public boolean hasNext() {
            return marker <= markers - 1;
        }

        /**
         * Generate the next string of results for a particular bin
         */
        @Override
        public String next() {
            if (marker >= markers) {
                throw new NoSuchElementException();
            }
            String res = Integer.toString(ranges[marker * 2])
                    + "\t" + Integer.toString(ranges[marker * 2 + 1])
                    + "\t" + Float.toString(scaled_depth[marker])
                    //+"\t"+Float.toString(data[marker*2+1]) // the median BAF
                    + "\t" + Integer.toString(best_state[marker])
                    + "\t" + Integer.toString(cn_arr[best_state[marker]])
                    + "\t" + Float.toString(baf_mat[marker * states + 0])
                    + "\t" + Float.toString(baf_mat[marker * states + 1])
                    + "\t" + Float.toString(baf_mat[marker * states + 2])
                    + "\t" + Float.toString(baf_mat[marker * states + 3]);
            ++marker;
            return res;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
