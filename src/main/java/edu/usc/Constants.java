package edu.usc;

/**
 * The following constants must be set here, and source compiled, as the Mappers
 * and Reducers that are run on the cluster are stateless (i.e. do not accept
 * initializers)
 */
public class Constants {

    /**
     * This is the width of the view in base pairs that is used for by a Depth
     * Read Mapper to examine the aligned reads. Default is 100
     */
    public static final int read_bin_width = 100;
    /**
     * The min quality score for a read to be considered mapped. 0 is worst, 1
     * is best
     */
    public static final double mapping_quality_threshold = .99;
    /**
     * The min quality score for a base to be considered mapped. 0 is worst, 1
     * is best
     */
    public static final double base_quality_threshold = .99;

    /**
     * The number of base pairs to be considered as a single data point in the
     * HMM. A wider window will provide smoother values but less resolution.
     * Default is 10000.
     */
    public static final int bin_width = 10000;

    /**
     * The minimum proportion of sites in a bin window that are heterozygous, as
     * defined by 1) being in the VCF file and 2) having a B allele depth
     * greater than zero. A measured heterozygosity level that is lower than
     * this threshold will favor the HMM states that correspond to a deletion
     * (either copy number = 1 or copy neutral LOH).
     */
    public static final float min_heterozygosity = .1f;

    /**
     * This penalty corresponds to the L1 penalty for a copy number not equal to
     * 2. A higher value enforces a parsimonious model of normal copy number. A
     * value of zero means no penalty, and a negative penalty instructs the
     * program to estimates this penalty as the SD of the overall signal.
     */
    public static final float abberration_penalty = -1f;
    /**
     * This penalty corresponds to the L1 penalty for transitions between
     * states. A higher value enforces a model with longer blocks of
     * aberrations. A value of zero means no penalty, and a negative penalty
     * instructs the program to estimates this penalty as the 2*SD of the
     * overall signal.
     */
    public static final float transition_penalty = -1f;
    /**
     * Alpha is the BAF mixture proportion in emission model. 0 means place all
     * weight on the contribution from the overall signal and 1 means place all
     * weight on the contribution from the BAF signal.
     */
    public static final float alpha = 0.5f;

}
