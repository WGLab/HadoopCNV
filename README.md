# HadoopCNV

## Introduction

HadoopCNV (internal codename: PennCNV3) is a Java implementation of MapReduce-based copy number variation caller for next-generation whole-genome sequencing data.

In addition to single nucleotide variants (SNVs) and small insertions or deletions (INDELs), whole-genome sequencing (WGS) data may also be used to identify large-scale alterations, such as copy number variations (CNVs) and other types of structural variants (SVs).  Existing CNV detection methods mostly rely on read depth or paired end distance or the combination thereof.  Additionally, resolving small regions in WGS samples with deep coverage can be very time consuming due to massive I/O cost. To facilitate the CNV detection from WGS data, we developed HadoopCNV, a hidden Markov model based algorithm, which infers detects aberration events such as copy number changes through information encoded in both allelic and overall read depth.  Our implementation is built on the Hadoop MapReduce paradigm, enabling parallel multiple processors at multiple nodes to efficiently process separate genomic regions in tandem. We also employ a Viterbi scoring algorithm to infer the most likely copy number/heterozygosity state for each region of the genome. 

## Setup

To setup the environment for HadoopCNV, you need to install Hadoop2.0+ in your computer or cluster first. Please refer to [Initialization](docs/initialization.md)

## Example

After initialization, to run HadoopCNV, please check our example first:
[Example](doc/example.md)

## Workflow

![HadoopCNV Workflow](docs/images/PennCNV3.png "HadoopCNV Workflow")

## Reference

Yang H, Chen G, Lima L, Fang H, Jimenez L, Li M, Lyon GJ, He M, Wang K. HadoopCNV : A Scalable Solution for Accurate Detection of Copy Number Variations from Whole-Genome Sequencing Data.

## License

[MIT License](http://wglab.mit-license.org)
