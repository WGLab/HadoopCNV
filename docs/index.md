# HadoopCNV 

BACKGROUND: Whole-genome sequencing (WGS) data may be used to identify large-scale alterations, such as copy number variations (CNVs). Existing CNV detection methods mostly rely on read depth or paired end distance or their combination, while neglecting allelic intensity ratios, which are known to be useful for CNV detection from SNP arrays. Additionally, most CNV callers are not scalable to handle a large number of samples.

METHODS: To facilitate large-scale and rapid CNV detection from WGS data, we developed a Dynamic Programming Imputation based algorithm called HadoopCNV, which infers copy number changes through information encoded in both allelic frequency and overall read depth. Our implementation is built on the Hadoop Distributed File System (HDFS) and the MapReduce paradigm, enabling multiple processors at multiple compute nodes to work in parallel. 

RESULTS: Compared to two widely used tools CNVnator and LUMPY, our method has similar or better performance on both simulated data sets and the NA12878 individual from the 1000 Genomes Project. Additionally, analysis on a 10 member pedigree sequenced by Illumina HiSeq showed that HadoopCNV has a Mendelian precision that is slightly better than CNVnator. Furthermore, we demonstrated the ability for HadoopCNV to accurately infer copy neutral loss of heterozygosity (LOH), while other tools cannot. More importantly, HadoopCNV on a 32-node cluster requires only 1.6 hours for a human genome with 30X coverage, making rapid analysis on thousands of genomes feasible.

CONCLUSIONS: The combination of high-resolution, allele-specific read depth from WGS data and Hadoop framework can result in efficient and accurate detection of CNVs. Our study also highlights that big data framework such as Hadoop can significantly facilitate genome analysis in the future. 


Please click the menu items to navigate through this website. Check [here](misc/whatsnew.md) to see what is new in SVGen.

---

![new](img/new.png) 2016Jan21: The first stable version of HadoopCNV is released.

---

## Reference

- Yang H, Chen G, Lima L, Fang H, Jimenez L, Li M, Lyon GJ, He M, Wang K. HadoopCNV: A dynamic programming imputation algorithm to detect copy number variants from sequencing data.




