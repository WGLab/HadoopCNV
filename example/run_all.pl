use strict;
use warnings;
my @LID_files = map { "/bamfiles/$_*" } ('LID57241.bam', 'LID57242.bam', 'LID57243.bam', 'LID57244.bam',
             'LID57245.bam', 'LID57246.bam', 'LID57247.bam', 'LID57248.bam',
             'LID57249.bam', 'LID57250.bam');
my @NA12878_files = map {"/bamfiles/$_*"} ('NA12878.chrom*.ILLUMINA.bwa.CEU.high_coverage.20100311.bam', 'NA12878D_HiSeqX_R1.bam', 'NA12878J_HiSeqX_R1.bam');
my @simulation = ('/bamfiles/cov20*.bam');
my $i=0;

for ( (@NA12878_files, @LID_files) ){
	my $file = quotemeta($_);
	my $name = $_;	
	$name =~ s/^.+\/(\w+).+$/$1/g; 

	if($i <= 2 ){
		system("mv config.txt temp");
		system("sed 's/VCF_FILE: .\\+/VCF_FILE: VCF\\/NA12878.vcf/' temp > config.txt");
		system("rm temp");
	}

	else{
		system("mv config.txt temp");
		system("sed 's/VCF_FILE: .\\+/VCF_FILE: VCF\\/$name.vcf/' temp > config.txt");
		system("rm temp");
	}
	my $file2 = $file;
	$file2 =~s/\*//g;
	system("mv config.txt temp");
	system("sed 's/BAM_FILE: .\\+/BAM_FILE: $file/' temp > config.txt");
	system("hdfs dfs -cp /bamfiles/cov20.extra.bam $file2.extra");
	system("rm temp");
	system("./run.sh");
	system("mv results.txt result_20160107/$name.txt");
	system('hdfs dfs -rm -r workdir/*');
	$i++;	
}
