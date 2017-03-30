use strict;
use warnings;
use Getopt::Long; 
use Pod::Usage;
use File::Basename;
BEGIN{
	push @INC,"/home/huiyang/project/CNV_Analysis";
}
use CNVutilities qw(:COMPLETE);
@ARGV == 2 or pod2usage("Pleae do <lumpy_cnv> <hadoop_cnv>\n");
my $file1 = $ARGV[0];                  #This is the query,
my $file2 = $ARGV[1];                  #This is the reference
my (@compares, @query, @del_query, @del_compare, @dup_query, @dup_compare);
   open(QUERY,"$file1") or die;
   open(COMPARE_FILE, "$file2") or die;
   @compares = <COMPARE_FILE>;
   @query =<QUERY>;
   @del_query = extract_cnv("deletion",\@query);
   @del_compare = extract_cnv("deletion",\@compares); 
   @dup_query = extract_cnv("duplication",\@query);	   
   @dup_compare = extract_cnv("duplication",\@compares); 	
   @del_query = cnv_sort(\@del_query) if(@del_query);
   @dup_query = cnv_sort(\@dup_query) if(@dup_query);
   @del_query = cnv_merge(\@del_query) if(@del_query);
   @dup_query = cnv_merge(\@dup_query) if(@dup_query);
  
my ($cnv_num, $cnv_bp, $cnv_query, $cnv_compare, $concordance_cnv, 
    $cnv_num_rec, $repeat_check_rec, $repeat_check, 
    $repeat_check_ref, $repeat_check_ref_rec,
    $repeat_check_pair_rec);
    
my ($con_length, $sen_length, $spe_length,
     $con_touch,  $sen_touch,  $spe_touch,
     $con_rec,    $sen_rec,    $spe_rec,
     $overlap_length, $query_length, $compare_length,
     $overlap_num_touch, $overlap_num_rec, $query_num, $compare_num);

sub merge_cnv {
     my ($query, $compare, $type) = @_;
     ($cnv_num, $cnv_bp, $cnv_query, $cnv_compare, $concordance_cnv, 
      $cnv_num_rec, $repeat_check_rec, $repeat_check, $repeat_check_pair_rec,
      $repeat_check_ref_rec, $repeat_check_ref)
     = cnv_compare($query, $compare, 0.5);

	my %lumpy_rec = %$repeat_check_rec;
	my %hadoopCNV_rec;
	my %pair_rec = %$repeat_check_pair_rec;
	for my $each (keys %pair_rec) {
		$hadoopCNV_rec{$pair_rec{$each}} = 1;
	}
	my @lumpy = @$query;
	my @hadoop = @$compare;
	my @result = ();
	for my $cnv (@lumpy){
		if (not exists $lumpy_rec{$cnv}){
			push @result, $cnv;
		} 
	}
	for my $cnv (@hadoop){
		if (not exists $hadoopCNV_rec{$cnv}){
			push @result, $cnv;
		} 
	}
	for my $cnv (keys %lumpy_rec) {
		push @result, $cnv;
	}
	for my $line (@result) {
		my ($chr, $start, $end) = split("\t", $line);
		print "$type\t$chr:$start-$end\n";
	}
}

merge_cnv(\@del_query, \@del_compare, "deletion");
merge_cnv(\@dup_query, \@dup_compare, "duplication");








   
   
   