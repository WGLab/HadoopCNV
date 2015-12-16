use strict;
use warnings;
use Getopt::Long; 
use Pod::Usage;
our($type);
my @dels = ();
my @dups = (); 
while(<>){
    s/[\r\n]+//g;
    my @words = split("\t");
    my ($chr, $start, $end, $rd, $hmm, $copy_num) = @words[0..5];
    next if($start>=$end);
    
    next if($chr!~/^(chr)?[\dXY]+$/ or $start!~/^\d+$/ or $end!~/^\d+$/);
    my $type;
    if ($rd eq "NaN"){
    	next;
    }
    if($copy_num >2) { 
    	$type = "duplication"; 
    	push @dups, [$chr, $start, $end];
    }
    elsif($copy_num <2) { 
    	$type = "deletion"; 
    	push @dels, [$chr, $start, $end];
    }
    else { next; }
	
}
my $THRESHOLD = 0.2;
my $LEN_THRE = 500;
sub merge {
	@_==1 or die "Error: Please send the ref of the CNV array into merge()";
	my @cnvs = @{$_[0]};
	my @new_cnvs = ();
	while(1){
		@new_cnvs=();
		my $merged = 0;
		for my $cnv (@cnvs){
			if (not @new_cnvs){
				push @new_cnvs, $cnv;
				next;
			}
			my ($chr, $start, $end) = @$cnv;
			my ($p_chr, $p_start, $p_end) =  @{$new_cnvs[-1]};
			if ($chr ne $p_chr){
				push @new_cnvs, $cnv;
				next;
			}
			else{
				my $interval = $start-$p_end;
				my $total = $end - $start +1 + $p_end - $p_start + 1;
				if ($interval/($total+0.0) <= $THRESHOLD ){
					$merged++;
					pop @new_cnvs;
					push @new_cnvs,[$chr,$p_start,$end];
				}
				else{
					push @new_cnvs,[$chr,$start,$end];
				}
			}
		}
		@cnvs = @new_cnvs;
		if ($merged == 0) {
			last;
		}
		
	}
	return @new_cnvs;
}
		
@dels = merge(\@dels);
@dups = merge(\@dups);
for my $cnv (@dels){
	my ($chr, $start, $end) = @$cnv;
	my $len = $end-$start+1;
	next if($len < $LEN_THRE);
	print join("\t", ("deletion", "$chr:$start-$end") )."\n";
}
for my $cnv (@dups){
	my ($chr, $start, $end) = @$cnv;
	my $len = $end-$start+1;
	next if($len < $LEN_THRE);
	print join("\t", ("duplication", "$chr:$start-$end") )."\n";
}


		
	

