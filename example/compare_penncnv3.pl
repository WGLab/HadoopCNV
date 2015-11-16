use strict;
use warnings;
use Getopt::Long; 
use Pod::Usage;
our($type);
my ($pre_chr, $pre_start, $pre_end, $pre_type) = (0, 0, 0, '0');
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
    if($copy_num >2) { $type = "duplication"; }
    elsif($copy_num <2) { $type = "deletion"; }
    else { next; }
	
	if($pre_type eq '0') { 
		$pre_chr  = $chr;
		$pre_type = $type;
		$pre_start = $start;
		$pre_end = $end; 
	}
	elsif($chr ne $pre_chr){
		$pre_chr = "chr".$pre_chr if($pre_chr =~ /^[\dXY]+$/);
		#print "$pre_type\t$pre_chr:$pre_start-$pre_end\n";
		if ($pre_type eq "deletion"){
			push @dels,[$pre_chr, $pre_start, $pre_end];
		}
		else{
			push @dups,[$pre_chr, $pre_start, $pre_end];
		}
		($pre_type, $pre_chr, $pre_start, $pre_end) = ($type, $chr, $start, $end);
	}
	elsif($type ne $pre_type){
		$pre_chr = "chr".$pre_chr if($pre_chr =~ /^[\dXY]+$/);
		#print "$pre_type\t$pre_chr:$pre_start-$pre_end\n";
		if ($pre_type eq "deletion"){
			push @dels,[$pre_chr, $pre_start, $pre_end];
		}
		else{
			push @dups,[$pre_chr, $pre_start, $pre_end];
		}
		($pre_type, $pre_chr, $pre_start, $pre_end) = ($type, $chr, $start, $end);
	}
	elsif ($start<=$pre_end){
		$pre_end = $end;
    }
    elsif ( $start-$pre_end <= 0.2*($pre_end-$pre_start+$end-$start) ){
    	$pre_end = $end;
    }
    else{
        $pre_chr = "chr".$pre_chr if($pre_chr =~ /^[\dXY]+$/);
		#print "$pre_type\t$pre_chr:$pre_start-$pre_end\n";
		if ($type eq "deletion"){
			push @dels,[$pre_chr, $pre_start, $pre_end];
		}
		else{
			push @dups,[$pre_chr, $pre_start, $pre_end];
		}
		($pre_type, $pre_chr, $pre_start, $pre_end) = ($type, $chr, $start, $end);
    }
}
$pre_chr = "chr".$pre_chr if($pre_chr =~ /^[\dXY]+$/);
#print "$pre_type\t$pre_chr:$pre_start-$pre_end\n";
if ($pre_type eq "deletion"){
			push @dels,[$pre_chr, $pre_start, $pre_end];
		}
		else{
			push @dups,[$pre_chr, $pre_start, $pre_end];
}

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
				my $total = $end - $start + $p_end - $p_start;
				if ($interval/($total+0.0) <= 0.2){
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
	next if($len < 500);
	print join("\t", ("deletion", "$chr:$start-$end") )."\n";
}
for my $cnv (@dups){
	my ($chr, $start, $end) = @$cnv;
	my $len = $end-$start+1;
	next if($len < 500);
	print join("\t", ("duplication", "$chr:$start-$end") )."\n";
}


		
	

