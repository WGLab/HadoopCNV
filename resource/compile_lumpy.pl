use strict;
use warnings;
use Math::Round qw(:all);
while (<>){
    my @words = split();
    my ($chr1, $chr2, $start1, $start2, $end1, $end2,$type) 
       = @words[0,3,1,2,4,5,10];
    next if($chr1 ne $chr2);
    next if($chr1!~/^(chr)?[\dXY]+$/ or $chr2!~/^(chr)?[\dXY]+$/);
    $type =~ s/^.*?:(.*?)$/$1/;
    $type = lc $type;
    next if ($type ne "deletion" and $type ne "tandemdup" and $type ne "duplication");
    $type = "duplication" if($type eq "tandemdup" or $type eq "duplication");
    my $start = round(($start1+$start2)/2);
    my $end   = round(($end1+$end2)/2);
    if ($chr1!~/chr/) {
    	$chr1 = 'chr'.$chr1;
    }
    print "$type\t$chr1:$start-$end\n";

}