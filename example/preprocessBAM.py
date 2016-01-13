from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
import sys,os
import re
def toChr(chr):
    if re.match(r'^chr\w+$', chr):
        return chr
    else:
        return "chr"+chr

if __name__ == "__main__":
    if len(sys.argv)!=3:
        sys.stderr.write("Usage: python preprocessBAM <prefix> <fasta_dir>\n")
        exit(1)
    samAppend = []
    chr_hash = {}
    fa_dir = sys.argv[2]
    fa_files = os.listdir(fa_dir)
    # The variables for the alignment
    name = ''
    start = 1
    length = 1
    num = 1
    # The current position in FASTA file
    pos = 0
    seenThis = False
    for file in fa_files:
        chr = ''
        if re.search(r'\.fa(sta)?$',file):
            with open(os.path.join(fa_dir, file), 'r') as fh:
                for line in fh:
                    line = re.sub(r'[\r\n]+', '', line)
                    m = re.match(r'^>(\w+)$',line)
                    if m != None:
                        if name != '':
                            record = "\t".join([name, '0', chr, str(start), '0', "%dM" % length, '*', '0', '0', '*','*'])
                            samAppend.append(record)
                        name = ''
                        start = 1
                        length = 1
                        num = 1
                        pos = 0
                        chr = m.group(1)
                        if chr in chr_hash:
                            seenThis = True
                        else:
                            sys.stderr.write("NOTICE: Processing chr %s\n" % chr)
                            seenThis = False
                        chr_hash[chr] = True
                    else:
                        for base in line:
                            if seenThis: 
                                continue
                            if re.match(r'^[ATCGN]$', base, re.I) == None:
                                sys.stderr.write("ERROR: Illegal base pair in your fasta file: %s!\n" % base)
                                exit(1)
                            pos += 1
                            if base == 'N' or base == 'n':
                                if name == '':
                                    continue
                                else:
                                    record = "\t".join([name, '0', chr, str(start), '0', "%dM" % length, '*', '0', '0', '*','*'])
                                    sys.stderr.write("RECORD: %s\n" % record)
                                    name = ''
                                    num += 1
                                    samAppend.append(record)
                            else:
                                if name == '':
                                    name = "%s_artificial_%d" % (chr, num)
                                    start = pos
                                    length = 1
                                else:
                                    length += 1
                        if chr == '':
                            sys.stderr.write("ERROR: Illegal format in your fasta file! %s\n" % file)
                            exit(1)
            if chr != '' and name!='' and not seenThis:
                record = "\t".join([name, '0', chr, str(start), '0', "%dM" % length, '*', '0', '0', '*','*'])
                samAppend.append(record)
    prefix = sys.argv[1]
    with open(prefix+".extra.sam", 'w') as fh:
        for line in samAppend:
            fh.write(line + "\n") 
    #os.system("rm temp.sam temp1.sam")
                            
                            
                            
                            
                            