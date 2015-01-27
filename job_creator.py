def pair_fastq(fastq_dir):
    
    files = glob.glob(fastq_dir)
    PATTERN_FASTQ = re.compile("(.*)[\._][Rr]?[1|2](.*?).f(ast)?q(.gz)?")
    samples = {}
    fs = sorted(files)
    if len(fs)==1:
        return [fs]
    for f in fs:
        m =  Fastq.PATTERN_FASTQ.match(f)
        if m:
            samples.setdefault(m.groups()[0],[]).append(f)
        else:
            raise Exception("Unrecognizable fastq file: %s " % f)
            
    return samples.values()
    

def test(fastq_dir):
    fqs = pair_fastq(fastq_dir)
    """
    dirs = []
    for r1,r2 in Fastq().pair(files):
        sn = r1.strip(left_tag).strip(right_tag)
        r1 = r1.strip(left_tag)
        r2 = r2.strip(left_tag)
        print "/home/hadoop/bio/app/tophat/2.0.13/tophat -p 8 -o %s /home/hadoop/bio/data/index/hg19/bowtie2/ucsc.hg19 %s %s" % (sn,r1,r2)
        dirs.append(sn)
    for d in dirs:
        print "/home/hadoop/bio/app/cufflinks/2.2.1/cufflinks -p 8 -o cufflinks_%s %s/accepted_hits.bam" % (d,d)
        
    """
test()    