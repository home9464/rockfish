import os
import re
import subprocess



"""
process a job in parallel.

If a job has multiple samples, then this will split into many sub-jobs with each job consists of one sample.


#e ying.sun@fluidigm.com
export PATH=/bio/app/samtools/0.1.19/:$PATH
export PATH=/bio/app/bowtie/2.2.3:$PATH
/bio/app/fastqc/0.11.2/fastqc Undetermined_R1.fastq.gz Undetermined_R2.fastq.gz
/bio/app/tophat/2.0.13/tophat2 -p 8 -x 1000 -o Undetermined /bio/data/index/hg19/bowtie2/ucsc.hg19 Undetermined_R1.fastq.gz Undetermined_R2.fastq.gz
java -jar /bio/app/picard/1.125/picard.jar ReorderSam I=Undetermined/accepted_hits.bam O=A.tophat.bam R=/bio/data/fasta/hg19/ucsc.hg19.fasta
java -jar /bio/app/picard/1.125/picard.jar CollectAlignmentSummaryMetrics AS=true MAX_INSERT_SIZE=2000 AS=true I=A.tophat.bam O=A.CollectAlignmentSummaryMetrics
java -jar /bio/app/picard/1.125/picard.jar CollectGcBiasMetrics R=/bio/data/fasta/hg19/ucsc.hg19.fasta AS=true I=A.tophat.bam O=A.CollectGcBiasMetrics CHART=A.CollectGcBiasMetrics.pdf
java -jar /bio/app/picard/1.125/picard.jar CollectBaseDistributionByCycle R=/bio/data/fasta/hg19/ucsc.hg19.fasta AS=true I=A.tophat.bam O=A.CollectBaseDistributionByCycle CHART=A.CollectBaseDistributionByCycle.pdf
java -jar /bio/app/picard/1.125/picard.jar CollectRnaSeqMetrics REF_FLAT=/bio/data/annotation/hg19/refFlat.txt AS=true STRAND=NONE I=A.tophat.bam O=A.CollectRnaSeqMetrics CHART=CollectRnaSeqMetrics.pdf
/bio/app/cufflinks/2.2.1/cufflinks -p 4 -o cufflinks_A A.tophat.bam

"""
def split_job(job_dir):
    fs = []
    for root,dirs,files in os.walk(job_dir):
        for f in files:
            fs.append(os.path.join(root,f))
    PATTERN_FASTQ = re.compile("(.*)[\._][Rr]?[1|2](.*?).f(ast)?q(.gz)?")
    samples = {}
    fs = sorted(fs)
    if len(fs)==1:
        return [fs]
    for f in fs:
        m =  PATTERN_FASTQ.match(f)
        if m:
            common_name = os.path.basename(m.groups()[0])
            samples.setdefault(common_name,[]).append(f)
        else:
            raise Exception("Unrecognizable fastq file: %s " % f)
    
    #return samples
    cmds = []
    cmds.append('rm -f b')
    for k,v in samples.items():
        #make a new sub folder
        cmds.append('mkdir -p %s' % k)
        
        #move all inputs to the new sub folder
        cmds.append('mv %s %s/' % (' '.join(v),k))

        #copy everything else to the new sub folder 
        cmds.append('cp * %s/' % k)

        #tag the new sub folder with "b"
        cmds.append('touch %s/b' % k)
        
    shell.exec_command_remote(';'.join(cmds),job_dir)

def pair_fastq(fastq_dir):
    fs = []
    for root,dirs,files in os.walk(fastq_dir):
        for f in files:
            fs.append(os.path.join(root,f))
            
    PATTERN_FASTQ = re.compile("(.*)[\._][Rr]?[1|2](.*?).f(ast)?q(.gz)?")
    samples = {}
    fs = sorted(fs)
    if len(fs)==1:
        return [fs]
    for f in fs:
        m =  PATTERN_FASTQ.match(f)
        if m:
            common_name = os.path.basename(m.groups()[0])
            samples.setdefault(common_name,[]).append(f)
        else:
            raise Exception("Unrecognizable fastq file: %s " % f)
            
    return samples
    

def test(fastq_dir,job_dir='/project/mosprot/trimmed'):
    
    fqs = pair_fastq(fastq_dir)
    for k,v in fqs.items():
        cmd = []
        cmd.append('#e ying.sun@fluidigm.com')
        cmd.append('export PATH=/bio/app/samtools/0.1.19/:$PATH')
        cmd.append('export PATH=/bio/app/bowtie/2.2.3:$PATH')
        job_full_path = os.path.join(job_dir,k)
        if not os.path.exists(job_full_path):
            os.makedirs(job_full_path)
            
        R1=os.path.basename(v[0])
        R2=os.path.basename(v[1])
        
        cmd.append('/bio/app/tophat/2.0.13/tophat2 -p 8 -x 1000 -o %s /bio/data/index/hg19/bowtie2/ucsc.hg19 %s %s' % (k,R1,R2))
        cmd.append('java -jar /bio/app/picard/1.125/picard.jar ReorderSam I=%s/accepted_hits.bam O=%s.tophat.bam R=/bio/data/fasta/hg19/ucsc.hg19.fasta' % (k,k))
        cmd.append('java -jar /bio/app/picard/1.125/picard.jar CollectAlignmentSummaryMetrics AS=true MAX_INSERT_SIZE=2000 I=%s.tophat.bam O=%s.CollectAlignmentSummaryMetrics' % (k,k))
        cmd.append('java -jar /bio/app/picard/1.125/picard.jar CollectGcBiasMetrics R=/bio/data/fasta/hg19/ucsc.hg19.fasta AS=true I=%s.tophat.bam O=%s.CollectGcBiasMetrics CHART=%s.CollectBaseDistributionByCycle.pdf' % (k,k,k))
        cmd.append('java -jar /bio/app/picard/1.125/picard.jar CollectBaseDistributionByCycle R=/bio/data/fasta/hg19/ucsc.hg19.fasta AS=true I=%s.tophat.bam O=%s.CollectBaseDistributionByCycle CHART=%s.CollectBaseDistributionByCycle.pdf' % (k,k,k))
        cmd.append('java -jar /bio/app/picard/1.125/picard.jar CollectRnaSeqMetrics REF_FLAT=/bio/data/annotation/hg19/refFlat.txt AS=true STRAND=NONE I=%s.tophat.bam O=%s.CollectRnaSeqMetrics CHART=%s.CollectRnaSeqMetrics.pdf' % (k,k,k))
        cmd.append('/bio/app/cufflinks/2.2.1/cufflinks -p 8 -o cufflinks_%s %s.tophat.bam' % (k,k))
        fo = file(os.path.join(job_full_path,'cmd.txt'),'w')
        print >>fo,'\n'.join(cmd)
        fo.close()
        
        #copy input files
        cmd_copy = "cp %s %s/" % (v[0],job_full_path)
        subprocess.check_call(cmd_copy,shell=True)
        
        cmd_copy = "cp %s %s/" % (v[1],job_full_path)
        subprocess.check_call(cmd_copy,shell=True)
        
        cmd_begin = "touch %s/b" % job_full_path
        subprocess.check_call(cmd_begin,shell=True)
test("/project/mosprot/trimmed","/project/mosprot/test")
