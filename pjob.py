import os
import re
import util
import subprocess

"""
process a job in parallel.

If a job has multiple samples, then this will split into many sub-jobs with each job consists of one sample.
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
    
#test("/home/hadoop/input/fastq/rnaseq/mosprot/10")
#test("/home/hadoop/input/fastq/rnaseq/mosprot/trimmed")
