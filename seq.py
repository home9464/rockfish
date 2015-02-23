import os,re,subprocess

"""

EXPERIMENT=150218_M00478_0285_000000000-ADB0U
cp -r ~/miseq/MiSeqAnalysis/${EXPERIMENT} .
find ${EXPERIMENT} -name "*.bcl" -exec gzip {} \;
bcl2fastq -R ${EXPERIMENT} -o ${EXPERIMENT}-fastq 2>/dev/null && rm -fr ${EXPERIMENT}
 
"""

IP_MISEQ = '10.2.2.235'
PATH_MISEQ='miseq/MiSeqAnalysis'
#PATH_LOCAL='/home/hadoop/miseq/MiSeqAnalysis'
#PATH_LOCAL_TMP='/home/hadoop/miseq/tmp'
PATH_LOCAL='/home/hadoop/miseq/experiments'
PATH_LOCAL_TMP='/home/hadoop/miseq/tmp'
bcl2fastq = '/home/hadoop/tool/bcl2fastq/bin/bcl2fastq'
PATH_HDFS='/miseq'

def shell_exec(cmd,shell=True):
    """cmd is a string!
    """
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=shell)
    std_out,std_err = p.communicate()
    if std_out:
        return std_out.strip().split('\n')
    else:
        return []

#deprecated
def isComplete(exp_dir):
    """tell if a sequencing run is complete
    """
    COMPLETED_FILE='RunCompletionStatus.xml'
    COMPLETED_TAG = 'SuccessfullyCompleted'
    status = os.path.join(exp_dir,COMPLETED_FILE)
    if os.path.exists(status):
        if re.compile(COMPLETED_TAG).findall('\n'.join(file(status).readlines())):
            return True
    return False

def listCompletedExperiments():
    """list completed experiments in MiSeq
    """
    rsync_error = re.compile('rsync error')
    COMPLETED_FILE='RunCompletionStatus.xml'
    COMPLETED_TAG = re.compile('SuccessfullyCompleted')
    all_experiments = []
    completed_experiments = []
    cmd = "rsync -n %s::%s/ | awk '($1 ~ /^d/) {print $5}'" % (IP_MISEQ,PATH_MISEQ)
    ret = shell_exec(cmd)
    if ret:
        all_experiments =  [r for r in ret if not (r in ('.','Temp'))]
        for e in all_experiments:
            #try to get the 'RunCompletionStatus.xml' from experiment
            cmd = "rsync %s::%s/%s/%s %s/" % (IP_MISEQ,PATH_MISEQ,e,COMPLETED_FILE,PATH_LOCAL_TMP)
            shell_exec(cmd)
            #try to find the keyword 'SuccessfullyCompleted' from 'RunCompletionStatus.xml'
            status = os.path.join(PATH_LOCAL_TMP,COMPLETED_FILE)
            if os.path.exists(status):
                if COMPLETED_TAG.findall('\n'.join(file(status).readlines())):
                    completed_experiments.append(e)
                    shell_exec('rm -fr %s' % os.path.join(PATH_LOCAL_TMP,COMPLETED_FILE))
                        
    return sorted(completed_experiments,reverse=True)
    
def listConvertedExperiments():
    """list converted experiments in HDFS (so we do not need to convert it again)
    """
    converted = []
    cmd = "hdfs dfs -ls -d %s/* | awk '{print $8}'" % PATH_HDFS
    ret = shell_exec(cmd)
    if ret:
        converted =  [r.replace(PATH_HDFS+'/','') for r in ret]
    return converted

def convert2fastq():
    """
    1. synchronize MiSeq sequencer for new experiments
    2. converted .bcl in experiments into .fastq.gz
    3. transfer .fastq.gz to HDFS
    """
    print "Discover"
    not_converted = listCompletedExperiments()
    already_converted = listConvertedExperiments()
    tobe_converted = [x for x in not_converted if not (x in already_converted)]
    
    print "Synchronize"
    for t in tobe_converted:
        cmd_sync = "rsync -aru %s::%s/%s %s/" % (IP_MISEQ,PATH_MISEQ,t,PATH_LOCAL)
        shell_exec(cmd_sync)
        
    print "Convert"
    for d in tobe_converted:
        src = os.path.join(PATH_LOCAL,d)    
        print d
        dest = os.path.join(PATH_LOCAL_TMP,d)
        cmd = []
        cmd.append('find %s -name "*.bcl" -exec gzip {} \;' % src)
        cmd.append('%s -R %s -o %s 2>/dev/null' % (bcl2fastq,src,dest))
        cmd.append('cp %s/*.xml %s/SampleSheet.csv %s/' % (dest,src,dest))
        cmd.append('hdfs dfs -put %s %s/' % (dest,PATH_HDFS))
        cmd.append('rm -fr %s %s' % (src,dest))
        #print ' && '.join(cmd)
        #break
        shell_exec(' && '.join(cmd))
        
convert2fastq()
#print listCompletedExperiments()
