#!/uufs/chpc.utah.edu/common/home/HCIHiSeqPipeline/tool/python2.7.1/bin/python
import multiprocessing
import Queue
import time
import os
import sys
import imp

class TomatoMain(object):
    def __init__(self):
        self.all_jobs = {}
        self.job_index = 0

        self.util = imp.load_source('util','util.py')
        if self.util.TEST:
            print 'Test mode'
        self.job = imp.load_source('job','job.py')
        self.user_jobs = multiprocessing.Manager().dict()
        self.pending_jobs = Queue.PriorityQueue()
        
    def run(self):
        self.util.make_directories()
        self.util.clean_job_directory()
        print "[%s] Synchronizing ..." % self.util.now(),
        self.update_app_data()
        print '[OK]'
        while True:
            self.util = imp.load_source('util','util.py')
            self.job = imp.load_source('job','job.py')
            self.load()
            time.sleep(self.util.RSYNC_QUERY_INTERVAL)
            self.update_alive_jobs()
            self.update_app_data()
    
    def load(self):
        #temporarily stop receiving new jobs
        #return job_index,alive_jobs
        #job_idx = job_index
        #job_dict = alive_jobs
        
        #clean every time?
        new_jobs,abort_jobs = self.util.sync_query()
        for job_name, create_time in abort_jobs:
            
            owner = self.util.get_job_owner(job_name)
            try:
                pa = self.all_jobs.get(job_name,None)
                if pa:
                    pid = pa[0].pid
                    ji = pa[1]
                    #p.terminate()
                    self.util.shell_exec('kill -9 %s' % pid) #SIGKILL is better than SIGTERM?
                    ps = multiprocessing.Process(target=self.abort,args=(owner,job_name,ji,self.user_jobs))
                    ps.start()
                    #ps.join()
                    #lastly remove this job on CHPC Master node
                    #self.util.shell_exec('rm -fr %s' % os.path.join(self.util.CLUSTER_JOB_DIR,str(ji)))
                        
            except Exception,e:
                print e
        
        
        tmp_job_dict = {}  
        tmp_jobs = []

        for job_name,create_time in new_jobs:
            #job name is the input path.
            if not self.all_jobs.has_key(job_name):  #can not accept same job twice
                
                #print self.util.extract_email_from_cmd(job_name)
                job_owner = self.util.get_job_owner(job_name)
                
                #if self.util.WHITE_BOX:
                #    if not job_owner in self.util.WHITE_BOX: #only accept jobs from users in white box
                #        continue
                    
                #if self.util.BLACK_BOX:
                #    if '*' in self.util.BLACK_BOX or job_owner in self.util.BLACK_BOX:#block any jobs from users in black box
                #        continue
                    
                job_priority = self.util.get_job_priority(job_owner)
                
                job_walltime = self.util.get_job_walltime(job_priority)
                                    
                if not self.util.PRIORITY_USER.has_key(job_owner):#not a priority user
                    
                    tmp_job_dict.setdefault(job_owner,[]).append(job_name)
                    
                    myjobs =  self.user_jobs.get(job_owner) #how many jobs are running under this user?
                    if myjobs: 
                        _total = len(myjobs) + len(tmp_job_dict[job_owner])
                        #if len(myjobs) < self.util.NUM_JOBS_MAX:
                        if _total <= self.util.NUM_JOBS_MAX:
                            #tmp_queue.put((job_name,job_owner,job_priority,job_walltime))
                            tmp_jobs.append((job_name,job_owner,job_priority,job_walltime))
                            
                    else:#no other running jobs
                        _total = len(tmp_job_dict[job_owner])
                        if _total <= self.util.NUM_JOBS_MAX:
                        #if len(tmp_job_dict[job_owner]) <= self.util.NUM_JOBS_MAX:
                            #tmp_queue.put((job_name,job_owner,job_priority,job_walltime))
                            tmp_jobs.append((job_name,job_owner,job_priority,job_walltime))
                else:
                    #tmp_queue.put((job_name,job_owner,job_priority,job_walltime))
                    tmp_jobs.append((job_name,job_owner,job_priority,job_walltime))

        available_nodes = self.util.NUM_CLUSTER_NODES_AVAILABLE - len(self.all_jobs)
        _job_rank = 0

        for t in tmp_jobs:
            self.pending_jobs.put(t) #push it back
            
        tmp_jobs2 = []
        while not self.pending_jobs.empty():
            job_name,job_owner,job_priority,job_walltime = self.pending_jobs.get()#pop a pending job from the queue
            _job_rank += 1
            #if _job_rank > available_nodes:
            #print 'Rank:', job_name,_job_rank
            #    my_rank = _job_rank-available_nodes 
            #    self.util.accept_pending_job(job_name,my_rank)
            self.util.accept_pending_job(job_name,_job_rank)
            
            tmp_jobs2.append((job_name,job_owner,job_priority,job_walltime)) #push it back

        for t in tmp_jobs2:
            self.pending_jobs.put(t) #push it back
            
        for k in range(0,available_nodes):
            if not self.pending_jobs.empty():
                job_name,job_owner,job_priority,job_walltime = self.pending_jobs.get()#pop a pending job from the queue
                #print 'Run:', job_name
                myjobs =  self.user_jobs.get(job_owner)#check out other jobs belong to this user
                if myjobs: #this user has some job in process
                    if not self.util.PRIORITY_USER.has_key(job_owner):#this user is not a priority user
                        if len(myjobs) >= self.util.NUM_JOBS_UNLIMITED_WALLTIME: #and submitted more than 3 jobs
                            job_walltime = self.util.HOURS_LIMITED_WALLTIME
                else: #first job of this job
                    myjobs = []
                self.job_index += 1
                myjobs.append(self.job_index)
                self.user_jobs[job_owner] = myjobs
                #print job_idx,job_owner,job_name,job_walltime
                p = multiprocessing.Process(target=self.begin,args=(job_owner,job_name,self.job_index,job_walltime,self.user_jobs))
                p.start()
                self.all_jobs[job_name] = (p,self.job_index)
    
    def update_app_data(self):
        self.util.sync_get_app()
        self.util.sync_get_data()

    def update_alive_jobs(self):
        #eliminate zombie <defunct> child process once it is done 
        ret = {}
        for k,v in self.all_jobs.items():
            if not v[0].is_alive():
                #print '%s is dead' % k
                v[0].terminate()
            else:
                ret[k] = v
        self.all_jobs = ret
        
    def begin(self,owner,name,index,walltime,jobdict):
        #start a new job
        self.job.Job(owner,name,index,walltime,jobdict).start()
    
    
    def abort(self,owner,name,index,jobdict):
        #abort a running job
        self.job.Job(owner,name,index,0,jobdict).abort()
        
if __name__=='__main__':
    TomatoMain().run()


class VersionConf2:
    def __init__(self):
        self.current_version=None
        self.current_apppath = []
        
    def parse_version(self,fn = 'conf/1.txt'):
        """
        Get all supported pipelines defined in configuration file "fn"
        
        @fn: the version configuration file
        @return: {"Version:Pipeline": [(Index,AppName,AppPath,AppParameters)], }        
        """
        m = {}
        p = {}
        x  = re.compile('^\[\s*(\d+)\s*\]$') #[ 2 ]
        y  = re.compile('^\[\s*(current)\s*\]$')#[ current ]
        b_current = False
        k=None
        for line in file(fn):
            line = line.strip()
            if line:
                if not line.startswith('#'):
                    if b_current:
                        self.current = line
                        continue
                    
                    ver = x.findall(line)
                    current_ver = y.findall(line)
                    if current_ver:
                        b_current = True
                        continue
                    
                    if line.startswith('@'):
                        pipelineName,pipelineSteps = line.split(':')
                        p.setdefault(k,[]).append((pipelineName.strip(),pipelineSteps.strip()))
                    elif ver:
                        k = ver[0]
                    else:
                        if k:
                            #print line
                            vs = line.split(':')
                            Index = vs[0].strip() 
                            AppName = vs[1].strip() 
                            AppPath = vs[2].strip()
                            #self.apppath.append(AppPath)
                            Genome = vs[3].strip()
                            AppDesc = vs[4].strip()
                            AppParameters = ':'.join(vs[5:]).strip() 
                            m.setdefault(k,[]).append((Index,AppName,AppPath,Genome,AppDesc,AppParameters))
                            
        if not self.current_version:
            self.current_version = sorted(m.keys())[-1]
        
        for v in m[self.current_version]:
            self.current_apppath.append(v[2])
        
        ret = {}
        for versionNumber,pipelines in p.items():
            steps = m[versionNumber]
            for pipelineName,pipelineSteps in pipelines:
                for i in pipelineSteps.split(','):
                    for j in steps:
                        if j[0] == i:
                            #cmd = os.path.join(j[2],j[1])+' '+j[5]
                            ret.setdefault(versionNumber+':'+pipelineName,[]).append((j[0],j[1],j[2],j[5]))
        return ret

    def get_current_version(self):
        if not self.current_version:
            self.parse_version()
        return str(self.current_version)
    
    def get_app_path(self):
        return list(Set([i for i in self.current_apppath if i]))
    
    def get_pipeline(self,versionNumber=None,pipelineName=None):
        """get a versioned pipeline if both versionNumber and pipelineName are present.
        otherwise return all pipelines in current version.
        """
        ks = []
        m = self.parse_version()
        if not versionNumber:
            versionNumber = self.current_version
        
        for k in m.keys():
            vs = k.split(':')
            version = vs[0]
            pipeline = vs[1]
            if version== versionNumber:
                #print pipeline
                ks.append(pipeline)
            if not pipelineName:
                return ks
        #print versionNumber+':'+pipelineName
        ret = m.get(versionNumber+':'+pipelineName,None)
        if not ret:
            raise Exception('Pipeline "%s" does not exist in version: "%s"' % (pipelineName,versionNumber))
        return ret

