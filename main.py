#!/usr/bin/python
import multiprocessing
import Queue
import time
import os
import sys
import imp
import operator

class RockfishMain(object):
    def __init__(self):
        #tracking running jobs
        self.job_running = {} 
        
        #tracking the owner of each job
        self.job_owner = multiprocessing.Manager().dict()
        
        #tracking the final state of job, either "succcessful" or "failed"
        self.job_status = multiprocessing.Manager().dict()
        self.job_index = 0

        self.util = imp.load_source('util','util.py')
        if self.util.TEST:
            print 'Test mode'
            
        self.job = imp.load_source('job','job.py')
        
    def run(self):
        self.util.make_directories()
        self.util.clean_job_directory()
        print "[%s] Synchronizing ..." % self.util.now(),
        self._sync()
        print '[OK]'
        while True:
            self.util = imp.load_source('util','util.py')
            self.job = imp.load_source('job','job.py')
            self.load()
            time.sleep(self.util.RSYNC_QUERY_INTERVAL)
            self.update_alive_jobs()
            self._sync()
    
    def load(self):
        #temporarily stop receiving new jobs
        #return job_index,alive_jobs
        #job_idx = job_index
        #job_dict = alive_jobs
        
        #clean every time?
        new_jobs,abort_jobs = self.util.sync_query()
        #terminate aborted jobs
        for job_name, create_time in abort_jobs:
            owner = self.util.get_job_owner(job_name)
            try:
                #the job may be in the states of "running" or "waiting" 
                pa = self.job_running.get(job_name,None)
                if pa: #
                    pid = pa[0].pid
                    ji = pa[1]
                    #p.terminate()
                    self.util.shell_exec('kill -9 %s' % pid) #SIGKILL is better than SIGTERM?
                    ps = multiprocessing.Process(target=self.abort,args=(owner,job_name,ji,self.job_owner))
                    ps.start()
                    #ps.join()
                    #lastly remove this job on cluster's master node
                    #self.util.shell_exec('rm -fr %s' % os.path.join(self.util.CLUSTER_MASTER_JOB_DIR,str(ji)))
                else:
                    pass

            except Exception,e:
                print e
        
        if abort_jobs: #clear the tag files on source machine
            del_cmd = []
            for i in map(operator.itemgetter(0),abort_jobs):
                del_cmd.append(' '.join((os.path.join(i,self.util.FILE_TAG_ABORT),os.path.join(i,self.util.FILE_TAG_BEGIN),os.path.join(i,self.util.FILE_TAG_DRYRUN))))
            #print 'rm -f %s'% ' '.join(del_cmd)
            self.util.shell_exec_remote('rm -f %s'%' '.join(del_cmd))
        
        tmp_job_dict = {}  
        tmp_jobs = [] #accepted jobs
        pending_jobs = Queue.PriorityQueue()

        #accept new jobs
        for job_name,create_time in new_jobs:
            #job name is the input path.
            if not self.job_running.has_key(job_name):  #if this job was not running
                #print self.util.extract_email_from_cmd(job_name)
                job_owner = self.util.get_job_owner(job_name)
                priority,max_num_jobs,wallhours = self.util.get_user_priority(job_owner)
                tmp_job_dict.setdefault(job_owner,[]).append(job_name)
                myjobs =  self.job_owner.get(job_owner) #check out how many jobs are running under this user
                if myjobs: 
                    _total = len(myjobs) + len(tmp_job_dict[job_owner])
                    if _total < max_num_jobs: #
                        tmp_jobs.append((job_name,job_owner,priority,wallhours))
                            
                else:#no other running jobs
                    _total = len(tmp_job_dict[job_owner])
                    if _total < max_num_jobs:
                        tmp_jobs.append((job_name,job_owner,priority,wallhours))
                        
                """                    
                job_priority = self.util.get_job_priority(job_owner)
                job_walltime = self.util.get_job_walltime(job_priority)
                if not self.util.PRIORITY_USER.has_key(job_owner):#not a priority user
                    tmp_job_dict.setdefault(job_owner,[]).append(job_name)
                    myjobs =  self.job_owner.get(job_owner) #how many jobs are running under this user?
                    if myjobs: 
                        _total = len(myjobs) + len(tmp_job_dict[job_owner])
                        if _total <= self.util.NUM_JOBS_MAX:
                            tmp_jobs.append((job_name,job_owner,job_priority,job_walltime))
                            
                    else:#no other running jobs
                        _total = len(tmp_job_dict[job_owner])
                        if _total <= self.util.NUM_JOBS_MAX:
                            tmp_jobs.append((job_name,job_owner,job_priority,job_walltime))
                else:
                    tmp_jobs.append((job_name,job_owner,job_priority,job_walltime))
                """
                
        available_nodes = self.util.NUM_CLUSTER_NODES_AVAILABLE - len(self.job_running)
        _job_rank = 0

        for t in tmp_jobs:
            pending_jobs.put(t) #push it back
        
        #return
        #change the RANK
        """
        print "Changed the Rank"
        tmp_jobs2 = []
        while not pending_jobs.empty():
            job_name,job_owner,job_priority,job_walltime = pending_jobs.get()#pop a pending job from the queue
            print "GET"
            _job_rank += 1
            #if _job_rank > available_nodes:
            #print 'Rank:', job_name,_job_rank
            #    my_rank = _job_rank-available_nodes 
            #    self.util.accept_pending_job(job_name,my_rank)
            print "ACCEPT"
            self.util.accept_pending_job(job_name,_job_rank)
            
            print "APPEND"
            tmp_jobs2.append((job_name,job_owner,job_priority,job_walltime)) #push it back

        print "PUT"
        for t in tmp_jobs2:
            pending_jobs.put(t) #push it back
        print "OK"
        return
        """
        
        #print job_name,job_owner,job_priority,job_walltime
        for k in range(0,available_nodes):
            if not pending_jobs.empty():
                job_name,job_owner,job_priority,job_walltime = pending_jobs.get()#pop a pending job from the queue
                #print 'Run:', job_name
                myjobs =  self.job_owner.get(job_owner,[])#check out other jobs belong to this user
                """
                if myjobs: #this user has some job in process
                    _priority,_max_num_jobs,_wallhours = self.util.get_user_priority(job_owner)
                    if len(myjobs) >= _max_num_jobs: #and submitted more than 3 jobs
                        job_walltime = self.util.HOURS_LIMITED_WALLTIME
                    #if not self.util.PRIORITY_USER.has_key(job_owner):#this user is not a priority user
                    #    if len(myjobs) >= self.util.NUM_JOBS_UNLIMITED_WALLTIME: #and submitted more than 3 jobs
                    #        job_walltime = self.util.HOURS_LIMITED_WALLTIME
                else: #first job of this job
                    myjobs = []
                """
                self.job_index += 1
                myjobs.append(self.job_index)
                self.job_owner[job_owner] = myjobs
                #print job_idx,job_owner,job_name,job_walltime
                p = multiprocessing.Process(target=self.begin,args=(job_owner,job_name,self.job_index,job_walltime,self.job_owner))
                p.start()
                self.job_running[job_name] = (p,self.job_index)
    
    def _sync(self):
        self.util.sync_get_app()
        self.util.sync_get_data()
        self.util.sync_get_pipeline()
        self.util.sync_workers()

    def update_alive_jobs(self):
        #eliminate zombie <defunct> child process once it is done 
        ret = {}
        for k,v in self.job_running.items():
            if not v[0].is_alive():
                #print '%s is dead' % k
                v[0].terminate()
            else:
                ret[k] = v
        self.job_running = ret
        
    def begin(self,owner,name,index,walltime,jobdict):
        #start a new job
        isSuccessful = self.job.Job(owner,name,index,walltime,jobdict).start()
        if not isSuccessful:
            print "Failed:",'%s:%s:%d' % (owner,name,index)
        self.job_status['%s:%s:%d' % (owner,name,index)] = isSuccessful 
    
    def abort(self,owner,name,index,jobdict):
        #abort a running job
        self.job.Job(owner,name,index,0,jobdict).abort()
        
if __name__=='__main__':
    RockfishMain().run()

