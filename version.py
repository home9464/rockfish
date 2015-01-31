import os,sys,imp,re
from sets import Set
"""
parse the pipeline file "N.conf" where N is an integer indicates the pipeline version
"""
class VersionConf:
    def __init__(self,version=None):
        self.util = imp.load_source('util','util.py')
        self.pipelines = {} 
        self.path_app = []
        self.version = version
        if not version:
            self.version = self.get_current_version()
        self.fn = os.path.join(self.util.CLUSTER_PIPELINE_DIR,'%s.conf' % self.version)
        self._parse()
        
    def _parse(self):
        """
        parse the version configuration file
        """
        m = {}
        p = {}
        for line in file(self.fn):
            line = line.strip()
            if line:
                if not line.startswith('#'):#comments
                    
                    if line.startswith('@'): #pre-defined pipelines
                        pipelineName,pipelineSteps = line.split(':')
                        p[pipelineName] = [i.strip() for i in pipelineSteps.strip().split(',')] 
                        
                    else: #pre-defined components
                        vs = line.split(':')
                        Index = vs[0].strip() 
                        AppName = vs[1].strip() 
                        AppPath = vs[2].strip()
                        self.path_app.append(AppPath)
                        Genome = vs[3].strip()
                        AppDesc = vs[4].strip()
                        AppParameters = ':'.join(vs[5:]).strip()
                        
                        m.setdefault(Index,[]).append((Index,AppName,AppPath,Genome,AppDesc,AppParameters))
                            
        for pipelineName,commands in p.items():
            for i in commands:
                j = m.get(i,None)
                if j:
                    for k in j:
                        #print k
                        #self.pipelines.setdefault(pipelineName,[]).append((k[0],k[1],k[2],k[5]))
                        
                        self.pipelines.setdefault(pipelineName,[]).append((k[0],k[1],k[2],k[3],k[5]))
                        
    def get_current_version(self):
        myversion = None
        if self.version:
            return self.version
        
        else: 
            v = []
            px = re.compile('^(\d+)\.conf')
            py = re.compile('^v(\d+)')
            
            for root, dirs, files in os.walk(self.util.CLUSTER_PIPELINE_DIR):
                for f in files:
                    m = px.findall(f)
                    n = py.findall(f)
                    if m:   
                        v.append(int(m[0]))
                    if n:   
                        myversion = int(n[0])
                        
            if v:
                if myversion in v:
                    return str(myversion)
                else:
                    return str(sorted(v,reverse=True)[0])
            else:
                raise Exception('Can not find version configuration file.')
    
    def get_app_path(self):
        ret = []
        for i in self.path_app:
            j = os.path.join(self.util.LOCAL_SERVER_APP_DIR,i)
            ret.append(j)
        return list(Set(ret))
    
    def get_data_path(self):
        return os.path.join(self.util.CLUSTER_DATA_DIR,self.get_current_version())

    
    def get_pipeline(self,pipelineName=None):
        #print self.pipelines.items()
        if pipelineName:
            return self.pipelines.get(pipelineName,None)
        return self.pipelines
    