import os
import imp

class AppLocator(object):
    def __init__(self,app_dir):
        #each computational node has 16GB memory, 2TB Disk.
        self.util = imp.load_source('util','util.py')
        self.app_map={}
        self.app_dir = [a.replace(self.util.LOCAL_SERVER_APP_DIR,self.util.CLUSTER_APP_DIR) for a in app_dir]
        self.supported_extension = ('pl','py','jar','sh','bash')
        self._initialize()
    
    def _search_executable_script(self,suffix_of_script):
        ret = {}
        #msg = self.util.shell_exec('file -i `find %s -name "*.%s" 2>/dev/null`' % (self.util.CLUSTER_APP_DIR,suffix_of_script))
        for d in self.app_dir:
            msg = self.util.shell_exec('file -i `find %s -name "*.%s" 2>/dev/null`' % (d,suffix_of_script))
            if msg[0].startswith('Usage'):
                pass
                #ret = {}
            else:
                for m in msg:
                    full_path = [s.strip() for s in m.split(':')][0]
                    #ret[os.path.basename(full_path)] = self.suffix_to_app[suffix_of_script]+' '+full_path
                    ret[os.path.basename(full_path)] = full_path
        return ret
     
    def _search_executable_binary(self): #compiled native  binary executable files
        ret = {}
        for d in self.app_dir:
            msg = self.util.shell_exec('file `find %s -type f 2>/dev/null` | grep executable' % d)
            if msg:
                if msg[0].startswith('Usage'):
                    pass
                else:
                    for m in msg:
                        full_path = [s.strip() for s in m.split(':')][0]
                        ret[os.path.basename(full_path)] = full_path
        return ret

    def _initialize(self):
        """{'GenomeAnalysisTK.jar':'java -Xmx8g -jar /home/tool/gatk/GenomeAnalysisTK.jar',
        'SortSam.jar':'java -Xmx8g -jar /home/tool/picard-tools-1.36/SortSam.jar',
        'novoalign':'/home/tool/novoalign/novoalign',...}
        """
        self.app_map = self._search_executable_binary()
        for i in self.supported_extension:
            b = self._search_executable_script(i)
            self.app_map = dict(self.app_map,**b)
    
    def locate(self,command):
        cmds = []
        c = filter(None, [s.strip() for s in command.split(' ')])
        c[0] = self.app_map.get(c[0],c[0])
        return ' '.join(c)

class AppRuntimeMapper(object):
    def __init__(self):
        self.util = imp.load_source('util','util.py')
        self.ext_to_app ={'pl':'perl','py':'python','jar':'java -Xmx14g -jar','sh':'bash','bash':'bash'}
        
    def map(self,command):
        ret = []
        for cmd in command:
            c = cmd.split(' ')
            appname = c[0]
            extname = appname.split('.')
            if len(extname) > 1:
                ext=extname[-1]  
                rt = self.ext_to_app.get(ext,None)
                if rt:
                    c[0] = ' '.join((rt,c[0]))
            ret.append(' '.join(c))
        return ret
    
class DataLibraryFile(object):
    def __init__(self):
        self.util = imp.load_source('util','util.py')

    def get_data_files(self):
        """get all files in DATA path
        """
        ret = []
        for root, dirs, files in os.walk(self.util.CLUSTER_DATA_DIR):
            for name in files:
                ret.append(name)
        return ret

    def get_genome(self,aligner='nov'):
        """get all genome index files in DATA path
        """
        ret = {}
        for root, dirs, files in os.walk(self.util.CLUSTER_DATA_DIR):
            for name in files:
                try:
                    toks = name.split('.')
                    genome = toks[0]
                    app = toks[1]
                    platform = toks[2]
                    if platform in ('illumina','solid','bisulphite'):
                        if app==aligner:
                            ret.setdefault(platform,[]).append(genome)
                except:
                    pass
        m = {}
        for k,v in ret.items():
            m[k]= list(set(v)) 
        return m

