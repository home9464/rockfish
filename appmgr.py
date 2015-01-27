import os,sys,imp,re,inspect,time

from version import VersionConf

class AppManager(object):
    def __init__(self,version,pipeline_name,inputs,global_params={}):
        """
        ver(int): requested version
        genome(str): genome, hg19, mm9, etc.
        platform(str): illumina, solid, etc.
        links([], steps):
        
        inputs:
        1 A_1.txt.gz, A_2.txt.gz
        2 .fq.gz
        3 .bam
        4 .sam
        5 .sam.gz
        5 .vcf
        """
        self.util = imp.load_source('util','util.py')
        
        #add to commands
        self.commands = []
        
        vobj = VersionConf(version)
        
        self.apppath = vobj.get_app_path()
         
        self.version = vobj.get_current_version()
        
        self.datapath = vobj.get_data_path() 
        
        self.links = vobj.get_pipeline(pipeline_name)
        #print self.links
        if not self.links:
            raise Exception('Pipeline [%s] was not found in version %s' % (pipeline_name,version))
        
        
        self.platform = 'illumina'
        if global_params.has_key('-p'):
            self.platform = global_params['-p'][0]
        
        
        if not global_params.has_key('-g'):
            raise Exception("No genome : use -g")
        else:
            self.genome = global_params['-g'][0]
        
        #change the products in-place
        #[(group1),(group2),(group3),...]
        #print 'INPUT:',inputs
        
        self.products = inputs #[[]], it is a nested list , "list of list" 
        
        self.control_case = False
        if len(inputs)==2: #case-control
            self.control_case = True
           
        self.sys_params = global_params

        self.delete_intermediate_file = True
        if self.sys_params.has_key('-r'):
            self.delete_intermediate_file = False

        #self.gatk_call_variants_with_multiple_bams = False
        #if self.sys_params.has_key('-m'):
        #    self.gatk_call_variants_with_multiple_bams = True

        self.KNOWN_INDEL_INTERVALS  = '%s.indel.intervals' % self.genome

    def _normalize_app_name(self,appname):
        """
        GenomeAnalysisTK.jar -T UnifiedGenotyper
        bwa aln
        VarScan.jar somaticFilter
        """
        ret = [i for i in appname.split(' ') if i.strip()]
          
    def get_command(self):
        t = {}
        k = []
        for i in self.links:
            t.setdefault(i[0],[]).append(i[1:])
            if k:
                if k[-1] == i[0]:
                    continue  
            k.append(i[0])
        #print t['9']
        for j in k: #for each link
            cmds = t[j]
            appName, appPath, Genome,appParams = cmds[0]
            app = appName.split(' ')[0].replace('.','_') #app.jar -> app_jar
            getattr(AppManager,app)(self,cmds)
            
        return self.commands
    
    def get_product(self):
        return self.products
    
    def pair_fastq(self,files):
        """@return: [[A_1,A_2],[B_1,B_2],...]"""
        k1 = {}
        fs = sorted(files)
        if len(fs)==1:
            return [fs]
        for f in fs:
            #m1 =  re.match('(.*)_1\.(.+)',f)
            #m2 =  re.match('(.*)_2\.(.+)',f)
            m1 =  re.match(self.util.RE_PAIRED_END_READ_FILE_1,f)
            m2 =  re.match(self.util.RE_PAIRED_END_READ_FILE_2,f)
            if m1 or m2:
                if m1:
                    k1.setdefault(''.join(m1.groups()),[]).append(f) 
                if m2:
                    k1.setdefault(''.join(m2.groups()),[]).append(f)
            else: #any other files that do not end with '1.x' or '2.x'.
                fn = '.'.join(f.split('.')[:-1])
                k1.setdefault(fn,[]).append(f)
        return k1.values()

    
    def get_file_type(self):
        for i in self.products:#i is []
            for k in i:
                j = k.lower()
                if j.endswith('.txt'):
                    return 'FASTQ'
                elif j.endswith('.txt.gz'):
                    return 'FASTQ'
                elif j.endswith('.fq'):
                    return 'FASTQ'
                elif j.endswith('.fq.gz'):
                    return 'FASTQ'
                elif j.endswith('.fastq'):
                    return 'FASTQ'
                elif j.endswith('.fastq.gz'):
                    return 'FASTQ'
                elif j.endswith('.sam'):
                    return 'SAM'
                elif j.endswith('.sam.gz'):
                    return 'SAM'
                elif j.endswith('.bam'):
                    return 'BAM'
                elif j.endswith('.vcf'):
                    return 'VCF'
                elif j.endswith('.annovar'):
                    return 'ANNOVAR'
                elif j.endswith('.pileup'):
                    return 'PILEUP'
                elif j.endswith('.mpileup'):
                    return 'MPILEUP'
                elif j.endswith('.gvf'):
                    return 'GVF'
                elif j.endswith('.cdr'):
                    return 'CDR'
                else:
                    raise Exception("File format was not recognized")
            
    ########################
    #Novoalign
    ########################
    def _get_app(self,conf,required_file_type):
        """
        myname = inspect.stack()[1][3] # get parent's name -> who is calling me?'
        """
        if not self.products:
            raise Exception("Can not find any input files")
        
        t = self.get_file_type()
        
        if required_file_type:
            if not t in required_file_type.split(','):
                raise Exception('Application [%s] Only accept file format [%s]' % (conf[0][0],required_file_type)) 
         
        m = []
        for c in conf: #if there is grouped apps, the len(conf) > 1, otherwise len(conf)==1 
            appname,apppath,genome,appparams = c
            if genome:
                if not self.genome in genome.split(','):
                    raise Exception('[%s] can not be used for genome [%s] in version [%s], available genomes are %s' % (appname,self.genome,self.version,genome))
            x = '%s %s' % (os.path.join(apppath.replace(self.util.LOCAL_SERVER_APP_DIR,self.util.CLUSTER_APP_DIR),appname),appparams)
            m.append(x)
        if not m:
            raise Exception('Failed to get necessary application ')
        return m

    def tophat2(self,conf):
        """
        tophat -p 4 -o 2338_10 /home/hadoop/bio/data/index/hg19/bowtie2/ucsc.hg19 Hi_2338_10_R1.fastq.gz Hi_2338_10_R2.fastq.gz
        """
        m = self._get_app(conf,'FASTQ')
        app = m[0]
        
        sam_header= "--rg-id readgroup --rg-sample sample --rg-library library --rg-platform illumina"
        
        current = []
        for p in self.products:
            pair = self.pair_fastq(p)#i is []
            tmp = []
            #genome_index_file = '%s.nov.%s.nix' % (self.genome,self.platform)
            genome_index_file = '/rockfish/bio/data/index/hg19/bowtie2/ucsc.hg19'
            for i in pair:#i is []
                cc = None
                if len(i)==1: #single-end sequence read file
                    f = i[0]
                    output = f.rstrip('.gz')
                    output = output.rstrip('.gzip')
                    output = output.rstrip('.txt')
                    output = output.rstrip('.fastq')
                    output = output.rstrip('.fq')
                    output = output.rstrip('.fasta')
                    output = output.rstrip('.fa')
                    cc = "%s %s -o %s %s" % (app,sam_header,output,genome_index_file,f)
                
                elif len(i)==2: #pair-end sequence read file
                    f1,f2 = i
                    output = os.path.commonprefix((f1,f2)).strip('_')
                    if not output:
                        output = 'tmp'
                    cc = "%s %s -o %s %s %s %s" % (app,sam_header,output,genome_index_file,f1,f2)
                    
                else:
                    raise Exception('Two many input files: %d' % len(self.products))
            
                if cc:
                    params = self.sys_params.get('-tophat2',None)
                    if params:
                        reserved_params = ['-o']
                        cc = self.util.update_cmd_params(cc,reserved_params,params[0],first_param_index=1)
                        self.commands.append('%s' % cc)
                    else:
                        self.commands.append(cc)
                    tmp.append(output+'/accepted_hits.bam')
                else:
                    pass

            if tmp:
                current.append(tmp)
        self.products = current 
        
    def novoalign(self,conf):
        """predefined_parameters -> novoalign,/PATH/TO/novoalign,parameters
        """
        m = self._get_app(conf,'FASTQ')
        app = m[0]
        
        sam_header= "-o SAM $'@RG\\tID:rockfish\\tPL:illumina\\tLB:libtmp\\tSM:%s\\tCN:fluidigm'"
        
        current = []
        for p in self.products:
            pair = self.pair_fastq(p)#i is []
            tmp = []
            genome_index_file = '%s.nov.%s.nix' % (self.genome,self.platform)
            for i in pair:#i is []
                cc = None
                if len(i)==1: #single-end sequence read file
                    f = i[0]
                    output = f.rstrip('.gz')
                    output = output.rstrip('.gzip')
                    output = output.rstrip('.txt')
                    output = output.rstrip('.fastq')
                    output = output.rstrip('.fq')
                    output = output.rstrip('.fasta')
                    output = output.rstrip('.fa')
                    cmd_string = "%s %s -d %s -f %s | gzip >%s.sam.gz"
                    cc = cmd_string %(app,sam_header%output,genome_index_file,f,output)
                
                elif len(i)==2: #pair-end sequence read file
                    f1,f2 = i
                    output = os.path.commonprefix((f1,f2)).strip('_')
                    if not output:
                        output = 'tmp'
                    cmd_string = "%s %s -d %s -f %s %s| gzip >%s.sam.gz"
                    cc = cmd_string %(app,sam_header%output,genome_index_file,f1,f2,output)

                    
                else:
                    raise Exception('Two many input files: %d' % len(self.products))
            
                if cc:
                    params = self.sys_params.get('-novoalign',None)
                    if params:
                        reserved_params = ['-d','-f','-o']
                        cc = self.util.update_cmd_params(cc,reserved_params,params[0],first_param_index=1)
                        self.commands.append('%s' % cc)
                    else:
                        self.commands.append(cc)
                    tmp.append(output+'.sam.gz')
                else:
                    pass
            if tmp:
                current.append(tmp)
        self.products = current 

    def bwa(self,conf):
        m = self._get_app(conf,'FASTQ')
        
        bwamem = m[0]
        #print m
        current = []

        sam_header = "-r '@RG\\tID:rockfish\\tPL:illumina\\tLB:libtmp\\tSM:%s\\tCN:fluidigm'"
        for p in self.products:
            pair = self.pair_fastq(p)#i is []
            tmp = []
            
            #genome_index_file = '%s.bwa.%s' % (self.genome,self.platform)
            genome_index_file = '/rockfish/data/index/hg19/bwa/ucsc.hg19'
            
            for i in pair:#i is []
                cmd_sam = None
                if len(i)==1: #single-end sequence read file
                    f = i[0]
                    output = f.split('.')[0]
                    cmd = "%s %s %s %s | gzip > %s.sam.gz" % (bwamem,sam_header%output,genome_index_file,f,output)
                
                elif len(i)==2: #pair-end sequence read file
                    f1,f2 = i
                    output = os.path.commonprefix((f1,f2)).strip('_')
                    if not output:
                        output = 'tmp'
                    cmd = "%s %s %s %s %s | gzip > %s.sam.gz" % (bwamem,sam_header%output,genome_index_file,f1,f2,output)
                
                else:
                    raise Exception('Two many input files: %d' % len(self.inputs))
            
                #if self.user_defined_params:
                user_params = self.sys_params.get('-bwamem',None)
                if user_params:
                    reserved_params = []
                    if cmd_aln[0]:
                        self.commands.append('%s' % self.util.update_cmd_params(cmd,reserved_params,user_params[0],first_param_index=2))
                    if cmd_aln[1]:
                        self.commands.append('%s' % self.util.update_cmd_params(cmd,reserved_params,user_params[0],first_param_index=2))
                        
                else: #no user-defined parameters
                    self.commands.append(cmd)
                    
                tmp.append('%s.sam.gz' % output)
            if tmp:
                current.append(tmp)
        self.products = current 
    
    ########################
    #Picard
    ########################
    def picard(self,conf,appk,required_inputfiletype,outputfilesuffix='.bam',b_change_product=True):
        """
        """
        m = self._get_app(conf,required_inputfiletype)
        app = m[0]
        appn = os.path.basename(app.split(' ')[0])

        suffix = None
        if appk:
            if outputfilesuffix:
                suffix = '.%s.%s' % (appk,outputfilesuffix)
        else:
            if outputfilesuffix:
                suffix = '.%s' % outputfilesuffix
        current = []
        for p in self.products:
            tmp = []
            for i in p:
                px = re.compile(suffix)
                if not px.findall(i): #no need to sort again
                    fout = None
                    if i.endswith('.sam'):
                        fbase = i.rstrip('.sam')
                    elif i.endswith('.sam.gz'):
                        fbase = i.rstrip('.sam.gz')
                    elif i.endswith('.bam'):
                        fbase = i.rstrip('.bam')
                    else:
                        fbase = 'tmp'
                    if suffix:
                        fout = fbase+suffix
                    else:
                        fout = fbase
                    
                    self.commands.append('%s INPUT=%s OUTPUT=%s' % (app,i,fout))
                    if self.delete_intermediate_file:
                        if appk: #BuildBamIndex
                            self.commands.append('rm -f %s' % fout.rstrip(suffix)+'.ba*')
                        
                    if b_change_product:
                        tmp.append(fout)
                    else:
                        tmp.append(i)
                
                else: #no need to do again. SortBam will not work on "sort.bam" 
                    tmp.append(i)
            current.append(tmp)
        self.products=current  

    def cufflinks(self,conf):
        """
        /rockfish/app/cufflinks/2.2.1/cufflinks -p 4 -o cufflinks_Undetermined Undetermined/accepted_hits.bam
        """
        m = self._get_app(conf,"BAM")
        app = m[0]
        
        current = []
        #print self.products
        tmp = []
        for p in self.products:
            for k in p:
                fout = 'cufflink_%s' % os.path.dirname(p[0])
                cmd = '%s -o %s %s' % (app,fout,p[0])
                self.commands.append(cmd)
                if self.delete_intermediate_file:
                    self.commands.append('rm -f %s' % p[0])                    
                tmp.append(fout)
        current.append(tmp)
        self.products = current 

    ########################
    #Picard
    ########################
    def SortSam_jar(self,conf):
        appk = 'sort'
        filetype='BAM,SAM'
        outsuffix = 'bam'
        self.picard(conf,appk,filetype,outsuffix)
        
    ########################
    #Picard
    ########################
    def FixMateInformation_jar(self,conf):
        appk = 'mate'
        filetype='BAM,SAM'
        outsuffix = 'bam'
        self.picard(conf,appk,filetype,outsuffix)

    ########################
    #Picard
    ########################
    def MarkDuplicates_jar(self,conf):
        appk = 'dup'
        filetype='BAM'
        outsuffix = 'bam'
        m = self._get_app(conf,filetype)
        app = m[0]
        appn = os.path.basename(app.split(' ')[0])

        suffix = '.%s.%s' % (appk,outsuffix)
                        
        current = []
        for p in self.products:
            tmp = []
            for i in p:
                px = re.compile(suffix)
                if not px.findall(i): #no need to sort again
                    fout = None
                    if i.endswith('.sam'):
                        fbase = i.rstrip('.sam')
                    elif i.endswith('.sam.gz'):
                        fbase = i.rstrip('.sam.gz')
                    elif i.endswith('.bam'):
                        fbase = i.rstrip('.bam')
                    else:
                        fbase = 'tmp'
                    
                    fout = fbase+suffix
                    
                    self.commands.append('%s INPUT=%s OUTPUT=%s M=%s.dupmetrics' % (app,i,fout,fbase))
                    if self.delete_intermediate_file:
                        if appk: #BuildBamIndex
                            self.commands.append('rm -f %s' % fout.rstrip(suffix)+'.ba*')
                    tmp.append(fout)
                else: #no need to do again. SortBam will not work on "sort.bam" 
                    tmp.append(i)
            current.append(tmp)
        self.products = current
        
    
    ########################
    #Picard
    ########################
    def BuildBamIndex_jar(self,conf):
        appk = None
        filetype='BAM'
        outsuffix = 'bai'
        #no changes on existing products
        self.picard(conf,appk,filetype,outsuffix,False)
        
    ########################
    #Picard
    ########################
    def CalculateHsMetrics_jar(self,conf):
        appk = None
        filetype='BAM'
        outsuffix = 'matrics'
        #no changes on existing products
        self.picard(conf,appk,filetype,outsuffix,False)

    ########################
    #Picard
    ########################
    def CollectInsertSizeMetrics_jar(self,conf):
        appk = None
        filetype='BAM'
        outsuffix = 'InsertSizeMetrics'
        #no changes on existing products
        self.picard(conf,appk,filetype,outsuffix,False)

    ########################
    #Picard
    ########################
    def CollectMultipleMetrics_jar(self,conf):
        appk = None
        filetype='BAM'
        outsuffix = None
        #no changes on existing products
        self.picard(conf,appk,filetype,outsuffix,False)

    ########################
    #Picard
    ########################
    def CollectRnaSeqMetrics_jar(self,conf):
        """
        REF_FLAT=File    Gene annotations in refFlat form. Format described here: http://genome.ucsc.edu/goldenPath/gbdDescriptionsOld.html#RefFlat Required.
        """
        appk = None
        filetype='BAM'
        outsuffix = 'RnaSeqMetrics'
        #no changes on existing products
        self.picard(conf,appk,filetype,outsuffix,False)
        
    ########################
    #Picard
    ########################
    def ReorderSam_jar(self,conf):
        
        #appk = 'order'
        #filetype='SAM,BAM'
        #outsuffix = 'bam'
        #self.picard(conf,appk,filetype,outsuffix)

        appk = 'order'
        filetype='BAM,SAM'
        outsuffix = 'bam'
        m = self._get_app(conf,filetype)
        app = m[0]
        appn = os.path.basename(app.split(' ')[0])

        suffix = '.%s.%s' % (appk,outsuffix)
                        
        current = []
        for p in self.products:
            tmp = []
            for i in p:
                px = re.compile(suffix)
                if not px.findall(i): #no need to sort again
                    fout = None
                    if i.endswith('.sam'):
                        fbase = i.rstrip('.sam')
                    elif i.endswith('.sam.gz'):
                        fbase = i.rstrip('.sam.gz')
                    elif i.endswith('.bam'):
                        fbase = i.rstrip('.bam')
                    else:
                        fbase = 'tmp'
                    
                    fout = fbase+suffix
                    
                    self.commands.append('%s INPUT=%s OUTPUT=%s REFERENCE=%s.fasta' % (app,i,fout,self.genome))
                    if self.delete_intermediate_file:
                        if appk: #BuildBamIndex
                            self.commands.append('rm -f %s' % fout.rstrip(suffix)+'.ba*')
                    tmp.append(fout)
                else: #no need to do again. SortBam will not work on "sort.bam" 
                    tmp.append(i)
            current.append(tmp)
        self.products = current
        
        
    ########################
    #Picard
    ########################
    def CleanSam_jar(self,conf):
        appk = 'clean'
        filetype='SAM'
        outsuffix = 'sam'
        self.picard(conf,appk,filetype,outsuffix)

    ########################
    #GATK
    ########################

    def GenomeAnalysisTK_jar(self,conf):
        
        #m = self._get_app(conf,'FASTQ')
        m = self._get_app(conf,None)
        app = m[0]
        t = re.compile('^.+?\s+-T\s+(.+?)\s.*').findall(app)
        if not t:
            raise Exception('-T AppName is required to run GATK')
        tx = t[0]
        
        current = []
        if tx=='UnifiedGenotyper':
            t = self.get_file_type()
            if not t == 'BAM':
                raise Exception('BAM file is required to run GATK-UnifiedGenotyper')
            #reserved_params = ['-I','-o','--dbsnp','-glm']
            reserved_params = ['-I','-o','-R']
            if self.control_case:
                for p in self.products:
                    tmp = []
                    _prefix = os.path.commonprefix(p).strip('_').strip()
                    if _prefix:
                        fvcf = '%s.vcf' % _prefix
                    else:
                        fvcf = 'tmp.vcf'
                    cmd_params = '-R %s.fasta -I %s -o %s' % (self.genome,' -I '.join(p),fvcf)
                    user_params = self.sys_params.get('-UnifiedGenotyper',None)
                    if user_params:
                        cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                    self.commands.append('%s %s' % (app,cmd_params))
                    tmp.append(fvcf)
                    current.append(tmp)
            else:
                for p in self.products:
                    tmp = []
                    for fbam in p:
                        #fvcf = '%s' % fbam.split('.')[0]+'.vcf'
                        fvcf = fbam.rstrip('.bam')+'.vcf'
                        #cmd_params = '-I %s -o %s --dbsnp %s' % (fbam,fvcf,self.dbsnp)
                        cmd_params = '-R %s.fasta -I %s -o %s' % (self.genome,fbam,fvcf)
                        user_params = self.sys_params.get('-UnifiedGenotyper',None)
                        if user_params:
                            cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                        self.commands.append('%s %s' % (app,cmd_params))
                        tmp.append(fvcf)
                    current.append(tmp)
            self.products = current

        if tx=='HaplotypeCaller':
            t = self.get_file_type()
            if not t == 'BAM':
                raise Exception('BAM file is required to run GATK-HaplotypeCaller')
            #reserved_params = ['-I','-o','--dbsnp','-glm']
            reserved_params = ['-I','-o','-R']

            """
            if self.gatk_call_variants_with_multiple_bams:
                for p in self.products:
                    tmp = []
                    _prefix = os.path.commonprefix(p).strip('_').strip()
                    if _prefix:
                        fvcf = '%s.vcf' % _prefix
                    else:
                        fvcf = 'tmp.vcf'
                
                    #cmd_params = '-I %s -o %s --dbsnp %s' % (' -I '.join(self.products),fn_vcf_output,self.dbsnp)
                    cmd_params = '-R %s.fasta -I %s -o %s' % (self.genome,' -I '.join(p),fvcf)
                    user_params = self.sys_params.get('-HaplotypeCaller',None)
                    if user_params:
                        cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                    self.commands.append('%s %s' % (app,cmd_params))
                    tmp.append(fvcf)
                    current.append(tmp)
            """
            if self.control_case:
                for p in self.products:
                    tmp = []
                    _prefix = os.path.commonprefix(p).strip('_').strip()
                    if _prefix:
                        fvcf = '%s.vcf' % _prefix
                    else:
                        fvcf = 'tmp.vcf'
                    cmd_params = '-R %s.fasta -I %s -o %s' % (self.genome,' -I '.join(p),fvcf)
                    user_params = self.sys_params.get('-HaplotypeCaller',None)
                    if user_params:
                        cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                    self.commands.append('%s %s' % (app,cmd_params))
                    tmp.append(fvcf)
                    current.append(tmp)
            
            else:
                for p in self.products:
                    tmp = []
                    for fbam in p:
                        #fvcf = '%s' % fbam.split('.')[0]+'.vcf'
                        fvcf = fbam.rstrip('.bam')+'.vcf'
                        #cmd_params = '-I %s -o %s --dbsnp %s' % (fbam,fvcf,self.dbsnp)
                        cmd_params = '-R %s.fasta -I %s -o %s' % (self.genome,fbam,fvcf)
                        user_params = self.sys_params.get('-HaplotypeCaller',None)
                        if user_params:
                            cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                        self.commands.append('%s %s' % (app,cmd_params))
                        tmp.append(fvcf)
                    current.append(tmp)
            self.products = current
        
        if tx== 'RealignerTargetCreator':
            #-T RealignerTargetCreator -R ucsc.hg19.fasta -o hg19.1000G_biallelic.indels.intervals --known 1000G_biallelic.indels.hg19.vcf
            pass
        
        if tx=='IndelRealigner':
            t = self.get_file_type()
            if not t == 'BAM':
                raise Exception('BAM file is required to run GATK-IndelRealigner')
            px = re.compile(".realign.")
            reserved_params = ['-I','-o','-R']
            for p in self.products:
                tmp = []
                for fbam in p:
                    if not px.findall(fbam):
                        fbam_output = fbam.rstrip('bam')+'realign.bam'
                        cmd_params = '-R %s.fasta -targetIntervals %s -I %s -o %s' % (self.genome,self.KNOWN_INDEL_INTERVALS,fbam,fbam_output)
                        self.commands.append('%s %s' % (app,cmd_params))
                    
                        if self.delete_intermediate_file:
                            self.commands.append('rm -f %s' % fbam.rstrip('.bam')+'.ba*')
                        tmp.append(fbam_output)
                    else:
                        tmp.append(fbam_input)
                current.append(tmp)
            self.products = current
        
        if tx=='BaseRecalibrator':
            for p in self.products:
                for fbam in p:
                    fbase = fbam.rstrip('bam')
                    foutput = fbase+'grp'
                    #app = 'GenomeAnalysisTK.jar -T IndelRealigner -R %s -targetIntervals %s -known %s' % (self.genome_fasta,self.hg19_indel_interval,self.hg19_known_indel_vcf)
                    #params = '-R %s.fasta -knownSites %s -I %s -o %s' % (self.genome,self.dbSNP,fbam,foutput)
                    params = '-R %s.fasta -I %s -o %s' % (self.genome,fbam,foutput)
                    self.commands.append('%s %s' % (app,params))
        
        if tx=='PrintReads':
            current = []
            px = re.compile(".recal.")
            for p in self.products:
                tmp = []
                for fbam in p:
                    if not px.findall(fbam):
                        fbase = fbam.rstrip('.bam')
                        foutput = fbase+'.recal.bam'
                        params = '-R %s.fasta -I %s -BQSR %s.grp -o %s' % (self.genome,fbam,fbase,foutput) 
                        self.commands.append('%s %s' % (app,params))
                        if self.delete_intermediate_file:
                            self.commands.append('rm -f %s' % fbam.rstrip('.bam')+'.ba*')
                            self.commands.append('rm -f %s' % fbam.rstrip('.bam')+'.grp')
                        tmp.append(foutput)
                    else:
                        tmp.append(fbam)
                current.append(tmp)
            self.products = current
        
        if tx=='VariantRecalibrator':
            t = self.get_file_type()
            if not t == 'VCF':
                raise Exception('VCF file is required to run GATK-VariantRecalibrator')
            
            for p in self.products:
                tmp = []
                for fvcf in p: 
                    frecal = fvcf.rstrip('.vcf')+'.recal'
                    ftranches = fvcf.rstrip('.vcf')+'.tranches'
                    #cmd_params = '-resource:hapmap,known=false,training=true,truth=true,prior=15.0 %s.hapmap.vcf \
                    #-resource:omni,known=false,training=true,truth=false,prior=12.0 %s.omni.vcf \
                    #-resource:dbsnp,known=true,training=false,truth=false,prior=8.0 %s.dbsnp.vcf \
                    #-an QD -an DP -an ReadPosRankSum -an MQRankSum -an FS -an MQ --maxGaussians 4 \
                    #-input %s -recalFile %s -tranchesFile %s' % (self.reference_genome,self.reference_genome,self.reference_genome,fvcf,frecal,ftranches)
                    cmd_params = '-R %s.fasta -input %s -recalFile %s -tranchesFile %s' % (self.genome,fvcf,frecal,ftranches)
                    reserved_params = ['-input','-recalFile','-R','-tranchesFile']
                    user_params = self.sys_params.get('-VariantRecalibrator',None)
                    if user_params:
                        cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                    self.commands.append('%s %s' % (app,cmd_params))
                    tmp.append(fvcf)
                current.append(tmp)
            self.products = current


        if tx=='ApplyRecalibration':
            ts_filter_level = '99.0'
            
            for p in self.products:
                tmp = []
                for f in p:
                    if f.endswith('.vcf'):
                        frecal = f.rstrip('.vcf')+'.recal'
                        ftranches = f.rstrip('.vcf')+'.tranches'
                        fout = f.rstrip('.vcf')+'.tmp.vcf'
                        reserved_params = ['-R','-input','-o','-recalFile','-tranchesFile']
                        cmd_params = '-recalFile %s -tranchesFile %s --ts_filter_level %s -input %s -o %s' % (frecal,ftranches,ts_filter_level,f,fout)
                        user_params = self.sys_params.get('-ApplyRecalibration',None)
                        if user_params:
                            cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                        self.commands.append('%s %s' % (app,cmd_params))
                        if self.delete_intermediate_file:
                            self.commands.append('rm -f %s' % frecal)
                            self.commands.append('rm -f %s' % ftranches)
                        tmp.append(fout)
                current.append(tmp)
            self.products = current   
                
        if tx=='SelectVariants':
            for p in self.products:
                tmp = []
                for f in p:
                    fout = f.rstrip('.tmp.vcf')+'.recal.vcf'
                    reserved_params = ['-R','--variant','-o']
                    cmd_params = '--variant %s -o %s' % (f,fout)
                    user_params = self.sys_params.get('-SelectVariants',None)
                    if user_params:
                        cmd_params = self.util.update_cmd_params(cmd_params,reserved_params,user_params[0])
                    self.commands.append('%s %s' % (app,cmd_params))
                    if self.delete_intermediate_file:
                        self.commands.append('rm -f %s' % f)
                    tmp.append(fout)
                current.append(tmp)
            self.products = current
               
        if tx=='DepthOfCoverage':
            for p in self.products:
                for f in p:
                    fout = f.rstrip('bam')+'coverage'
                    params = '-R %s.fasta -I %s -o %s' % (self.genome,f,fout)
                    self.commands.append('%s %s' % (app,params))
        

        if tx=='VariantFiltration':
            """    
            $$$
            #For SNPs
            java -jar GenomeAnalysisTK.jar -T VariantFiltration -R hg19.fasta --variant x1.bwa.gatk.snp.vcf -o x1.bwa.gatk.snp.filter.vcf -cluster 3 -window 10 \
            --filterExpression "MQ < 40.0" --filterName "MQ<40.0" \
            --filterExpression "QD < 2.0" --filterName "QD<2.0" \
            --filterExpression  "FS > 60.0" --filterName "FS>60.0" \
            --filterExpression  "HaplotypeScore > 13.0" --filterName "HaplotypeScore>13.0" \
            -filterExpression  "MQRankSum < -12.5" --filterName "MQRankSum<-12.5" \
            --filterExpression  "ReadPosRankSum < -8.0" --filterName "ReadPosRankSum<-8.0"
        
            #For Indels
            java -jar GenomeAnalysisTK.jar -T VariantFiltration -R hg19.fasta --variant x1.bwa.gatk.indel.vcf -o x1.bwa.gatk.indel.filter.vcf -cluster 3 -window 10 \
            --filterExpression "QD < 2.0" --filterName "QD<2.0" \
            --filterExpression "FS > 200.0" --filterName "FS>200.0" \
            --filterExpression "InbreedingCoeff < -0.8" --filterName "InbreedingCoeff<-0.8" \
            --filterExpression "ReadPosRankSum < -20.0" --filterName "ReadPosRankSum<-20.0"

            AA ancestral allele
            AC allele count in genotypes, for each ALT allele, in the same order as listed
            AF allele frequency for each ALT allele in the same order as listed: use this when estimated from primary data, not called genotypes
            AN total number of alleles in called genotypes
            BQ RMS base quality at this position
            CIGAR cigar string describing how to align an alternate allele to the reference allele
            DB dbSNP membership
            DP combined depth across samples, e.g. DP=154
            END end position of the variant described in this record (esp. for CNVs)
            H2 membership in hapmap2
            MQ RMS mapping quality, e.g. MQ=52
            MQ0 Number of MAPQ == 0 reads covering this record
            NS Number of samples with data
            SB strand bias at this position
            SOMATIC indicates that the record is a somatic mutation, for cancer genomics
            VALIDATED validated by follow-up experiment
        
        
        
            MQ: RMS mapping quality, e.g. MQ=52
        
            HaplotypeScore: Consistency of the site with two (and only two) segregating haplotypes.
                        Higher scores are indicative of regions with bad alignments, often leading to artifactual SNP and indel calls.

 
            QD(QualByDepth): Variant confidence (given as (AB+BB)/AA from the PLs) / unfiltered depth.
                    Low scores are indicative of false positive calls and artifacts.
                        
        
            FS(FisherStrand): Phred-scaled p-value using Fisher's Exact Test to detect strand bias in the reads.
                    the variation being seen on only the forward or only the reverse strand.  More bias is indicative of false positive calls.
        
        
            HRun(HomopolymerRun): Largest contiguous homopolymer run of the variant allele in either direction on the reference.
        
            VQSLOD: Only present when using Variant quality score recalibration. 
                    Log odds ratio of being a true variant versus being false under the trained gaussian mixture model.
        
            "QD < 2.0", "MQ < 40.0", "FS > 60.0", "HaplotypeScore > 13.0", "MQRankSum < -12.5", "ReadPosRankSum < -8.0".
        
            """
            for p in self.products:
                tmp = []
                for fin in p:
                    self.commands.append('vcf2snp.py %s' % fin)
                    vcf_snp = fin.rstrip('vcf')+'snp.vcf' 
                    vcf_indel = fin.rstrip('vcf')+'indel.vcf'

                    vcf_snp_filter = vcf_snp.rstrip('vcf')+'filter.vcf' 
                    vcf_indel_filter = vcf_indel.rstrip('vcf')+'filter.vcf' 
            
                    #-cluster 3 -window 10 --filterName "SNP" --filterExpression "QD<2.0||MQ<40.0||FS>60.0||HaplotypeScore>13.0" 
                    params_snp = '--variant %s -o %s ' % (vcf_snp,vcf_snp_filter)
                    self.commands.append('%s %s' % (app,params_snp))
                    vcf_snp_pass = vcf_snp_filter.rstrip('vcf')+'pass.vcf' 
                    self.commands.append("""awk '/^#/ {print $0; next} $7 == "PASS" {print $0}' %s  > %s """ % (vcf_snp_filter,vcf_snp_pass))


                    #--filterName "INDEL" --filterExpression "QD<2.0||FS>200.0"
                    params_indel = '--variant %s -o %s  ' % (vcf_indel,vcf_indel_filter)
                    self.commands.append('%s %s' % (app,params_indel))
                    vcf_indel_pass = vcf_indel_filter.rstrip('vcf')+'pass.vcf' 
                    self.commands.append("""awk '/^#/ {print $0; next} $7 == "PASS" {print $0}' %s  > %s """ % (vcf_indel_filter,vcf_indel_pass))
                    
            
                    vcf_filter = fin.rstrip('vcf')+'filter.vcf' 
                    self.commands.append('cat %s %s > %s' % (vcf_snp_pass,vcf_indel_pass,vcf_filter))
                    self.commands.append('rm -f %s' % (' '.join([vcf_snp,vcf_indel,vcf_snp_filter,vcf_indel_filter,vcf_snp_pass,vcf_indel_pass])))
                    
                    tmp.append(vcf_filter)
                current.append(tmp)
            self.products = current

        if tx=='ClipReads':
            #-T ClipReads -I my.bam -I your.bam -o my_and_your.clipped.bam -R Homo_sapiens_assembly18.fasta \
            #-XF seqsToClip.fasta -X CCCCC -CT "1-5,11-15" -QT 10
            current = []
            px = re.compile(".clip.")
            for p in self.products:
                tmp=[]
                for fbam in p:
                    if not px.findall(fbam):
                        fout = fbam.rstrip('.bam')+'.clip.bam'
                        params = '-I %s -o %s' % (fbam,fout)  
                        self.commands.append('%s %s' % (app,params))
                        if self.delete_intermediate_file:
                            self.commands.append('rm -f %s' % fbam)
                        tmp.append(fout)
                    else:
                        tmp.append(fbam)
                current.append(tmp)       
            self.products = current

        if tx=='ReduceReads':
            #http://www.broadinstitute.org/gatk/gatkdocs/org_broadinstitute_sting_gatk_walkers_compression_reducereads_ReduceReads.html
            #-T ReduceReads -R ref.fasta -I myData.bam -o myData.reduced.bam
            current = []
            px = re.compile(".reduce.")
            for p in self.products:
                tmp=[]
                for fbam in p:
                    if not px.findall(fbam):
                        fout = fbam.rstrip('.bam')+'.reduce.bam'
                        params = '-R %s.fasta -I %s -o %s' % (self.genome,fbam,fout)  
                        self.commands.append('%s %s' % (app,params))
                        if self.delete_intermediate_file:
                            self.commands.append('rm -f %s' % fbam)
                        tmp.append(fout)
                    else:
                        tmp.append(fbam)
                current.append(tmp)       
            self.products = current

        if tx=='VariantEval':
            #app = 'GenomeAnalysisTK.jar  -T VariantEval -EV TiTvVariantEvaluator -R %s.fasta -D %s -eval %s -l INFO' % (self.genome,self.dbsnp,fvcf)
            t = self.get_file_type()
            if not t == 'VCF':
                raise Exception('VCF file is required to run GATK-VariantEval')

            for p in self.products:
                for fvcf in p:
                    #app = 'GenomeAnalysisTK.jar  -T VariantEval -EV TiTvVariantEvaluator -R %s.fasta -D %s -eval %s -l INFO' % (self.genome,self.dbsnp,fvcf)
                    ks = ('TiTvVariantEvaluator','CountVariants','VariantQualityScore')
                    for k in ks: 
                        cmd_params = '-EV %s -R %s.fasta -l INFO -eval %s -o %s' % (k,self.genome,fvcf,fvcf.rstrip('vcf')+k)
                        self.commands.append('%s %s' % (app,cmd_params))
            


    ########################
    #samtools
    ########################
    def samtools(self,conf):
        
        appname,apppath,genome,appparams = conf[0]
        mainapp,subapp = appname.split(' ')
        
        if subapp=='mpileup':
            current = []
            m = self._get_app(conf,'BAM')
            app = m[0]
            cmd_params = '-f %s.fasta' % self.genome
            for p in self.products:
                tmp = []
                pname = os.path.commonprefix(p)
                if not pname:
                    pname = 'samtools.tmp'
                else:
                    pname = pname.strip('_')
                    #raise Exception('No Common Name')
                pname = '%s.%s' % (pname,subapp)
                self.commands.append('%s %s %s > %s' % (app,cmd_params,' '.join(p),pname))
                tmp.append(pname)
                current.append(tmp)
            self.products = current
               
        if subapp=='merge':
            current = []
            m = self._get_app(conf,'BAM')
            app = m[0]
            for p in self.products:
                tmp = []
                pname = os.path.commonprefix(p)
                if not pname:
                    raise Exception('No Common Name')
                fout = '%s.bam' % pname
                self.commands.append('%s %s %s' % (app,fout,' '.join(p)))
                tmp.append(fout)
                current.append(tmp)
            self.products = current   
        


    ########################
    #utility methods
    ########################

    def mergelanes(self):
        """
        merge multiple lane BAMs into one sample BAM.
        """
        current = []
        for p in self.products:
            tmp = []
            if len(p)==1:
                tmp.append(p)
            else:
                m = {}
                for i in p:
                    j = i.split('/')
                    if len(j)>1:
                        m.setdefault(j[0],[]).append(i)
                    
                for k,v in m.items():
                    common_prefix = os.path.commonprefix(v).strip('_').strip()
                    if not common_prefix:
                        common_prefix = 'merge'
                    if common_prefix.endswith(os.sep): #only the folder is same
                        common_prefix += 'merge'
                        
                    fout = '%s.bam' % common_prefix
                    cmd = 'samtools merge %s %s' % (fout,' '.join(v))
                    self.commands.append(AppLocator(self.apppath).locate(cmd))
                    if delete_intermediate_file:
                        self.commands.append('rm -f %s' % ' '.join([i.rstrip('.bam')+'.ba*' for i in v]))                    
                    tmp.append(fout)
            current.append(tmp)  
        self.products = current 
        #self.SortSam()

    def vcf2snpindel(self):
        """
        python vcf2snp.py x1.vcf
        
        #snp
        java -jar GenomeAnalysisTK.jar -T VariantFiltration -R hg19.fasta --variant x1.snp.vcf  -o x1.snp.filter.vcf -cluster 3 -window 10 --filterName "SNP" --filterExpression "QD<2.0||MQ<40.0||FS>60.0||HaplotypeScore>13.0"
        awk '/^#/ {print $0; next} $7 == "PASS" {print $0}' x1.snp.filter.vcf > x1.snp.filter.pass.vcf
        
        #indel
        java -jar GenomeAnalysisTK.jar -T VariantFiltration -R hg19.fasta --variant x1.indel.vcf  -o x1.indel.filter.vcf --filterName "INDEL" --filterExpression "QD<2.0||FS>200.0"
        awk '/^#/ {print $0; next} $7 == "PASS" {print $0}' x1.indel.filter.vcf > x1.indel.filter.pass.vcf
        
        cat x1.snp.filter.pass.vcf x1.indel.filter.pass.vcf > x1.pass.vcf
        """
        current = []  
        for p in self.products:
            tmp = []
            for fvcf in p:
                fsnp = fvcf.rstrip('vcf')+'snp.vcf' 
                findel = fvcf.rstrip('vcf')+'indel.vcf' 
                self.commands.append('vcf2snpindel %s' %  fvcf)
                tmp.append(fsnp)
                tmp.append(findel)
            current.append(tmp)  
        self.products = current
        

if __name__=='__main__':
    version = '1'
    #vobj = VersionConf(ver)
    #print vobj.get_current_version() 
    #print vobj.get_app_path() 
    #links = vobj.get_pipeline('@align -bam')
    #print links[0]
    #print links[1]

    #pipeline_name = '@align'
    #pipeline_name = '@align -bam'
    pipeline_name = '@snpindel -g hg19'
    #pipeline_name = '@pair'
    #pipeline_name = '@snpindel -g hg19 -bwa'
    genome = 'hg19'
    platform = 'illumina'
    inputs=['A_1.txt.gz','A_2.txt.gz']
    params={}
    ax = AppManager(version,pipeline_name,inputs,genome,platform,params)
    print '\n'.join(ax.get_command())
    #print ax.get_product()


    