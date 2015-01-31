import smtplib
import socket
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import Encoders
import os
import imp

import ntlm.smtp
from ntlm.smtp import ntlm_authenticate

#sudo apt-get install python-ntlm
#git clone https://github.com/xulz/python-ntlm.git
#sudo cp python-ntlm/*  /usr/local/lib/python2.7/dist-packages/ntlm/

class Gmail:
    def __init__(self,to,job_name):
        """to= ('hello@world.com','A_123@abc.com')
        """
        self.util = imp.load_source('util','util.py')
        self.to = to
        self.job_name = job_name
    
    #def send(self,success=False,body_msg=None,body_file=None,additional_subject=None,attach=None):
    
    def send(self,subject,body_msg=None,body_file=None,additional_subject=None,attach=None):
        return
        #subject = "Job %s finished" % self.job_name
        #else:
        #    subject = "Job %s failed" % self.job_name
        if additional_subject:
            subject = "%s, %s" % (subject,additional_subject)
        
        body=[]
        if body_msg:
            body.append(body_msg)
            body.append('\n')
            body.append('='*50)
            body.append('\n')
            
        if body_file:
            try:
                fh = open(body_file)
                body.append(fh.read())
                fh.close()
            
            except:
                pass
        
        for recipient in self.to:
            try: 
                msg = MIMEMultipart()
                msg['From'] = "BioinformaticsService"        
                msg['To'] = recipient        
                msg['Subject'] = subject
                #msg.attach(MIMEText(body))
                msg.attach(MIMEText('\n'.join(body)))
                
                if attach:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(open(attach,'rb').read())
                    Encoders.encode_base64(part)
                    part.add_header('Content-Disposition','attachment; filename="%s"' % os.path.basename(attach))
                    msg.attach(part)
                
                #for Gmail only
                mailServer = smtplib.SMTP(self.util.SMTP_SERVER)
                #mailServer.set_debuglevel(True)
                mailServer.ehlo()
                
                if self.util.SMTP_SERVER_USER=="smtp.gmail.com":
                    mailServer.starttls()
                    mailServer.ehlo()
                    mailServer.login(self.util.SMTP_SERVER_USER,self.util.SMTP_SERVER_PASSWORD)
                else:
                    ntlm_authenticate(mailServer,r"%s" % self.util.SMTP_SERVER_USER,self.util.SMTP_SERVER_PASSWORD)
                mailServer.sendmail("BioinformaticsService", recipient, msg.as_string())
                mailServer.close()
            except Exception,e:
                #pass
                print "Failed to send email:",e

def test():
    em = Gmail(['plone3@gmail.com'],'A')
    em.connect()
    #em.send(True)
    print 'OK'

    
