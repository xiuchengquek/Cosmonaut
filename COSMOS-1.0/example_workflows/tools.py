from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile

class Sleep(Tool):
    inputs = ['*']
    forward_input = True

    def cmd(self,i,s,p):
        return 'sleep 10'

class ECHO(Tool):
    outputs = ['txt']
    time_req = 1 #min
    
    def cmd (self,i,s,p):
        return 'echo {p[word]} > $OUT.txt'
    
class CAT(Tool):
    inputs = ['txt']
    outputs = [TaskFile(fmt='txt',basename='cat.txt')]
    time_req = 1
    
    def cmd(self,i,s,p):
        return 'cat {input} > $OUT.txt', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }
    
class PASTE(Tool):
    inputs = ['txt']
    outputs = [TaskFile(name='txt',basename='paste.txt',persist=True)]
    time_req = 1
    
    def cmd(self,i,s,p):
        return 'paste {input} > $OUT.txt', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }
    
class WC(Tool):
    inputs = ['txt']
    outputs = ['txt']
    time_req = 1

    default_para = { 'args': '' }
    
    def cmd(self,i,s,p):
        return 'wc {input} > $OUT.txt', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }

class FAIL(Tool):
    outputs = ['txt']
    def cmd(self,i,s,p):

        return '$OUT.txt __fail__'

class MD5Sum(Tool):
    inputs = ['*']
    outputs = ['md5']

    def cmd(self,i,s,p):
        return 'md5sum {inp}', dict(inp=" ".join(map(lambda x: str(x), i)))
