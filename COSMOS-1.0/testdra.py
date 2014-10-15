__author__ = 'quek'
#!/usr/bin/env python
2
3  import DRMAA
4  import os
5
6  def main():
    7      """Submit a job.
 8      Note, need file called sleeper.sh in home directory. An example:
 9      echo 'Hello World $1'
10      """
11      s=DRMAA.Session()
12      s.init()
13
14      print 'Creating job template'
15      jt = s.createJobTemplate()
16      jt.remoteCommand = os.getcwd() + '/sleeper.sh'
17      jt.args = ['42','Simon says:']
18      jt.joinFiles=True
19      jt.outputPath=":"+DRMAA.JobTemplate.HOME_DIRECTORY+'/tmp/DRMAA_JOB_OUT'
20
21      jobid = s.runJob(jt)
22      print 'Your job has been submitted with id ' + jobid
23
24      print 'Cleaning up'
25      s.deleteJobTemplate(jt)
26      s.exit()
27
28  if __name__=='__main__':
    29      main()