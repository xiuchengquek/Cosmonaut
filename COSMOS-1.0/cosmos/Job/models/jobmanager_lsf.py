import os,sys,re
import shlex,subprocess

from jobattempt import JobAttempt
from jobmanager import JobManagerBase
from subprocess import Popen, PIPE

if sys.version_info[:2] >= (2,7):
    from subprocess import check_output

class JobStatusError(Exception):
    pass

all_processes = []
current_jobs = []

decode_lsf_state = dict([
    ('UNKWN', 'process status cannot be determined'),
    ('PEND',  'job is queued and active'),
    ('PSUSP', 'job suspended while pending'),
    ('RUN',   'job is running'),
    ('SSUSP', 'job is system suspended'),
    ('USUSP', 'job is user suspended'),
    ('DONE',  'job finished normally'),
    ('EXIT',  'job finished, but failed'),
    ])

def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()

# def _get_bjobs_stat():
#     """
#     returns a dict keyed by lsf job ids, who's values are a dict of bjob
#     information about the job
#     """

class JobManager(JobManagerBase):
    """
    Note there can only be one of these instantiated at a time
    """   
    class Meta:
        app_label = 'Job'
        db_table  = 'Job_jobmanager'

    def __init__(self,*args,**kwargs):
        super(JobManager,self).__init__(*args,**kwargs)


    ## Override JobManager.submit_job::_run_job() at jobmanager.py
    def _run_job(self,jobAttempt):

        cmd  = self._create_cmd_str(jobAttempt)
        out  = jobAttempt.STDOUT_filepath
        err  = jobAttempt.STDERR_filepath
        spec = jobAttempt.drmaa_native_specification

        bsub = 'bsub -o {0} -e {1} {2} {3}'.format(out,err,spec,cmd)

        ## Make sure stdout/err file exist here
        open(out,'a').close()
        open(err,'a').close()
        
        p = Popen(shlex.split(bsub), stdout=PIPE, env=os.environ, stderr=PIPE, preexec_fn=preexec_function())
        #p.wait()
        (stdoutdata, stderrdata) = p.communicate()
        
        lsf_id = re.search('Job <(\d+)>', stdoutdata).group(1)  # "Job <######> is submiited to queue <-------->"

        jobAttempt.drmaa_jobID = lsf_id
        current_jobs.append(lsf_id)
        all_processes.append(lsf_id)

    def _check_for_finished_job(self):

        # Get (id,stat) from 'bjobs -a' command
        cmd1 = "bjobs -a"
        cmd2 = "awk 'NR>1{print $1,$3}'" # get jobid, stat for the current user
        cmd3 = cmd1 + " | " + cmd2
        
        if sys.version_info[:2] >= (2,7):
            p1  = Popen(shlex.split(cmd1),stdout=PIPE);
            out = subprocess.check_output(shlex.split(cmd2),stdin=p1.stdout)
            p1.communicate()
        else:
            p1  = Popen(cmd3,shell=True,stdout=PIPE);  # security issue here.
            out = p1.communicate()[0]

        # change (id, stat) lists into dict
        bjobs_stat =  dict(map(lambda x: x.split(' '), filter(None, out.split('\n'))))

        #for id in current_jobs:  # current_jobs may have (yet) unlisted job ids
        for id in bjobs_stat:
            status = bjobs_stat[str(id)]
            if id in current_jobs and status in ['DONE','EXIT','UNKWN','ZOMBI']:
                current_jobs.remove(id)
                ja = JobAttempt.objects.get(drmaa_jobID=id)
                succ = True if status == 'DONE' else False
                ja._hasFinished(succ,status)
                return ja
        return None


    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        try:
            bjob = get_bjobs()[jobAttempt.drmaa_jobID]
            return decode_lsf_state[bjob['STAT']]
        except Exception:
            'unknown'


    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        os.system('bkill {0}'.format(jobAttempt.drmaa_jobID))

