import os,sys,time

from django.db    import models
from django.utils import timezone

from cosmos               import session
from cosmos.config        import settings
from cosmos.utils.helpers import mkdir_p, spinning_cursor

from jobattempt import JobAttempt


class JobStatusError(Exception):
    pass

class JobManagerBase(models.Model):
    """
    Note there can only be one of these instantiated at a time
    """
    class Meta:
        abstract = True

    created_on = models.DateTimeField(null=True,default=None)
    def __init__(self,*args,**kwargs):
        kwargs['created_on'] = timezone.now()
        super(JobManagerBase,self).__init__(*args,**kwargs)

    @property
    def jobAttempts(self):
        """
        This JobManager's jobAttempts
        """
        return JobAttempt.objects.filter(jobManager=self)

    def delete(self,*args,**kwargs):
        """
        Deletes this job manager
        """
        self.jobAttempts.delete()
        super(JobManagerBase,self).__init__(self,*args,**kwargs)

    def get_numJobsQueued(self):
        """
        The number of queued jobs.
        """
        return self.jobAttempts.filter(queue_status = 'queued').count()

    def __create_command_sh(self,jobAttempt):
        """
        Create a shell script that will execute a command
        """
        try:
            with open(jobAttempt.command_script_path,'wb') as f:
                f.write("#!/bin/bash\n")
                f.write(jobAttempt.command)
            os.system('chmod 700 {0}'.format(jobAttempt.command_script_path))        
        except:
            print "Could not create shell script at {}".format(jobAttempt.command_script_path)
            raise

    def _create_cmd_str(self,jobAttempt):
        """
        The command to be stored in the command.sh script
        """        
        return "python {profile} -f {profile_out} {cmd_script_path}".format(
            profile = os.path.join(settings['cosmos_library_path'],'contrib/profile/profile.py'),
            #db             = jobAttempt.profile_output_path+'.sqlite',
            profile_out     = jobAttempt.profile_output_path,
            cmd_script_path = jobAttempt.command_script_path
        )


    def add_jobAttempt(self, task, command, jobName = "Generic_Job_Name"):
        """
        Adds a new JobAttempt
        :param command: The system command to run
        :param jobName: an optional name for the jobAttempt
        :param jobinfo_output_dir: the directory to story the stdout and stderr files
        """
        jobAttempt = JobAttempt(jobManager=self, task=task, command = command, jobName = jobName)
        jobAttempt.command_script_path = os.path.join(jobAttempt.jobinfo_output_dir,'command.sh')
        mkdir_p(jobAttempt.jobinfo_output_dir)

        self.__create_command_sh(jobAttempt)
        jobAttempt.save()

        return jobAttempt

    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        raise NotImplementedError

    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        raise NotImplementedError

    def yield_all_queued_jobs(self):
        "Yield all queued jobs."
        i=0
        while self.get_numJobsQueued() > 0:
            i+=1
            sys.stderr.write(spinning_cursor(i))

            job = self._check_for_finished_job()

            sys.stderr.write('\b')

            if job != None:
                yield job
            else:
                time.sleep(1) #dont sleep if a job just returned

    def _check_for_finished_job(self):
        """
        Checks to see if one of the queued jobAttempts are finished.  
        If a jobAttempt is finished, this method is responsible for calling:
        jobAttempt._hasFinished()

        :return: A finished jobAttempt, or None if all queued jobs are still running
        """
        raise NotImplementedError

    def submit_job(self,jobAttempt):
        """
        Submits and runs a job
        """
        if not jobAttempt.queue_status == 'not_queued': 
            raise JobStatusError, 'JobAttempt has already been submitted'

        jobAttempt.drmaa_native_specification = session.drmaa_spec(jobAttempt)

        # Will be redefined in sub files
        self._run_job(jobAttempt)  

        jobAttempt.queue_status = 'queued'
        jobAttempt.save()

        return jobAttempt

    def _run_job(self,jobAttempt):
        raise NotImplementedError


    def toString(self):
        return "JobManager %s, created on %s" % (self.id,self.created_on)
