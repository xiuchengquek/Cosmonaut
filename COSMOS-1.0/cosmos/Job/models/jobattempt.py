import os,re,json,glob

from cosmos import django_settings

from django.conf import settings as django_conf_settings, global_settings

#custom template context processor for web interface
django_conf_settings.configure(
    TEMPLATE_CONTEXT_PROCESSORS=global_settings.TEMPLATE_CONTEXT_PROCESSORS + ('cosmos.utils.context_processor.contproc',), **django_settings.__dict__)

from django.db              import models
from django.core.validators import RegexValidator
from django.utils           import timezone
from picklefield.fields     import PickledObjectField

#from cosmos import session

class JobAttempt(models.Model):
    """
    An attempt at running a task.
    """
    class Meta:
        app_label = 'Job'
        db_table  = 'Job_jobattempt'

    queue_status_choices = (
        ('not_queued','JobAttempt has not been submitted to the JobAttempt Queue yet'),
        ('queued','JobAttempt is in the JobAttempt Queue and is waiting to run, is running, or has finished'),
        ('completed','JobAttempt has completed'), #this means job.finished() has been executed.  use drmaa_state to find out if job was successful or failed.
    )
    created_on  = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)

    jobManager = models.ForeignKey('Job.JobManager')
    task       = models.ForeignKey('Workflow.Task')

    #job status and input fields
    queue_status        = models.CharField(max_length=150, default="not_queued",choices = queue_status_choices)
    successful          = models.BooleanField(default=False)
    status_details      = models.TextField(max_length=1000,default='',help_text='Extra information about status')
    command             = models.TextField(max_length=1000,default='')
    command_script_path = models.TextField(max_length=1000)
    jobName             = models.CharField(max_length=150,validators = [RegexValidator(regex='^[A-Z0-9_]*$')])

    #drmaa related input fields
    drmaa_native_specification = models.CharField(max_length=400, default='')

    #drmaa related and job output fields
    drmaa_jobID = models.BigIntegerField(null=True) #drmaa drmaa_jobID, note: not database primary key

    #time
    system_time = models.IntegerField(null=True,help_text='Amount of time that this process has been scheduled in kernel mode.')
    user_time   = models.IntegerField(null=True,help_text='Amount of time that this process has been scheduled in user   mode.')
    cpu_time    = models.IntegerField(null=True,help_text='system_time + user_time')
    wall_time   = models.IntegerField(null=True,help_text='Elapsed real (wall clock) time used by the process.')
    percent_cpu = models.IntegerField(null=True,help_text='(cpu_time / wall_time) * 100')

    #memory
    avg_rss_mem                      = models.IntegerField(null=True,help_text='Average resident set size (Kb)')
    max_rss_mem                      = models.IntegerField(null=True,help_text='Maximum resident set size (Kb)')
    single_proc_max_peak_rss         = models.IntegerField(null=True,help_text='Maximum single process rss used (Kb)')

    avg_virtual_mem                  = models.IntegerField(null=True,help_text='Average virtual memory used (Kb)')
    max_virtual_mem                  = models.IntegerField(null=True,help_text='Maximum virtual memory used (Kb)')
    single_proc_max_peak_virtual_mem = models.IntegerField(null=True,help_text='Maximum single process virtual memory used (Kb)')

    major_page_faults                = models.IntegerField(null=True,help_text='The number of major faults the process has made which have required loading a memory page from disk')
    minor_page_faults                = models.IntegerField(null=True,help_text='The number of minor faults the process has made which have not required loading a memory page from disk')

    avg_pte_mem                      = models.IntegerField(null=True,help_text='Average page table entries size (Kb)')
    max_pte_mem                      = models.IntegerField(null=True,help_text='Maximum page table entries size (Kb)')

    avg_locked_mem                   = models.IntegerField(null=True,help_text='Average locked memory size (Kb)')
    max_locked_mem                   = models.IntegerField(null=True,help_text='Maximum locked memory size (Kb)')

    avg_data_mem                     = models.IntegerField(null=True,help_text='Average size of data segments (Kb)')
    max_data_mem                     = models.IntegerField(null=True,help_text='Maximum size of data segments (Kb)')

    avg_lib_mem                      = models.IntegerField(null=True,help_text='Average library memory size (Kb)')
    max_lib_mem                      = models.IntegerField(null=True,help_text='Maximum library memory size (Kb)')

    #io
    nonvoluntary_context_switches    = models.IntegerField(null=True,help_text='Number of non voluntary context switches')
    voluntary_context_switches       = models.IntegerField(null=True,help_text='Number of voluntary context switches')
    block_io_delays                  = models.IntegerField(null=True,help_text='Aggregated block I/O delays')

    avg_fdsize                       = models.IntegerField(null=True,help_text='Average number of file descriptor slots allocated')
    max_fdsize                       = models.IntegerField(null=True,help_text='Maximum number of file descriptor slots allocated')

    #misc
    exit_status                      = models.IntegerField(null=True,help_text='Exit status of the primary process being profiled')
    #names                           = models.CharField(max_length=255,null=True,help_text='Names of all descendnt processes (there is always a python process for the profile.py script)')
    #pids                            = models.CharField(max_length=255,null=True,help_text='Pids of all the descendant processes')
    num_polls                        = models.IntegerField(null=True,help_text='Number of times the resource usage statistics were polled from /proc')
    num_processes                    = models.IntegerField(null=True,help_text='Total number of descendant processes that were spawned')
    SC_CLK_TCK                       = models.IntegerField(null=True,help_text='sysconf(_SC_CLK_TCK), an operating system variable that is usually equal to 100, or centiseconds')

    avg_num_threads                  = models.IntegerField(null=True,help_text='Average number of threads')
    max_num_threads                  = models.IntegerField(null=True,help_text='Maximum number of threads')


    profile_fields = [('time',[
        'user_time','system_time', 'cpu_time', 'wall_time', 'percent_cpu',
        ]),
                      ('memory',[
                          'avg_rss_mem','max_rss_mem','single_proc_max_peak_virtual_mem',
                          'avg_virtual_mem','max_virtual_mem','single_proc_max_peak_rss',
                          'minor_page_faults','major_page_faults',
                          'avg_pte_mem','max_pte_mem',
                          'avg_locked_mem','max_locked_mem',
                          'avg_data_mem','max_data_mem',
                          'avg_lib_mem','max_lib_mem',
                          ]),
                      ('i/o',[
                          'voluntary_context_switches', 'nonvoluntary_context_switches','block_io_delays',
                          'avg_fdsize', 'max_fdsize',
                          ]),
                      ('misc', [
                          #'exit_status','names', 'pids', 'num_polls','num_processes','SC_CLK_TCK',
                           'exit_status',                 'num_polls','num_processes','SC_CLK_TCK',
                          'avg_num_threads','max_num_threads',
                          ])
    ]

    extra_jobinfo = PickledObjectField(null=True,default=None)

    def __init__(self,*args,**kwargs):
        kwargs['created_on'] = timezone.now()
        super(JobAttempt,self).__init__(*args,**kwargs)

    @property
    def jobinfo_output_dir(self):
        return os.path.join(self.task.output_dir,'jobinfo')

    @staticmethod
    def profile_fields_as_list():
        """
        :returns: [profile_fields], a simple list of profile_field names, without their type information
        """
        return reduce(lambda x,y: x+y,[tf[1] for tf in JobAttempt.profile_fields])

    @property
    def workflow(self):
        """
        This jobattempt's workflow
        """
        self.task.workflow

    @property
    def resource_usage(self):
        """
        :returns: (name,value,help,type)
        """
        for type,fields in self.profile_fields:
            for field in fields:
                val = getattr(self,field)
                yield field, val, self._meta.get_field(field).help_text,type

    @property
    def resource_usage_short(self):
        """
        :returns: (name,value)
        """
        for field in JobAttempt.profile_fields_as_list():
            yield field, getattr(self,field)

    def update_from_profile_output(self):
        """
        Updates the resource usage from profile output
        """
        try:
            p = json.load(file(self.profile_output_path,'r'))
            for k,v in p.items():
                setattr(self,k,v)
        except ValueError:
            "Probably empty resource usage because command didn't exist"
            pass
        except IOError:
            "Job probably immediately failed so there's no job data"
            pass

    def get_status(self):
        return self.jobManager.get_jobAttempt_status(self)

    @property
    def STDOUT_filepath(self):
        """
        Returns the path to the STDOUT file
        """
        return os.path.join(self.jobinfo_output_dir,'cosmos_id_{0}.stdout'.format(self.id))
    @property
    def STDERR_filepath(self):
        """
        Returns the path to the STDERR file
        """
        return os.path.join(self.jobinfo_output_dir,'cosmos_id_{0}.stderr'.format(self.id))

    @property
    def STDOUT_txt(self):
        """
        The contents of the STDOUT file
        """
        path = self.STDOUT_filepath
        if path is None or not os.path.exists(path):
            return 'STDOUT file does not exist: {0}'.format(path)
        else:
            with open(path,'rb') as f:
                return f.read()
    @property
    def STDERR_txt(self):
        """
        The contents of the STDERR file
        """
        path = self.STDERR_filepath
        if path is None or not os.path.exists(path):
            return 'STDERR file does not exist: {0}'.format(path)
        else:
            with open(path,'rb') as f:
                return f.read()
    @property
    def profile_output_path(self):
        """
        Path to store a job's profile.py output
        """
        return os.path.join(self.jobinfo_output_dir,str(self.id)+'.profile')

    def get_command_shell_script_text(self):
        """
        Return the contents of the command.sh file
        """
        if not os.path.exists(self.command_script_path):
            return "Error: {0} does not exist".format(self.command_script_path)
        with open(self.command_script_path,'rb') as f:
            return f.read()

    def _hasFinished(self,successful,extra_jobinfo,status_details=''):
        """
        Function for JobManager to Run when this JobAttempt finishes
        """
        # Make sure output files actually exist: in picard.collectmultiplemetrics, output is a basename, not an exact name
        if successful:
            for o in self.task.output_files:
                if (not os.path.exists(o.path) or os.stat(o.path) == 0) and (not glob.glob(o.path+'*')):
                    successful = False
                    status_details = 'Job was done, but file {0} is missing or empty'.format(o.path)
                    break
                
        self.status_details = status_details
        self.successful     = successful
        self.extra_jobinfo  = extra_jobinfo
        self.queue_status   = 'finished'
        self.update_from_profile_output()
        self.finished_on    = timezone.now()
        self.save()

    @models.permalink
    def url(self):
        return ('jobAttempt_view',[str(self.id)])

    def __str__(self):
        return '<JobAttempt[{0}] [drmaa_jobId:{1}]>'.format(self.id,self.drmaa_jobID)

    def toString(self):
        attrs_to_list = ['command_script_text','successful','queue_status','STDOUT_filepath','STDERR_filepath']
        out = []
        for attr in attrs_to_list:
            out.append('{0}: {1}'.format(attr,getattr(self,attr)))

        return "\n".join(out)
