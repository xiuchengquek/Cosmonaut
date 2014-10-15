"""
Workflow models
"""
#from cosmos import session
from cosmos.config import settings

from cosmos.Job.models.jobattempt import JobAttempt
if   settings['DRM'] == 'local':
    from cosmos.Job.models.jobmanager_local import JobManager
elif settings['DRM'] == 'LSF':
    from cosmos.Job.models.jobmanager_lsf   import JobManager
else:
    from cosmos.Job.models.jobmanager_drmaa import JobManager

from django.db import models, transaction
from django.db.models import Q,Count
from django.db.utils import IntegrityError


import os,sys,re,signal

from cosmos.utils.helpers import validate_name,validate_not_null, mkdir_p, folder_size, get_workflow_logger
from cosmos.utils import helpers

from django.core.exceptions import ValidationError

from picklefield.fields import PickledObjectField, dbsafe_decode

from django.utils import timezone

import networkx as nx

import pygraphviz as pgv

import hashlib

import signals

from cosmos.Workflow.templatetags import extras

#if (sys.version_info >= (2,7)):
#    from collections import OrderedDict    # default in Python 2.7
#else:
#    from ordereddict import OrderedDict    # for Python 2.6
from ordereddict import OrderedDict

status_choices=(
                ('successful','Successful'),
                ('no_attempt','No Attempt'),
                ('in_progress','In Progress'),
                ('failed','Failed')
                )


class TaskError              (Exception): pass
class TaskValidationError    (Exception): pass
class TaskFileValidationError(Exception): pass
class WorkflowError          (Exception): pass

i = 0
def get_tmp_id():
    global i
    i +=1
    return i

class TaskFile(models.Model, object):
    """
    Task File
    """
    path     = models.CharField(max_length=250, null=True)
    name     = models.CharField(max_length= 50, null=True)
    fmt      = models.CharField(max_length= 30, null=True) #file format
    basename = models.CharField(max_length= 50, null=True)

    persist                      = models.BooleanField(default=False)
    deleted_because_intermediate = models.BooleanField(default=False)


    def __init__(self,*args,**kwargs):
        """
        :param name: This is the name of the file, and is used as the key for obtaining it.
                     No Tool an have multiple TaskFiles with the same name.  Defaults to ``fmt``.
        :param fmt:  The format of the file.  Defaults to the extension of ``path``.
        :param path: The path to the file.  Required.
        :param basename: (str) The name to use for the file for auto-generated paths.  You must explicitly
            specify the extension of the filename, if you want one i.e. 'myfile.txt' not 'myfile'
        :param persist: (bool) If True, this file will not be deleted even if it is an intermediate
            file, and workflow.delete_intermediates is turned on.  Defaults to False.
        """
        super(TaskFile,self).__init__(*args,**kwargs)


        if not self.fmt and self.path:
            try:
                groups = re.search('\.([^\.]+)$',self.path).groups()
                self.fmt = groups[0]

            except AttributeError as e:
                raise AttributeError('{0}. Probably malformed path: {1}'.format(e,self.path))

        if not self.name and self.fmt:
            self.name = self.fmt
        if not self.fmt and self.name:
            self.fmt = self.name

        self.tmp_id = get_tmp_id()

        if not re.search("^[\w\.]+$",self.name):
            raise TaskFileValidationError, 'The taskfile.name must be alphanumeric. Failed name is "{0}"'.format(self.name)


    @property
    def workflow(self):
        return self.task.workflow

    @property
    def task(self):
        "The task this TaskFile is an output for"
        return Task.objects.get(_output_files__in = [self])

    @property
    def file_size(self,human_readable=True):
        "Size of the taskfile's output_dir"
        return folder_size(self.path,human_readable=human_readable) or 'NA'


    @property
    def sha1sum(self):
        return hashlib.sha1(file(self.path).read())

    def __str__(self):
        return "#F[{0}:{1}:{2}]".format(self.id if self.id else 't_{0}'.format(self.tmp_id),self.name,self.path)

    @models.permalink
    def url(self):
        return ('taskfile_view',[str(self.id)])

    def delete_because_intermediate(self):
        """
        Deletes this file and marks it as deleted because it is an intermediate file.
        """
        if not self.persist:
            self.workflow.log.info('Deleting Intermediate file {0}'.format(self.path))
            self.deleted_because_intermediate = True
            if os.path.isdir(self.path):
                os.system('rm -rf {0}'.format(os.path.join(self.path,'*')))
            else:
                os.system('echo "" > {0}'.format(self.path)) # overwrite with empty file
            self.save()
        else:
            raise WorkflowError, "{0} should not be deleted because persist=True".format(self)

class Workflow(models.Model):
    """   
    This is the master object.  It contains a list of :class:`Stage` which represent a pool of jobs
    that have no dependencies on each other and can be executed at the same time. 
    """
    name                 = models.CharField(max_length=250, unique=True)
    output_dir           = models.CharField(max_length=250)

    jobManager           = models.OneToOneField('Job.JobManager',null=True)

    dry_run              = models.BooleanField(default=False,help_text="don't execute anything")

    max_reattempts       = models.SmallIntegerField(default=6)

    default_queue        = models.CharField(max_length=255,default=None,null=True)

    delete_intermediates = models.BooleanField(default=False,help_text="Delete intermediate files")

    #cmd_executed = models.CharField(max_length=255,default=None,null=True)

    comments             = models.TextField(null=True,default=None)

    created_on           = models.DateTimeField(null=True, default=None)
    finished_on          = models.DateTimeField(null=True, default=None)

    def __init__(self, *args, **kwargs):
        kwargs['created_on'] = timezone.now()
        super(Workflow,self).__init__(*args, **kwargs)

        validate_name(self.name)
        #Validate unique name
        # if Workflow.objects.filter(name=self.name).exclude(pk=self.id).count() >0:
        #     raise ValidationError('Workflow with name {0} already exists.  Please choose a different one or use .__reload()'.format(self.name))

        self.log, self.log_path = get_workflow_logger(self)

    @property
    def tasks(self):
        """Tasks in this Workflow"""
        return Task.objects.filter(stage__in=self.stage_set.all())

    @property
    def task_edges(self):
        """Edges in this Workflow"""
        return TaskEdge.objects.filter(parent__in=self.tasks)

    @property
    def task_tags(self):
        """TaskTags in this Workflow"""
        return TaskTag.objects.filter(task__in=self.tasks)

    @property
    def task_files(self):
        "TaskFiles in this Stage"
        return TaskFile.objects.filter(task_output_set__in=self.tasks)

    @property
    def wall_time(self):
        """Time between this workflow's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on - self.created_on if self.finished_on else timezone.now().replace(microsecond=0) - self.created_on

    # not sure if this works so commented
    # @property
    # def total_stage_wall_time(self):
    #     """
    #     Sum(stage_wall_times).  Can be different from workflow.wall_time due to workflow stops and reloads.
    #     """
    #     times = map(lambda x: x['finished_on']-x['started_on'],Stage.objects.filter(workflow=self).values('finished_on','started_on'))
    #     return reduce(lambda x,y: x+y, filter(lambda wt: wt,times))

    @property
    def stages(self):
        """Stages in this Workflow"""
        return self.stage_set.all()

    @property
    def file_size(self,human_readable=True):
        """Size of the output directory"""
        return folder_size(self.output_dir,human_readable=human_readable)

    @property
    def log_txt(self):
        """Path to the logfile"""
        return file(self.log_path,'rb').read()

    @staticmethod
    def start(name,**kwargs):
        """
        Starts a workflow.  If a workflow with this name already exists, return the workflow.

        :param name: (str) A unique name for this workflow. All spaces are converted to underscores. Required.
        :param restart: (bool) Complete restart the workflow by deleting it and creating a new one. Optional.
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        :param delete_intermediates: (str) Deletes intermediate files to save scratch space.
        """

        kwargs.setdefault('dry_run',False)
        kwargs.setdefault('root_output_dir',settings['default_root_output_dir'])
        kwargs.setdefault('default_queue',settings['default_queue'])
        kwargs.setdefault('delete_intermediates', False)
        kwargs.setdefault('comments',None)

        restart = kwargs.pop('restart',False)
        prompt_confirm = kwargs.pop('prompt_confirm',True)

        #name = re.sub("\s","_",name)

        if restart:
            wf = Workflow.__restart(name=name, prompt_confirm=prompt_confirm, **kwargs)
        elif Workflow.objects.filter(name=name).count() > 0:
            wf = Workflow.__reload(name=name, prompt_confirm=prompt_confirm, **kwargs)
            #wf = Workflow.__resume(name=name, **kwargs)
        else:
            wf = Workflow.__create(name=name, **kwargs)

        #remove stale objects
        wf._delete_stale_objects()

        #terminate on ctrl+c
        def ctrl_c(signal,frame):
                wf.terminate()
        try:
            signal.signal(signal.SIGINT, ctrl_c)
        except ValueError: #signal only works in main thread and django complains
            pass

        return wf

    @staticmethod
    def __resume(name,dry_run, default_queue, delete_intermediates, root_output_dir, comments,**kwargs):
        """
        Resumes a workflow without deleting any unsuccessful tasks.  Provides a way to override workflow
        properties.
        Probably won't be called by anything except __reload

        see :py:meth:`start` for parameter definitions
        """

        if Workflow.objects.filter(name=name).count() == 0:
            raise ValidationError('Workflow {0} does not exist, cannot resume it'.format(name))
        wf = Workflow.objects.get(name=name)
        wf.dry_run=dry_run
        wf.default_queue=default_queue
        wf.delete_intermediates = delete_intermediates
        wf.output_dir = os.path.join(root_output_dir,wf.name.replace(' ','_'))
        if comments:
            wf.comments = comments

        wf.save()
        wf.log.info('Resuming {0}'.format(wf))
        return wf

    @staticmethod
    def __reload(name, dry_run, default_queue, delete_intermediates,delete_unsuccessful_stages=False, prompt_confirm=True,**kwargs):
        """
        Resumes a workflow, keeping successful tasks and deleting unsuccessful ones.

        see :py:meth:`start` for parameter definitions
        """
        #TODO create a delete_stages(stages) method, that works faster than deleting individual stages
        #TODO ideally just change the queryset manager to do this automatically
        if prompt_confirm and not helpers.confirm("Reloading the workflow, are you sure you want to delete any unsuccessful tasks in '{0}'?".format(name),default=True,timeout=30):
            print "Exiting."
            sys.exit(1)

        wf = Workflow.__resume(name,dry_run,default_queue,delete_intermediates,**kwargs)
        wf.finished_on = None
        #Stage.objects.filter(workflow=wf).update(order_in_workflow=None)

        if delete_unsuccessful_stages:
            #delete a stage with any unsuccessful tasks
            for s in wf.stages.filter(successful=False):
                s.delete()
        else:
            #delete a stage if ALL tasks are unsuccessful
            for s in Stage.objects.filter(workflow=wf).exclude(task__successful=True):
                wf.log.info('{0} has no successful tasks.'.format(s))
                s.delete()

        #Delete unsuccessful tasks
        utasks = wf.tasks.filter(successful=False)
        num_utasks = len(utasks)
        if num_utasks > 0:
            wf.bulk_delete_tasks(utasks)

            # Update stages that are resuming
            Stage.objects.filter(workflow=wf,successful=False,task__successful=True).update(
                successful=False,status='in_progress',finished_on=None
            )
        wf.save()
        return wf

    @staticmethod
    def __restart(name,prompt_confirm=True,**kwargs):
        """
        Restarts a workflow.  Will delete the old workflow and all of its files
        but will retain the old workflow id for convenience

        see :py:meth:`start` for parameter definitions

        """
        wf_id = None
        if Workflow.objects.filter(name=name).count():
            if prompt_confirm and not helpers.confirm("Are you sure you want to restart Workflow '{0}'?  All files will be deleted.".format(name),default=True,timeout=30):
                print "Exiting."
                sys.exit(1)
            old_wf = Workflow.objects.get(name=name)
            wf_id = old_wf.id
            old_wf.delete()

        new_wf = Workflow.__create(_wf_id=wf_id, name=name, **kwargs)

        return new_wf

    @staticmethod
    def __create(name,root_output_dir,_wf_id=None,**kwargs):
        """
        Creates a new workflow

        see :py:meth:`start` for parameter definitions
        :param _wf_id: the ID to use for creating a workflow
        """
        if Workflow.objects.filter(id=_wf_id).count(): raise ValidationError('Workflow with this _wf_id already exists')

        output_dir = os.path.join(root_output_dir,name.replace(' ','_'))
        mkdir_p(output_dir)

        wf = Workflow.objects.create(id=_wf_id,name=name, jobManager = JobManager.objects.create(),output_dir=output_dir, **kwargs)

        wf.log.info('Created Workflow {0}.'.format(wf))

        return wf


    def add_stage(self, name):
        """
        Adds a stage to this workflow.  If a stage with this name (in this Workflow) already exists,
        and it hasn't been added in this session yet, return the existing one after removing its
        finished_on datetimestamp and resetting it's order_in_workflow

        :param name: (str) The name of the stage, must be unique within this Workflow. Required.
        """
        #TODO name can't be "log" or change log dir to .log
        name = re.sub("\s","_",name)

        stage, created = Stage.objects.get_or_create(workflow=self,name=name)
        min,max = Stage.objects.filter(workflow=self).aggregate(
            models.Max('order_in_workflow'),
            models.Min('order_in_workflow')
        ).values()
        max = 0 if max is None else max
        if created:
            self.log.info('Creating {0}'.format(stage))
            stage.order_in_workflow = max+1
        else:
            self.log.info('Loading {0}'.format(stage))
            self.finished_on = None

        stage.save()
        return stage

    def _delete_stale_objects(self):
        """
        Deletes objects that are stale from the database.  This should only happens when the program exists ungracefully.
        """
        #TODO implement a catch all exception so that this never happens.  i think i can only do this if scripts are not run directly
        JobAttempt.objects.filter(task=None).delete()
        TaskFile.objects.filter(task_output_set=None).delete()
        TaskTag.objects.filter(task=None).delete()


    def terminate(self,exit=True):
        """
        Terminates this workflow and Exits
        :param exception: an exception to raise after terminating
        """
        self.log.warning("Terminating {0}...".format(self))
        self.save()
        jobAttempts = self.jobManager.jobAttempts.filter(queue_status='queued')
        self.log.info("Sending Terminate signal to all running jobs.")
        for ja in jobAttempts:
            self.jobManager.terminate_jobAttempt(ja)

        #this basically a bulk task._has_finished and jobattempt.hasFinished
        task_ids = jobAttempts.values('task')
        tasks = Task.objects.filter(pk__in=task_ids)

        self.log.info("Marking {0} terminated Tasks as failed.".format(tasks.count()))
        tasks.update(status = 'failed',finished_on = timezone.now())

        stages = Stage.objects.filter(Q(task__in=tasks)|Q(workflow=self,successful=False))
        self.log.info("Marking {0} terminated Stages as failed.".format(stages.count()))
        stages.update(status = 'failed',finished_on = timezone.now())

        self.log.info("Marking {0} terminated JobAttempts as failed.".format(len(jobAttempts)))
        jobAttempts.update(queue_status='finished',finished_on = timezone.now())

        self.comments = "{0}<br/>{1}".format(self.comments if self.comments else '',"terimate()ed")

        self.finished()

        self.log.info("Exiting.")

        if not exit:
            return
        else:
            sys.exit(1)

    def get_all_tag_keys_used(self):
        """Returns a set of all the keyword tags used on any task in this workflow"""
        return set([ d['key'] for d in TaskTag.objects.filter(task__in=self.tasks).values('key') ])

    def save_resource_usage_as_csv(self,filename):
        """Save resource usage to filename"""
        import csv
        profile_fields = JobAttempt.profile_fields_as_list()
        keys = ['stage'] + list(self.get_all_tag_keys_used()) + profile_fields
        f = open(filename, 'wb')
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writer.writerow(keys)
        for stage_resources in self.yield_stage_resource_usage():
            dict_writer.writerows(stage_resources)

    def yield_stage_resource_usage(self):
        """
        :yields: A dict of all resource usage, tags, and the name of the stage of every task
        """
        for stage in self.stages:
            dicts = [ dict(nru) for nru in stage.yield_task_resource_usage() ]
            for d in dicts: d['stage'] = re.sub('_',' ',stage.name)
            yield dicts

    @transaction.commit_on_success
    def bulk_save_tasks(self,tasks):
        """
        Does a bulk insert of tasks.  Identical tasks should not be in the database.

        :param tasks: (list) a list of tasks

        .. note:: this does not save task->taskfile relationships

        >>> tasks = [stage.new_task(pcmd='cmd1',save=False,{'i':1}),stage.new_task(pcmd='cmd2',save=False,{'i':2})]
        >>> stage.bulk_save_tasks(tasks)
        """
        self.log.info("Bulk adding {0} Tasks...".format(len(tasks)))

        #need to manually set IDs because there's no way to get them in the right order for tagging after a bulk create
        m = Task.objects.all().aggregate(models.Max('id'))['id__max']
        id_start =  m + 1 if m else 1
        for i,t in enumerate(tasks): t.id = id_start + i

        # try:
        Task.objects.bulk_create(tasks,batch_size=100)
        # except IntegrityError as e:
        #     for tpl, tasks in helpers.groupby(tasks + list(self.tasks), lambda t: (t.tags,t.stage)):
        #         if len(list(tasks)) > 1:
        #             print 'ERROR! Duplicate tags in {0}, which are:'.format(tpl[1])
        #             pprint.pprint(tpl[0])
        #
        #     raise(IntegrityError('{0}'.format(e)))

        #create output directories
        for t in tasks:
            os.system('mkdir -p {0}'.format(t.job_output_dir))
            #os.mkdir(t.job_output_dir) #this is not in JobManager because JobManager should be not care about these details

        ### Bulk add tags
        tasktags = []
        for t in tasks:
            for k,v in t.tags.items():
                tasktags.append(TaskTag(task=t,key=k,value=v))
        self.log.info("Bulk adding {0} TaskTags...".format(len(tasktags)))
        TaskTag.objects.bulk_create(tasktags,batch_size=100)

        ### Reset status of stages with new tasks
#        reset_stages_pks = set(map(lambda t: t.stage.pk, tasks))
#        Stage.objects.filter(id__in=reset_stages_pks).update(status="no_attempt",finished_on=None)

        return

    @transaction.commit_on_success
    def bulk_save_taskfiles(self,taskfiles):
        """
        :param taskfiles: (list) A list of taskfiles.
        """
        self.log.info("Bulk adding {0} TaskFiles...".format(len(taskfiles)))
        m = TaskFile.objects.all().aggregate(models.Max('id'))['id__max']
        id_start =  m + 1 if m else 1
        for i,t in enumerate(taskfiles):
            t.id = id_start + i
        try:
            TaskFile.objects.bulk_create(taskfiles,batch_size=100)
        except IntegrityError as e:
            return '{0}.  There are probably multiple tasks with the same output files'.format(e)

    @transaction.commit_on_success
    def bulk_save_task_edges(self,edges):
        """
        :param edges: [(parent, child),...] A list of tuples of parent -> child relationships
        """

        ### Bulk add parents
        task_edges = map(lambda e: TaskEdge(parent=e[0],child=e[1]),edges)
        self.log.info("Bulk adding {0} TaskEdges...".format(len(task_edges)))
        TaskEdge.objects.bulk_create(task_edges,batch_size=100)

    @transaction.commit_on_success
    def bulk_delete_tasks(self,tasks):
        """Bulk deletes tasks and their related objects"""
        task_output_dirs = map(lambda t: t.output_dir,tasks)

        self.log.info("Bulk deleting {0} tasks".format(len(tasks)))
        self.log.info('Bulk deleting JobAttempts...')
        JobAttempt.objects.filter(task__in = tasks).delete()
        self.log.info('Bulk deleting TaskTags...')
        TaskTag.objects.filter(task__in=tasks).delete()
        self.log.info('Bulk deleting TaskEdges...')
        TaskEdge.objects.filter(Q(parent=self)|Q(child=self)).delete()
        self.log.info('Bulk deleting TaskFiles...')
        TaskFile.objects.filter(task_output_set__in=tasks).delete()
        self.log.info('Bulk deleting Tasks...')
        tasks.delete()

        self.log.info('Deleting Task output directories')
        for d in task_output_dirs:
            os.system('rm -rf {0}'.format(d))

    #TODO this probably doesn't have to be a transaction
    @transaction.commit_on_success
    def delete(self, *args, **kwargs):
        """
        Deletes this workflow.
        """
        self.log.info("Deleting {0} and it's output dir {1}...".format(self,self.output_dir))
        save_str_representation = str(self)
        wf_output_dir = self.output_dir

        self.jobManager.delete()
        self.bulk_delete_tasks(self.tasks)
        self.log.info('Bulk Deleting Stages...'.format(self.name))
        self.stages.delete()

        super(Workflow, self).delete(*args, **kwargs)

        self.log.info('{0} Deleted.'.format(save_str_representation))
        x = list(self.log.handlers)
        for h in x:
            self.log.removeHandler(h)
            h.flush()
            h.close()

        if os.path.exists(wf_output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))

    def _run_task(self,task):
        """
        Creates and submits and JobAttempt.

        :param task: the task to submit a JobAttempt for
        """
        if (task.NOOP):
            return 'NOOP'

        #TODO fix this it's slow (do it in bulk when running a workflow?)
        if task.stage.status in ['no_attempt','failed']:
            if task.stage.status == 'no_attempt':
                task.stage.started_on = timezone.now()
            task.stage.set_status('in_progress')
            task.stage.save()
        task.set_status('in_progress')
        self.log.info('Running {0}'.format(task))

        task.exec_command = task.pcmd

        #set output_file paths to the task's job_output_dir
        for f in task.output_files:
            if not f.path:
                basename = '{0}.{1}'.format('out' if f.name == f.fmt else f.name,f.fmt) if not f.basename else f.basename
                f.path = os.path.join(task.job_output_dir,basename)
                f.save()
            if f.fmt == 'dir':
                mkdir_p(f.path)

        #Replace TaskFile hashes with their paths
        for m in re.findall('(#F\[(.+?):(.+?):(.+?)\])',task.exec_command):
            try:
                taskfile = TaskFile.objects.get(pk=m[1])
                task.exec_command = task.exec_command.replace(m[0],taskfile.path)
            except ValueError as e:
                raise ValueError('{0}.  Task is {1}. Taskfile str is {2}'.format(e,task,m[0]))
            except TypeError as e:
                raise TypeError("{0}. m[0] is {0} and taskfile is {1}".format(m[0],taskfile))

        jobAttempt = self.jobManager.add_jobAttempt(
            task=task,
            command=task.exec_command,
            jobName=""
        )

        task.jobattempt_set.add(jobAttempt)
        if self.dry_run:
            self.log.info('Dry Run: skipping submission of job {0}.'.format(jobAttempt))
        else:
            self.jobManager.submit_job(jobAttempt)
            self.log.info('Submitted jobAttempt with drmaa jobid {0}.'.format(jobAttempt.drmaa_jobID))
        task.save()
        return jobAttempt


    def _reattempt_task(self,task,failed_jobAttempt):
        """
        Reattempt running a task.

        :param task: (Task) the task to reattempt
        :param failed_jobAttempt: (bool) the previously failed jobAttempt of the task
        :returns: (bool) True if another jobAttempt was submitted, False if the max jobAttempts has already been reached.
        """
        numAttempts = task.jobAttempts.count()
        if not task.successful: #ReRun jobAttempt
            self.log.warning("{0} of {1} failed, on attempt # {2}, so deleting failed output files and retrying.\n".format(failed_jobAttempt,task,numAttempts)
                           + "<COMMAND path=\"{1}\">\n{0}\n</COMMAND>\n".format(failed_jobAttempt.get_command_shell_script_text(),failed_jobAttempt.command_script_path)
                           + "<STDERR>\n{0}\n</STDERR>".format(failed_jobAttempt.STDERR_txt)
            )
            if numAttempts < self.max_reattempts:
                os.system('rm -rf {0}/*'.format(task.job_output_dir))
                self._run_task(task)
                return True
            else:
                self.log.warning("{0} has failed and reached max_reattempts of {1}.\n<STDERR>\n{2}\n</STDERR>".format(self, self.max_reattempts,failed_jobAttempt.STDERR_txt))
                self.status = 'failed'
                self.save()
                return False


    def run(self,terminate_on_fail=True,finish=True):
        """
        Runs a workflow using the DAG of jobs

        :param terminate_on_fail: (bool) If True, the workflow will self terminate of any of the tasks of this stage fail `max_job_attempts` times
        """
        self.log.info("Generating DAG...")
        wfDAG = WorkflowManager(self)
        self.log.info("Running DAG.")
        def run_ready_tasks():
            submitted_tasks = wfDAG.run_ready_tasks()
            for st in submitted_tasks:
                if st.NOOP:
                    st._has_finished('NOOP')
                    wfDAG.complete_task(st)
            if submitted_tasks:
                run_ready_tasks()

        try:
            run_ready_tasks()

            for jobAttempt in self.jobManager.yield_all_queued_jobs():
                task = jobAttempt.task
                #self.log.info('Finished {0} for {1} of {2}'.format(jobAttempt,task,task.stage))
                if jobAttempt.successful or task.succeed_on_failure:
                    task._has_finished(jobAttempt)
                    wfDAG.complete_task(task)
                    run_ready_tasks()
                else:
                    if not self._reattempt_task(task,jobAttempt):
                        task._has_finished(jobAttempt) #job has failed and out of reattempts
                        if terminate_on_fail:
                            self.log.warning("{0} of {1} has reached max_reattempts and terminate_on_fail==True so terminating.".format(jobAttempt,task))
                            self.terminate()

            if finish:
                self.finished()
            return self

        except Exception as e:
            self.log.error('An exception was raised during workflow execution, terminating workflow and then re-raising exception.')
            self.terminate(exit=False)
            raise

    def finished(self):
        """
        Call at the end of every workflow.

        """
        self.finished_on = timezone.now()
        self.save()
        self.log.info("Finished {0}, last stage's output dir: {1}".format(self,
                                                                           self.stages.order_by('-order_in_workflow')[0].output_dir))

    def get_tasks_by(self,stage=None,tags={},op="and"):
        """
        Returns the list of tasks that are tagged by the keys and vals in tags dictionary

        :param op: (str) either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) tags to filter for
        :returns: (queryset) a queryset of the filtered tasks

        >>> task.get_tools_by(op='or',tags={'color':'grey','color':'orange'})
        >>> task.get_tools_by(op='and',tags={'color':'grey','shape':'square'})
        """

        if op == 'or':
            raise NotImplemented('sorry')

        if stage:
            tasks = stage.tasks
        else:
            tasks = self.tasks

        if tags == {}:
            return tasks
        else:
            for k,v in tags.items():
                tasks = tasks.filter(tasktag__key=k, tasktag__value=v)

            return tasks

    def get_task_by(self,tags={},stage=None,op="and"):
        """
        Returns the list of tasks that are tagged by the keys and vals in tags dictionary.

        :raises Exception: if more or less than one task is returned

        :param op: (str) Choose either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) A dictionary of tags you'd like to filter for
        :returns: (queryset) a queryset of the filtered tasks

        >>> task.get_task_by(op='or',tags={'color':'grey','color':'orange'})
        >>> task.get_task_by(op='and',tags={'color':'grey','color':'orange'})
        """

        tasks = self.get_tasks_by(stage=stage,op=op,tags=tags) #there's just one group of tasks with this tag combination
        n = tasks.count()
        if n>1:
            raise Exception("More than one task with tags {0} in {1}".format(tags,stage))
        elif n == 0:
            raise Exception("No tasks with with tags {0}.".format(tags))
        return tasks[0]

    def __str__(self):
        return 'Workflow[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
    
    def describe(self):
        return """output_dir: {0.output_dir}""".format(self)

    @models.permalink
    def url(self):
        return ('workflow_view',[str(self.id)])


class WorkflowManager():
    def __init__(self,workflow):
        self.workflow = workflow
        self.dag = self.createDiGraph()
        self.workflow.log.info('Using DAG to create Job Queue')
        self.dag_queue = self.dag.copy()
        self.dag_queue.remove_nodes_from(map(lambda x: x['id'],workflow.tasks.filter(successful=True).values('id')))
        self.queued_tasks = []

    def queue_task(self,task):
        self.queued_tasks.append(task.id)

    def run_ready_tasks(self):
        ready_tasks = [ n for n in self.get_ready_tasks() ]
        for n in ready_tasks:
            self.queue_task(n)
            self.workflow._run_task(n)
        return ready_tasks

    def complete_task(self,task):
        self.dag_queue.remove_node(task.id)
        self.dag.node[task.id]['status'] = task.status

        if task.status == 'successful' and self.workflow.delete_intermediates:
            # Input files may be ready for intermediate deleting if all
            # Tasks that depend on them are also successful, and they are not an input file (ie has >0 parents)
            for infile in task.input_files:
                if not infile.persist:
                    if all([ r['successful'] and r['_parents__count']>0
                              for r in infile.task_input_set.values('successful','id').annotate(models.Count('_parents')) ]
                    ):
                        infile.delete_because_intermediate()

        return self


    # def is_task_intermediate(self,task_id):
    #     """
    #     Checks to see if a task_id is an intermediary task.
    #     An intermediary task has at least 1 child and 1 parent, and all of its children are all successful.
    #     """
    #     successors = self.dag.successors(task_id)
    #     if len(self.dag.predecessors(task_id)) > 0 and len(successors) > 0:
    #         return all( self.dag.node[s]['status'] == 'successful' for s in successors )

    def get_ready_tasks(self):
        degree_0_tasks = map(lambda x:x[0],filter(lambda x: x[1] == 0,self.dag_queue.in_degree().items()))
        return Task.objects.filter(id__in=filter(lambda x: x not in self.queued_tasks,degree_0_tasks))
        #return map(lambda n_id: Task.objects.get(pk=n_id),filter(lambda x: x not in self.queued_tasks,degree_0_tasks)) 

    def createDiGraph(self):
        dag = nx.DiGraph()
        dag.add_edges_from([(ne['parent'],ne['child']) for ne in self.workflow.task_edges.values('parent','child')])
        for stage in self.workflow.stages:
            stage_name = stage.name
            for task in stage.tasks.all():
                dag.add_node(task.id,tags=task.tags,status=task.status,stage=stage_name,url=task.url())
        return dag

    def createAGraph(self):
        dag = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
        dag.node_attr['fontname']="Courier"
        dag.node_attr['fontsize']=8
        dag.add_edges_from(self.dag.edges())
        for stage,tasks in helpers.groupby(self.dag.nodes(data=True),lambda x:x[1]['stage']):
            sg = dag.add_subgraph(name="cluster_{0}".format(stage),label=str(stage),color='lightgrey')
            for n,attrs in tasks:
                def truncate_val(kv):
                    v = "{0}".format(kv[1])
                    v = v if len(v) <10 else v[1:8]+'..'
                    return "{0}: {1}".format(kv[0],v)
                label = " \\n".join(map(truncate_val,attrs['tags'].items()))
                status2color = { 'no_attempt':'black','in_progress':'gold1','successful': 'darkgreen','failed':'darkred'}
                sg.add_node(n,label=label,URL=attrs['url'].format(n),target="_blank",color=status2color[attrs['status']])

        return dag


    def as_img(self,format="svg"):
        g = self.createAGraph()
        #g = self.createAGraph(self.get_simple_dag())
        #g=nx.to_agraph(self.get_simple_dag())
        g.layout(prog="dot")
        return g.draw(format=format)

    def __str__(self):
        g = self.createAGraph()
        #g = self.createAGraph(self.get_simple_dag())
        #g=nx.to_agraph(self.get_simple_dag())
        return g.to_string()


class Stage(models.Model):
    """
    A group of jobs that can be run independently.  See `Embarassingly Parallel <http://en.wikipedia.org/wiki/Embarrassingly_parallel>`_ .
    
    .. note:: A Stage should not be directly instantiated, use :py:func:`Workflow.models.Workflow.add_stage` to create a new stage.
    """
    name        = models.CharField(max_length=200)

    workflow    = models.ForeignKey(Workflow)

    order_in_workflow = models.IntegerField(null=True)

#   status      = models.CharField(max_length=200,choices=status_choices,default='no_attempt')
    status      = models.TextField(default='no_attempt')

    successful  = models.BooleanField(default=False)

    started_on  = models.DateTimeField(null=True,default=None)
    created_on  = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)

    class Meta:
        unique_together = (('name','workflow'))

    def __init__(self,*args,**kwargs):
        kwargs['created_on'] = timezone.now()
        super(Stage,self).__init__(*args,**kwargs)

        validate_not_null(self.workflow)

        validate_name(self.name,self.name)
        #mkdir_p(self.output_dir)

    def set_status(self,new_status,save=True):
        "Set Stage status"
        self.log.info('{0} {1}'.format(self,new_status))
        self.status = new_status

        if new_status == 'successful':
            self.successful = True

        if save: self.save()

    @property
    def log(self):
        return self.workflow.log

    @property
    def percent_done(self):
        """
        Percent of tasks that have completed
        """
        done = Task.objects.filter(stage=self,successful=True).count()
        if self.num_tasks == 0 or done == 0:
            if self.status == 'in_progress' or self.status == 'failed':
                return 1
            return 0
        r = int(100 * float(done) / float(self.num_tasks))
        return r if r > 1 else 1

    def failed_jobAttempts(self):
        return JobAttempt.objects.filter(task__in=self.tasks,queue_status='finished',successful=False)


    def get_stats(self):
        """
        :param: a list of 3-tuples of format (title,field,statistic)
        :return: (dict) of stats about jobs
        """
        stats_to_get = [('avg_percent_cpu','percent_cpu','Avg',extras.format_percent),
                        ('avg_wall_time','wall_time','Avg',extras.format_time),
                        ('max_wall_time','wall_time','Max',extras.format_time),
                        ('avg_block_io_delays','block_io_delays','Avg',extras.format_time),
                        ('avg_rss_mem','avg_rss_mem','Avg',extras.format_memory_kb),
                        ('max_rss_mem','max_rss_mem','Max',extras.format_memory_kb),
                        ('avg_virtual_mem','avg_virtual_mem','Avg',extras.format_memory_kb)
        ]
        stat_names = [ s[0] for s in stats_to_get ]
        aggregate_kwargs = {}
        for title,field,statistic,formatfxn in stats_to_get:
            if statistic not in ['Avg','Sum','Max','Min','Count']:
                raise ValidationError('Statistic {0} not supported'.format(statistic))
            aggr_fxn = getattr(models, statistic)
            aggregate_kwargs[title]= aggr_fxn(field)
        r = self.successful_jobAttempts.aggregate(**aggregate_kwargs)
        d = OrderedDict()
        for title,field,stat,formatfxn in stats_to_get:
            d[title] = formatfxn(r[title])
        return d


    #TODO deprecated
    def get_sjob_stat(self,field,statistic):
        """
        Aggregates a task successful job's field using a statistic.
        :param field: (str) name of a tasks's field.  ex: wall_time or avg_rss_mem
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']
        
        >>> stage.get_stat('wall_time','Avg')
        120
        """

        if statistic not in ['Avg','Sum','Max','Min','Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field,statistic.lower())
        return self.successful_jobAttempts.aggregate(aggr_fxn(field))[aggr_field]

    @property
    def successful_jobAttempts(self):
        return JobAttempt.objects.filter(successful=True,task__in = Task.objects.filter(stage=self))

    def get_task_stat(self,field,statistic):
        """
        Aggregates a task's field using a statistic
        :param field: (str) name of a tasks's field.  ex: cpu_req, mem_req
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']
        
        >>> stage.get_stat('cpu_requirement','Avg')
        120
        """

        if statistic not in ['Avg','Sum','Max','Min','Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field,statistic.lower())
        r = Task.objects.filter(stage=self).aggregate(aggr_fxn(field))[aggr_field]
        return int(r) if r or r == 0.0 else r


    @property
    def file_size(self,human_readable=True):
        "Size of the stage's output_dir"
        return folder_size(self.output_dir,human_readable=human_readable)

    @property
    def wall_time(self):
        """Time between this stage's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on.replace(microsecond=0) - self.started_on.replace(microsecond=0) if self.finished_on else timezone.now().replace(microsecond=0) - self.started_on.replace(microsecond=0)

    @property
    def output_dir(self):
        "Absolute path to this stage's output_dir"
        return os.path.join(self.workflow.output_dir,self.name.replace(' ','_'))

    @property
    def tasks(self):
        "Queryset of this stage's tasks"
        return Task.objects.filter(stage=self)

    @property
    def task_edges(self):
        "Edges in this Stage"
        return TaskEdge.objects.filter(parent__in=self.tasks)

    @property
    def task_tags(self):
        "TaskTags in this Stage"
        return TaskTag.objects.filter(task__in=self.tasks)

    @property
    def task_files(self):
        "TaskFiles in this Stage"
        return TaskFile.objects.filter(task_output_set__in=self.tasks)

    @property
    def num_tasks(self):
        "The number of tasks in this stage"
        return Task.objects.filter(stage=self).count()

    @property
    def num_tasks_successful(self):
        "Number of successful tasks in this stage"
        return Task.objects.filter(stage=self,successful=True).count()

    def get_all_tag_keys_used(self):
        """Returns a set of all the keyword tags used on any task in this stage"""
        try:
            return self.tasks.all()[0].tags.keys()
        except IndexError:
            return {}
        except AttributeError:
            return set(map(lambda x: x['key'],TaskTag.objects.filter(task__in=self.tasks).values('key').distinct()))

    def yield_task_resource_usage(self):
        """
        :yields: (list of tuples) tuples contain resource usage and tags of all tasks.  The first element is the name, the second is the value.
        """
        #TODO rework with time fields
        for task in self.tasks:
            sja = task.get_successful_jobAttempt()
            if sja:
                yield [jru for jru in sja.resource_usage_short] + task.tags.items() #add in tags to resource usage tuples

    # def add_task(self, pcmd, tags={}, **kwargs):
    #     """
    #     Creates a new task for this stage, and saves it.
    #     If a task with `tags` already exists in this stage, just return it.
    #     Has the same signature as :meth:`Task.__init__` minus the stage argument.
    #
    #     :returns: The task added.
    #     """
    #     q = Task.objects.filter(stage=self,tags=tags)
    #     # if q.count() > 0:
    #     #     return q.all()[0]
    #
    #     return Task.create(stage=self,pcmd=pcmd,**kwargs)

    def is_done(self):
        """
        :returns: True if this stage is finished successfully or failed, else False
        """
        return self.status == 'successful' or self.status == 'failed'

    def _are_all_tasks_done(self):
        """
        :returns: True if all tasks have succeeded or failed in this stage, else False
        """
        return self.tasks.filter(Q(status = 'successful') | Q(status='failed')).count() == self.tasks.count()

    def _has_finished(self):
        """
        Executed when this stage has completed running.
        All it does is sets status as either failed or successful
        """
        num_tasks = Task.objects.filter(stage=self).count()
        num_tasks_successful = self.num_tasks_successful
        num_tasks_failed = Task.objects.filter(stage=self,status='failed').count()

        if num_tasks_successful == num_tasks:
            self.set_status('successful')
        elif num_tasks_failed + num_tasks_successful == num_tasks:
            self.set_status('failed')
        else:
            raise Exception('Stage._has_finished() called, but not all tasks are completed.')

        self.finished_on = timezone.now()
        self.save()
        signals.stage_status_change.send(sender=self, status=self.status)

    def get_tasks_by(self,tags={},op='and'):
        """
        An alias for :func:`Workflow.get_tasks_by` with stage=self
        
        :returns: a queryset of filtered tasks
        """
        return self.workflow.get_tasks_by(stage=self, tags=tags, op=op)

    def get_task_by(self,tags={},op='and'):
        """
        An alias for :func:`Workflow.get_task_by` with stage=self
        
        :returns: a queryset of filtered tasks
        """
        return self.workflow.get_task_by(stage=self, op=op, tags=tags)

    def group_tasks_by(self,keys=[]):
        """
        Yields tasks, grouped by tags in keys.  Groups will be every unique set of possible values of tags.
        For example, if you had tasks tagged by color, and shape, and you ran func:`stage.group_tasks_by`(['color','shape']),
        this function would yield the group of tasks that exist in the various combinations of `colors` and `shapes`.
        So for example one of the yields might be (({'color':'orange'n'shape':'circle'}), [ orange_circular_tasks ])
        
        :param keys: The keys of the tags you want to group by.
        :yields: (a dictionary of this group's unique tags, tasks in this group).
        
        .. note:: a missing tag is considered as None and thus placed into a 'None' group with other untagged tasks.  You should generally try to avoid this scenario and have all tasks tagged by the keywords you're grouping by.
        """
        if keys == []:
            yield {},self.tasks
        else:
            task_tag_values = TaskTag.objects.filter(task__in=self.tasks, key__in=keys).values() #get this stage's tags
            #filter out any tasks without all keys

            task_id2tags = {}
            for task_id, ntv in helpers.groupby(task_tag_values,lambda x: x['task_id']):
                task_tags = dict([ (n['key'],n['value']) for n in ntv ])
                task_id2tags[task_id] = task_tags

            for tags,task_id_and_tags_tuple in helpers.groupby(task_id2tags.items(),lambda x: x[1]):
                task_ids = [ x[0] for x in task_id_and_tags_tuple ]
                yield tags, Task.objects.filter(pk__in=task_ids)

    @transaction.commit_on_success
    def delete(self, *args, **kwargs):
        """
        Bulk deletes this stage and all files associated with it.
        """
        self.log.info('Deleting Stage {0}.'.format(self.name))
        if os.path.exists(self.output_dir):
            self.log.info('Deleting directory {0}...'.format(self.output_dir))
            os.system('rm -rf {0}'.format(self.output_dir))
        self.workflow.bulk_delete_tasks(self.tasks)
        super(Stage, self).delete(*args, **kwargs)
        self.log.info('{0} Deleted.'.format(self))

    @models.permalink
    def url(self):
        "The URL of this stage"
        return ('stage_view',[str(self.workflow.id),self.name])

    def __str__(self):
        return 'Stage[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))


class TaskTag(models.Model):
    """
    A SQL row that duplicates the information of Task.tags that can be used for filtering, etc.
    """
    task  = models.ForeignKey('Task')
    key   = models.CharField(max_length=255) # was 63 why?
    value = models.TextField() # was CharField

    def __str__(self):
        return "<TaskTag[self.id] {self.key}: {self.value} for Task[{task.id}]>".format(self=self,task=self.task)

class TaskEdge(models.Model):
    parent = models.ForeignKey('Task', related_name='parent_edge_set')
    child  = models.ForeignKey('Task', related_name='child_edge_set')
#    tags  = PickledObjectField(null=True,default={})
    "The keys associated with the relationship.  ex, the group_by parameter of a many2one"

    def __str__(self):
        return "Edge {0.parent}->{0.child}".format(self)

class Task(models.Model):
    """
    The object that represents the command line that gets executed.
    
    tags must be unique for all tasks in the same stage
    """
    stage              = models.ForeignKey(Stage,help_text="The stage this task belongs to.")

    pcmd               = models.TextField(help_text='Preformatted command.  almost always will contain special strings for TaskFiles which will later be replaced by their proper system path at execution')
    exec_command       = models.TextField(help_text='The actual command that is executed',null=True)

    memory_requirement = models.IntegerField(help_text="Memory to reserve for jobs in MB",default=None,null=True)
    cpu_requirement    = models.SmallIntegerField(help_text="Number of CPUs to reserve for this job",default=None,null=True)
    time_requirement   = models.IntegerField(help_text="Time required to run in minutes.  If a job runs longer it may be automatically killed.",default=None,null=True)

    successful         = models.BooleanField(default=False,help_text="True if the task has been executed successfully, else False")

    status             = models.CharField(max_length=100,choices = status_choices,default='no_attempt')
#   status_details     = models.CharField(max_length=100,default='',help_text='Extra information about this task\'s status')
    status_details     = models.TextField(default='',help_text='Extra information about this task\'s status')

    NOOP               = models.BooleanField(default=False,help_text="No operation.  Likely used to store an input file, this task is not meant to be executed.")
    succeed_on_failure = models.BooleanField(default=False, help_text="If True, Task will succeed and workflow will progress even if its JobAttempts fail.")

    # cleared_output_files     = models.BooleanField(default=False,help_text="If True, output files have been deleted/cleared.")
    # dont_delete_output_files = models.BooleanField(default=False,help_text="If True, prevents output files from being deleted even when this task becomes an intermediate and workflow.delete_intermediates == True.")

    _parents = models.ManyToManyField('Task',related_name='_children')

    tags        = PickledObjectField(null=False,default={})
    #on_success = PickledObjectField(null=False)

    created_on  = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)

    _output_files = models.ManyToManyField(TaskFile,related_name='task_output_set',null=True,default=None) #dictionary of outputs

    @property
    def output_files(self): return self._output_files.all()

    _input_files = models.ManyToManyField(TaskFile,related_name='task_input_set',null=True,default=None)
    @property
    def input_files(self): return self._input_files.all()

#TODO a fix for the below should be implemented, or at least a validation that tags and stage are in fact unique
#   Django has a bug that prevents indexing of BLOBs which is what tags is stored as
#    class Meta:
#        unique_together = (('tags','stage'))

    def __init__(self, *args, **kwargs):
        """
        :param stage: (Stage) The stage this task is a part of.
        :param pcmd: (str) The preformatted command to execute. Usually includes strings that represent TaskFiles which will be automatically parsed. Required.
        :param tags: (dict) A dictionary keys and values to tag the task with. These tags can later be used by methods such as :py:meth:`~Workflow.models.stage.group_tasks_by` and :py:meth:`~Workflow.models.stage.get_tasks_by` Optional.
        :param on_success: (method) A method to run when this task succeeds.  Method is called with one parameter named 'task', the successful task.
        :param memory_requirement: (int) How much memory to reserve for this task in MB. Optional.
        :param cpu_requirement: (int) How many CPUs to reserve for this task. Optional.
        :param time_requirement: (int) Time required in miinutes.  If a job exceeds this requirement, it will likely be killed.
        :param NOOP: (booean) No Operation, this task does not get executed.
        :param succeed_on_failure: (booean) Succeed even if JobAttempts fails.
        :param dont_delete_output_files: (boolean) Prevents output files from being deleted, even when this task becomes an intermediate.
        :param hard_reset: (bool) Deletes this task and all associated files and start it fresh. Optional.
        :returns: A new task instance.  The instance has not been saved to the database.
        """
        kwargs['created_on'] = timezone.now()
        super(Task,self).__init__(*args, **kwargs)

        # if len(self.tags) == 0:
        #     raise TaskValidationError, '{0} has no tags, at least one tag is required'.format(self)

    @staticmethod
    def create(stage,pcmd,**kwargs):
        """
        Creates a task.
        """
        task = Task(stage=stage, pcmd=pcmd, **kwargs)

        if Task.objects.filter(stage=task.stage,tags=task.tags).count() > 0:
            task.delete()
            raise ValidationError("Tasks belonging to a stage with the same tags detected! tags: {0}".format(task.tags))

        task.save()

        mkdir_p(task.output_dir)
        mkdir_p(task.job_output_dir)

        #Create task tags    
        for key,value in task.tags.items():
            TaskTag.objects.create(task=task,key=key,value=value)

        return task

    @property
    def workflow(self):
        "This task's workflow"
        return self.stage.workflow

    @property
    def parents(self):
        "This task's parents"
        #return map(lambda n: n.parent, TaskEdge.objects.filter(child=self).all())
        return self._parents.all()

    @property
    def task_edges(self):
        return TaskEdge.objects.filter(Q(parent=self)|Q(child=self))

    @property
    def task_tags(self):
        return TaskTag.objects.filter(task=self)

    @property
    def log(self):
        "This task's workflow's log"
        return self.workflow.log

    @property
    def file_size(self,human_readable=True):
        "Task filesize"
        return folder_size(self.output_dir,human_readable=human_readable)

    @property
    def output_file_size(self,human_readable=True):
        "Task filesize"
        return folder_size(self.job_output_dir,human_readable=human_readable)

    @property
    def output_dir(self):
        "Task output dir"
        return os.path.join(self.stage.output_dir,str(self.id))

    @property
    def job_output_dir(self):
        """Where the job output goes"""
        return os.path.join(self.output_dir,'out')

    # @property
    # def output_paths(self):
    #     "Dict of this task's outputs appended to this task's output_dir."
    #     r = {}
    #     for key,val in self.outputs.items():
    #         r[key] = os.path.join(self.job_output_dir,val)
    #     return r

    @property
    def jobAttempts(self):
        "Queryset of this task's jobAttempts."
        return self.jobattempt_set.all().order_by('id')

    @property
    def wall_time(self):
        "Task's wall_time"
        return self.get_successful_jobAttempt().wall_time if self.successful else None

    def numAttempts(self):
        "This task's number of job attempts."
        return self.jobattempt_set.count()

    def get_successful_jobAttempt(self):
        """
        Get this task's successful job attempt.
        
        :return: this task's successful job attempt.  If there were no successful job attempts, returns None
        """
        jobs = self.jobattempt_set.filter(successful=True)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) > 1:
            raise Exception('more than 1 successful job, something went wrong!')
        else:
            return None # no successful jobs

    def set_status(self,new_status,save=True):
        "Set Task's status"
        self.status = new_status

        if new_status == 'successful':
            self.successful = True
            self.log.info('{0} successful!'.format(self))

        if save: self.save()

    def _has_finished(self,jobAttempt):
        """
        Should be executed whenever this task finishes.
        
        Sets self.status to 'successful' or 'failed' and self.finished_on to 'current_timezone'
        Will also run self.stage._has_finished() if all tasks in the stage are done.
        """

        if (
            jobAttempt == 'NOOP'
            or jobAttempt.task.succeed_on_failure
            or self.jobattempt_set.filter(successful=True).count()
        ):
            self.set_status('successful')
        else:
            self.set_status('failed')

        self.finished_on = timezone.now()
        signals.task_status_change.send(sender=self, status=self.status)
        if self.stage._are_all_tasks_done(): self.stage._has_finished()

    def tag(self,**kwargs):
        """
        Tag this task with key value pairs.  If the key already exists, its value will be overwritten.
        
        >>> task.tag(color="blue",shape="circle")
        """
        #TODO don't allow tags called things like 'status' or other task attributes
        for key,value in kwargs.items():
            value = str(value)
            tasktag, created = TaskTag.objects.get_or_create(task=self,key=key,defaults= {'value':value})
            if not created:
                tasktag.value = value
            tasktag.save()
            self.tags[key] = value

    def clear_job_output_dir(self):
        """
        Removes all files in this task's output directory
        """
        for otf in self.output_files:
            if not otf.persist and not otf.deleted_because_intermediate:
                otf.delete_because_intermediate()

    @models.permalink
    def url(self):
        "This task's url."
        return ('task_view',[str(self.workflow.id),self.stage.name,self.tags_as_query_string()])

    def tags_as_query_string(self):
        """
        Returns a string of tag keys and values as a url query string
        """
        import urllib
        return urllib.urlencode(self.tags)

    @transaction.commit_on_success
    def delete(self, *args, **kwargs):
        """
        Deletes this task and all files associated with it
        """
        self.log.info('Deleting {0} and it\'s output directory {1}'.format(self,self.output_dir))
        #todo delete stuff in output_paths that may be extra files
        for ja in self.jobattempt_set.all(): ja.delete()
        self.task_tags.delete()
        self.task_edges.delete()
        self.output_files.delete()
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
        super(Task, self).delete(*args, **kwargs)

    def __str__(self):
        return '<Task[{1}] {0} {2}>'.format(self.stage,self.id,self.tags)

