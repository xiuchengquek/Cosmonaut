from django.shortcuts import render_to_response,render
from django.template import RequestContext
from django.http import HttpResponse
from cosmos.Workflow.models import Workflow, Stage, Task, TaskTag, WorkflowManager, TaskFile
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from cosmos.utils.helpers import groupby
from models import status_choices
from django.utils.datastructures import SortedDict
import os
from django.views.decorators.cache import never_cache
from django.utils.safestring import mark_safe
import math

@never_cache
def _get_stages_dict(workflow):
    #stages_dict = Stage.objects.get(workflow=workflow).values('successful',')
    pass

@never_cache
def index(request):
    workflows = Workflow.objects.all().order_by('-created_on')
    return render_to_response('Workflow/index.html', { 'request':request,'workflows': workflows }, context_instance=RequestContext(request))

@never_cache
def view(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    stages_ordered = Stage.objects.filter(workflow=workflow).order_by('order_in_workflow')
    return render_to_response('Workflow/view.html', { 'request':request,'workflow': workflow, 'stages_ordered':stages_ordered }, context_instance=RequestContext(request))

@never_cache
def workflow_stage_table(request,pid):
    """Summary table of a workflow's stages"""
    workflow = Workflow.objects.get(pk=pid)
    stages = Stage.objects.filter(workflow=workflow).order_by('order_in_workflow')
    return render_to_response('Workflow/Stage/table.html', { 'request':request, 'stages': stages }, context_instance=RequestContext(request))

@never_cache
def stage_table(request,pid):
    """Summary table of a stage"""
    stages = [Stage.objects.get(pk=pid)]
    return render_to_response('Workflow/Stage/table.html', { 'request':request, 'stages': stages }, context_instance=RequestContext(request))

def __get_filter_choices(stage):
    """
    :returns: { 'key' : [possible values] } for tags for a stage
    """
    #generate possible filter choices
    tasktags = TaskTag.objects.filter(task__stage=stage).values('key','value')
    filter_choices = SortedDict({ 'f_status': [ x[0] for x in status_choices ] }) #init with status filter
    for key,nts in groupby(tasktags,lambda x: x['key']):
        filter_choices[key] = sorted(set([ nt['value'] for nt in nts ])) #add each task_tag.key and all the unique task_tag.values
    return filter_choices

def __get_context_for_stage_task_table(request,pid):
    """Summary table of tasks.
    :param pid: the pid of the stage
    """
    stage = Stage.objects.get(pk=pid)
    #filtering
    filter_choices = __get_filter_choices(stage)
    #filter!
    all_filters = {}
    filter_url=''
    if 'filter' in request.GET: #user wanted a filter
        all_filters = dict([ (k,request.GET[k]) for k in filter_choices.keys()])
        tag_filters = all_filters.copy()
        for k,v in tag_filters.items():
            if v=='' or v==None or k =='f_status': del tag_filters[k] #filter tag_filters
        tasks_list = stage.get_tasks_by(tag_filters)
        fs = request.GET.get('f_status')
        if fs != None and fs != '':
            tasks_list = tasks_list.filter(status=request.GET['f_status'])
        
        filter_url = 'filter=True&'+'&'.join([ '{0}={1}'.format(k,v) for k,v in all_filters.items() ]) #url to retain this filter
        mark_safe(filter_url)
    else:
        tasks_list = Task.objects.filter(stage=stage).select_related()

    #pagination
    page_size = 20
    paginator = Paginator(tasks_list, page_size) # Show 25 contacts per page
    page = request.GET.get('page')
    if page is None: page = 1
    try:
        tasks = paginator.page(page)
    except PageNotAnInteger:
        # If page is not an integer, deliver first page.
        tasks = paginator.page(1)
    except EmptyPage:
        # If page is out of range (e.g. 9999), deliver last page of results.
        tasks = paginator.page(paginator.num_pages)
    page_slice = "{0}:{1}".format(page,int(page)+19)
    
    
    return { 'request':request,'stage': stage,'page_size':page_size,'paged_tasks':tasks, 'page_slice':page_slice, 'current_filters':all_filters, 'filter_url':filter_url }

@never_cache
def stage_task_table(request,pid):
    context = __get_context_for_stage_task_table(request,pid)
    return render(request,'Workflow/Task/table.html', context)

@never_cache
def stage_view(request,wf_id,stage_name):
    stage = Stage.objects.get(workflow=wf_id,name=stage_name)
    filter_choices = __get_filter_choices(stage)
    task_table_context = __get_context_for_stage_task_table(request,stage.id)
    this_context = {'request':request, 'stage': stage, 'filter_choices':filter_choices}
    for k,v in task_table_context.items(): this_context[k] = v 
    
    return render_to_response('Workflow/Stage/view.html', this_context, context_instance=RequestContext(request))

@never_cache
def task_view(request,wf_id,stage_name,tags_qs):
    import urlparse
    stage = Stage.objects.get(workflow=wf_id,name=stage_name)
    task = stage.get_task_by(tags=dict(urlparse.parse_qsl(tags_qs)))
    jobAttempts_list = task.jobattempt_set.all()
    return render_to_response('Workflow/Task/view.html', { 'request':request,'task': task, 'jobAttempts_list':jobAttempts_list }, context_instance=RequestContext(request))


@never_cache
def taskfile_view(request,tfid):
    tf = TaskFile.objects.get(pk=tfid)
    if tf.fmt == 'dir':
        import subprocess
        proc = subprocess.Popen(["ls", "-lh", "{0}".format(tf.path)], stdout=subprocess.PIPE)
        output, err = proc.communicate()
        if output.strip() == '': output = 'Empty Directory'

    else:
        if os.path.exists(tf.path):
            output = file(tf.path,'rb').read(int(math.pow(2,10)*100)) #read at most 100kb
        else:
            output = 'File does not exist'
    return render_to_response('Workflow/TaskFile/view.html', { 'request':request,'taskfile': tf, 'output':output, 'jobAttempt':None }, context_instance=RequestContext(request))


@never_cache
def view_log(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    return render_to_response('Workflow/view_log.html', { 'request':request,'workflow': workflow }, context_instance=RequestContext(request))

def visualize(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    return render_to_response('Workflow/visualize.html', { 'request':request,'workflowDAG': WorkflowManager(workflow) }, context_instance=RequestContext(request))

def visualize_as_img(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    image_data = WorkflowManager(workflow).as_img()
    return HttpResponse(image_data, mimetype="image/svg+xml")


@never_cache
def analysis(request,pid):
    # from django.conf import settings as django_settings
    # from cosmos import config as cosmos_settings
    #
    # wf = Workflow.objects.get(pk=pid)
    #
    # resultsDir = 'Workflow/plots'
    # resultsFile = "{0}.png".format(wf.id)
    # resultsFile_path = os.path.join(django_settings.MEDIA_ROOT,resultsDir,resultsFile)
    # plot_url = os.path.join(django_settings.MEDIA_URL,resultsDir,resultsFile)
    # plot_path = os.path.join(django_settings.MEDIA_ROOT,resultsDir,resultsFile)
    #
    # workflow = Workflow.objects.get(pk=pid)
    # ru_path = os.path.join(django_settings.MEDIA_ROOT,resultsDir,'resource_usage.csv')
    # ru_url = os.path.join(django_settings.MEDIA_URL,resultsDir,'resource_usage.csv')
    # plot_rscript_path = os.path.join(cosmos_settings.home_path,'Cosmos/profile/plot.R')
    # workflow.save_resource_usage_as_csv(ru_path)
    # cmd = 'Rscript {0} {1} {2}'.format(plot_rscript_path,ru_path, plot_path)
    # os.system(cmd)
    plot_url=''
    ru_url=''
    return render_to_response('Workflow/analysis.html', { 'request':request,'plot_url':plot_url,'resource_usage_url':ru_url}, context_instance=RequestContext(request))



