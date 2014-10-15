import django

if (django.VERSION >= (1,5)): 
    from django.conf.urls          import patterns, include, url     
else:
    from django.conf.urls.defaults import patterns, include, url          # 'defaults' deprecated in Django 1.5

from django.contrib.staticfiles.urls import staticfiles_urlpatterns

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'Cosmos.views.home', name='home'),
    # url(r'^Cosmos/', include('Cosmos.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),

    url(r'Job/JobAttempt/(\d+)/$', 'cosmos.Job.views.jobAttempt',name='jobAttempt_view'),
#   url(r'JobManager/JobAttempt/(\d+)/output/(.+)$', 'JobManager.views.jobAttempt_output',name='jobAttempt_output'),

    url(r'Workflow/TaskFile/(\d+)/$', 'cosmos.Workflow.views.taskfile_view',name='taskfile_view'),
    url(r'Workflow/TaskFile/(\d+)/profile_output/$', 'cosmos.Job.views.jobAttempt_profile_output',name='jobAttempt_profile_output'),

    url(r'GridEngine/$', 'cosmos.utils.views.GridEngine',name='GridEngine'),
    url(r'LSF/$',        'cosmos.utils.views.LSF',       name='lsf'),

    url(r'Workflow/$',                        'cosmos.Workflow.views.index',               name='workflow'),
    url(r'Workflow/(\d+)/$',                  'cosmos.Workflow.views.view',                name='workflow_view'),
    url(r'Workflow/(\d+)/stage_table/$',      'cosmos.Workflow.views.workflow_stage_table',name='workflow_stage_table'),
    url(r'Workflow/(\d+)/view_log/$',         'cosmos.Workflow.views.view_log',            name='workflow_view_log'),
    url(r'Workflow/(\d+)/analysis/$',         'cosmos.Workflow.views.analysis',            name='workflow_analysis'),
    url(r'Workflow/(\d+)/visualize/$',        'cosmos.Workflow.views.visualize',           name='workflow_visualize'),
    url(r'Workflow/(\d+)/visualize/as_img/$', 'cosmos.Workflow.views.visualize_as_img',    name='visualize_as_img'),

    url(r'Workflow/Stage/(\d+)/table/$',            'cosmos.Workflow.views.stage_table',     name='stage_table'),
    url(r'Workflow/Stage/(\d+)/stage_task_table/$', 'cosmos.Workflow.views.stage_task_table',name='stage_task_table'),

    url(r'Workflow/(?P<wf_id>\d+)/Stage/(?P<stage_name>[\w-]+?)/$',                      'cosmos.Workflow.views.stage_view',name='stage_view'),
    url(r'Workflow/(?P<wf_id>\d+)/Stage/(?P<stage_name>[\w-]+?)/Task/(?P<tags_qs>.*)/$', "cosmos.Workflow.views.task_view", name='task_view'),

    url(r'^$', 'cosmos.utils.views.index',name='home'),
)

from django.conf import settings
urlpatterns += staticfiles_urlpatterns()

urlpatterns += patterns('',url(r'^media/(?P<path>.*)$', 'django.views.static.serve', {'document_root': settings.MEDIA_ROOT,}),)
