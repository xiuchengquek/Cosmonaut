from cosmos.utils.helpers import groupby
import itertools as it
import networkx as nx
import pygraphviz as pgv
from cosmos.Workflow.models import Task,TaskError,Stage
from decorator import decorator
from tool import Tool
import itertools
import re
from cosmos.Workflow.models import TaskFile

class DAGError(Exception): pass
class StageNameCollision(Exception):pass
class FlowFxnValidationError(Exception):pass


@decorator
def flowfxn(func,dag,*RHS):
    """
* The decorated function should return a generator, so evaluate it
* Set the dag.active_tools to the decorated function's return value, if the function was not `sequence_`, which handles
this automatically
* Return the dag
"""

    if type(dag) != DAG: raise TypeError, 'The left hand side should be of type dag.DAG'
    dag.active_tools = list(func(dag,*RHS))

    try:
        stage_name = dag.active_tools[0].stage_name
    except IndexError:
        raise DAGError,'Tried to DAG.{0}(), but dag.active_tools is not set. Make sure to `add_` some INPUTs first.'.format(
            func.__name__
        )

    if not dag.ignore_stage_name_collisions and stage_name in dag.stage_names_used:
        raise StageNameCollision, 'Duplicate stage_names detected {0}. If you want to have flowfxns add tools to'.format(stage_name) + \
                                  'existing stages, set dag.ignore_stage_name_collusions=True.'

    if stage_name not in dag.stage_names_used:
        dag.stage_names_used.append(stage_name)

    return dag


class DAG(object):
    """
A Representation of a workflow as a :term:`DAG` of jobs.
"""
    
    def __init__(self,cpu_req_override=False,ignore_stage_name_collisions=False,mem_req_factor=1):
        """
:param cpu_req_override: set to an integer to override all task cpu_requirements. Useful when a :term:`DRM` does not support requesting multiple cpus
:param mem_req_factor: multiply all task mem_reqs by this number.
:param dag.ignore_stage_name_collisions: Allows the flowfxns to add to stages that already exists.
"""
        self.G = nx.DiGraph()
        self.active_tools = []
        self.cpu_req_override = cpu_req_override
        self.mem_req_factor = mem_req_factor
        self.stage_names_used = []
        self.ignore_stage_name_collisions = ignore_stage_name_collisions

    def get_tools_by(self,stage_names=[],tags={}):
        """
:param stage_names: (str) Only returns tasks belonging to stages in stage_names
:param tags: (dict) The criteria used to decide which tasks to return.
:return: (list) A list of tasks
"""
        for stage_name in stage_names:
            if stage_name not in self.stage_names_used:
                raise KeyError, 'Stage name "{0}" does not exist.'.format(stage_name)

        def dict_intersection_is_equal(d1,d2):
            for k,v in d2.items():
                try:
                    if d1[k] != v:
                        return False
                except KeyError:
                    pass
            return True

        tasks = [ task for task in self.G.nodes()
                  if (task.stage_name in stage_names)
            and dict_intersection_is_equal(task.tags,tags)
        ]
        return tasks

    def add_edge(self,parent,child):
        """
Adds a dependency
:param parent: (Tool) a parent
:param child: (Tool) a child
:return: (DAG) self
"""
        self.G.add_edge(parent,child)
        return self

    def branch_from_tools(self,tools):
        """
Branches from a list of tools
:param tools: (list) a list of tools
:return: (DAG) self
"""
        self.active_tools = tools
        return self

    def branch_(self,stage_names=[],tags={}):
        """
Updates active_tools to be the tools in the stages with name stage_name.
The next infix operation will thus be applied to `stage_name`.
This way the infix operations an be applied to multiple stages if the workflow isn't "linear".

:param stage_names: (str) Only returns tasks belonging to stages in stage_names
:param tags: (dict) The criteria used to decide which tasks to return.
:return: (list) A list of tasks
"""
        self.active_tools = self.get_tools_by(stage_names=stage_names,tags=tags)
        return self
        
    def create_dag_img(self,path):
        """
Writes the :term:`DAG` as an image.
gat
:param path: the path to write to
"""
        dag = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
        dag.node_attr['fontname']="Courier"
        dag.node_attr['fontsize']=8
        dag.add_edges_from(self.G.edges())
        for stage,tasks in groupby(self.G.nodes(),lambda x:x.stage_name):
            sg = dag.add_subgraph(name="cluster_{0}".format(stage),label=stage,color='lightgrey')
            for task in tasks:
                sg.add_node(task,label=task.label)
        
        dag.layout(prog="dot")
        dag.draw(path,format='svg')
        print 'wrote to {0}'.format(path)

    def configure(self,settings={},parameters={}):
        """
Sets the parameters an settings of every tool in the dag.

:param parameters: (dict) {'stage_name': { 'name':'value', ... }, {'stage_name2': { 'key':'value', ... } }
:param settings: (dict) { 'key':'val'} }
"""
        self.parameters = parameters
        for tool in self.G.node:
            tool.settings = settings
            if tool.stage_name not in self.parameters:
                #set defaults, then override with parameters
                self.parameters[tool.stage_name] = tool.default_params.copy()
                self.parameters[tool.stage_name].update(parameters.get(tool.__class__.__name__,{}))
                self.parameters[tool.stage_name].update(parameters.get(tool.stage_name,{}))
            tool.parameters = self.parameters.get(tool.stage_name,{})
        return self
            
    def add_to_workflow(self,workflow):
        """
Add this dag to a workflow. Only adds tools to stages that are new, that is, another tag in the same
stage with the same tags does not already exist.

:param workflow: the workflow to add
"""
        workflow.log.info('Adding tasks to workflow.')
        
        #Validation
        taskfiles = list(it.chain(*[ n.output_files for n in self.G.nodes() ]))
        #check paths
        #TODO this code is really weird.
        v = map(lambda tf: tf.path,taskfiles)
        v = filter(lambda x:x,v)
        if len(map(lambda t: t,v)) != len(map(lambda t: t,set(v))):
            import pprint
            raise DAGError('Multiple taskfiles refer to the same path. Paths should be unique. taskfile.paths are:{0}'.format(pprint.pformat(sorted(v))))

        #Add stages, and set the tool.stage reference for all tools
        stages = {}
        # for tool in nx.topological_sort(self.G):
        # stage_name = tool.stage_name
        # if stage_name not in stages: #have not seen this stage yet
        # stages[stage_name] = workflow.add_stage(stage_name)
        # tool.stage = stages[stage_name]

        # Load stages or add if they don't exist
        for stage_name in self.stage_names_used:
            stages[stage_name] = workflow.add_stage(stage_name)

        # Set tool.stage
        for tool in self.G.nodes():
            tool.stage = stages[tool.stage_name]

        #update tool._task_instance and tool.output_files with existing data
        stasks = list(workflow.tasks.select_related('_output_files','stage'))
        for tpl, group in groupby(stasks + self.G.nodes(), lambda x: (x.tags,x.stage.name)):
            group = list(group)
            if len(group) >1:
                tags = tpl[0]
                stage_name = tpl[1]
                tool = group[0] if isinstance(group[1],Task) else group[1]
                task = group[0] if isinstance(group[0],Task) else group[1]
                tool.output_files = task.output_files
                tool._task_instance = task
        
        #bulk save tasks
        new_nodes = filter(lambda n: not hasattr(n,'_task_instance'), nx.topological_sort(self.G))
        workflow.log.info('Total tasks: {0}, New tasks being added: {1}'.format(len(self.G.nodes()),len(new_nodes)))
        
        #bulk save task_files. All inputs have to at some point be an output, so just bulk save the outputs.
        #Must come before adding tasks, since taskfile.ids must be populated to compute the proper pcmd.
        taskfiles = list(it.chain(*[ n.output_files for n in new_nodes ]))
        workflow.bulk_save_taskfiles(taskfiles)
        
        #bulk save tasks
        for node in new_nodes:
                node._task_instance = self.__new_task(node.stage,node)
        tasks = [ node._task_instance for node in new_nodes ]
        workflow.bulk_save_tasks(tasks)
        
        ### Bulk add task->output_taskfile relationships
        ThroughModel = Task._output_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=tf.id) for n in new_nodes for tf in n.output_files ]
        ThroughModel.objects.bulk_create(rels)

        ### Bulk add task->input_taskfile relationships
        ThroughModel = Task._input_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=tf.id) for n in new_nodes for tf in n.input_files ]
        ThroughModel.objects.bulk_create(rels)


        ### Bulk add task->parent_task relationships
        ThroughModel = Task._parents.through
        new_edges = filter(lambda e: e[0] in new_nodes or e[1] in new_nodes,self.G.edges())
        rels = [ ThroughModel(from_task_id=child._task_instance.id,
                              to_task_id=parent._task_instance.id)
                 for parent,child in new_edges ]
        ThroughModel.objects.bulk_create(rels)


        #bulk save edges
        new_edges = filter(lambda e: e[0] in new_nodes or e[1] in new_nodes,self.G.edges())
        task_edges = [ (parent._task_instance,child._task_instance) for parent,child in new_edges ]
        workflow.bulk_save_task_edges(task_edges)

    def add_run(self,workflow,finish=True):
        """
Shortcut to add to workflow and then run the workflow
:param workflow: the workflow this dag will be added to
:param finish: pass to workflow.run()
"""
        self.add_to_workflow(workflow)
        workflow.run(finish=finish)


    def __new_task(self,stage,tool):
        """
Instantiates a task from a tool. Assumes TaskFiles already have real primary keys.

:param stage: The stage the task should belong to.
:param tool: The Tool.
"""
        pcmd = tool.pcmd
        # for m in re.findall('(#F\[(.+?):(.+?):(.+?)\])',pcmd):
        # if m[1] not in [t.id for t in tool.output_files]:
        # tool.input_files.append(TaskFile.objects.get(pk=m[1]))

        try:
            return Task(
                      stage = stage,
                      pcmd = pcmd,
                      tags = tool.tags,
                      # input_files = tool.input_files,
                      # output_files = tool.output_files,
                      memory_requirement = tool.mem_req * self.mem_req_factor,
                      cpu_requirement = tool.cpu_req if not self.cpu_req_override else self.cpu_req_override,
                      time_requirement = tool.time_req,
                      NOOP = tool.NOOP,
                      succeed_on_failure = tool.succeed_on_failure)
        except TaskError as e:
            raise TaskError('{0}. Task is {1}.'.format(e,tool))

    @flowfxn
    def add_(self,tools,stage_name=None,tag={}):
        """
Always the first flowfxn used to describe a DAG. Simply adds a list of tool instances to the dag,
without adding any dependencies.

.. note::

This operator is different than the others in that its input is a list of
instantiated instances of Tools, rather than a Tool class.

:param tools: (list) Tool instances.
:param stage_name: (str) The name of the stage to add to. Defaults to the name of the tool class.
:param tag: (dict) A dictionary of tags to add to the tools produced by this flowfxn.
:return: (DAG) self.

>>> DAG().add_([tool1,tool2,tool3,tool4])
"""
        if not isinstance(tools,list):
            raise FlowFxnValidationError, 'The parameter `tools` must be a list'
        if len(tools) == 0:
            raise FlowFxnValidationError,'The parameter `tools` must have at least one Tool in it'
        for t in tools:
            if len(t.tags) == 0:
                raise FlowFxnValidationError, '{0} has no tags, at least one tag is required'.format(t)
        if len(tools) > 0:
            if not isinstance(tools[0],Tool):
                raise FlowFxnValidationError, '`tools` must be a list of Tools'
        if stage_name is None:
            stage_name = tools[0].stage_name
        if len(set([ tuple(t.tags.items()) for t in tools])) < len(tools):
            raise FlowFxnValidationError, 'Duplicate tags detected when trying to add tools to stage "{0}". Tags within a stage must be unique. Duplicate Tags are {1}'.format(stage_name,t.tags)

        for tool in tools:
            tool.stage_name = stage_name
            tool.tags.update(tag)
            self.G.add_node(tool)
            yield tool

    @flowfxn
    def map_(self,tool_class,stage_name=None,tag={}):
        """
Creates one2one relationships of the dag's current active_tools with a new tool of
type `tool_class`.

:param tool_class: (subclass of Tool)
:param stage_name: (str) The name of the stage to add to. Defaults to the name of the tool class.
:param tag: (dict) A dictionary of tags to add to the tools produced by this flowfxn
:return: (DAG) self

>>> dag.map_(Tool_Class)
"""
        parent_tools = self.active_tools
        for parent_tool in parent_tools:
            tags2 = parent_tool.tags.copy()
            tags2.update(tag)
            new_tool = tool_class(stage_name=stage_name,dag=self,tags=tags2)
            self.G.add_edge(parent_tool,new_tool)
            yield new_tool
            
    @flowfxn
    def split_(self,split_by,tool_class,stage_name=None,tag={}):
        """
Creates one2many relationships for each tool in the dag's active_tools, with every possible combination
of keywords in split_by. New tools will be of class `tool_class` and tagged with one of the possible keyword
combinations.

:param split_by: (list of (str,list)) Tags to split by.
:param tool_class: (list) Tool instances.
:param stage_name: (str) The name of the stage to add to. Defaults to the name of the tool class.
:param tag: (dict) A dictionary of tags to add to the tools produced by this flowfxn.
:return: (DAG) self

>>> dag.split_([('shape',['square','circle']),('color',['red','blue'])],Tool_Class)


The above will create 4 new tools dependent on each tool in active_tools. The new tools will be tagged
with the tags of their parents plus these:

.. code-block:: python

{'shape':'square','color':'red'}, {'shape':'square','color':blue'},
{'shape':'circle','color':'red'}, {'shape':'square','circle':blue'}
"""
        parent_tools = self.active_tools
        splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
        for parent_tool in parent_tools:
            for new_tags in it.product(*splits):
                tags = dict(parent_tool.tags).copy()
                tags.update(tag)
                tags.update(dict(new_tags))
                new_tool = tool_class(stage_name=stage_name,dag=self,tags=tags)
                self.G.add_edge(parent_tool,new_tool)
                yield new_tool


    @flowfxn
    def reduce_(self,keywords,tool_class,stage_name=None,tag={}):
        """
Create new tools with a many2one relationship to the dag's current active_tools.

:param keywords: (list of str) Tags to reduce to. All keywords not listed will
not be passed on to the tasks generated. Tools not tagged with a value in keywords will be a parent
of all new tools generated.
:param tool_class: (list) Tool instances.
:param stage_name: (str) The name of the stage to add to. Defaults to the name of the tool class.
:param tag: (dict) A dictionary of tags to add to the tools produced by this flowfxn
:return: (DAG) self

>>> dag.reduce(['shape','color'],Tool_Class)

In the above example, a new stage will be created using `Tool_Class`. The active_nodes will be placed
into groups of the possible combinations of `shape` and `color`, and a child tools will be tagged
with the same `shape` and `color` of their parents.
"""
        parent_tools = self.active_tools
        if type(keywords) != list:
            raise TypeError('keywords must be a list')

        parent_tools_without_all_keywords = filter(lambda t: not all([k in t.tags for k in keywords]), parent_tools)
        parent_tools_with_all_keywords = filter(lambda t: all(k in t.tags for k in keywords), parent_tools)
        for tags, parent_tool_group in groupby(parent_tools_with_all_keywords,lambda t: dict((k,t.tags[k]) for k in keywords if k in t.tags)):
            parent_tool_group = list(parent_tool_group) + parent_tools_without_all_keywords
            tags.update(tag)
            new_tool = tool_class(stage_name=stage_name,dag=self,tags=tags)
            for parent_tool in parent_tool_group:
                self.G.add_edge(parent_tool,new_tool)
            yield new_tool

    @flowfxn
    def reduce_split_(self,keywords,split_by,tool_class,stage_name=None,tag={}):
        """
Create new tools by first reducing then splitting.

:param keywords: (list of str) Tags to reduce to. All keywords not listed will not be passed on to the tasks generated.
:param split_by: (list of (str,list)) Tags to split by. Creates every possible product of the tags.
:param tool_class: (list) Tool instances.
:param stage_name: (str) The name of the stage to add to. Defaults to the name of the tool class.
:param tag: (dict) A dictionary of tags to add to the tools produced by this flowfxn
:return: (DAG) self

>>> dag.reduce_split_(['color','shape'],[('size',['small','large'])],Tool_Class)

The above example will reduce the active_tools by `color` and `shape`, and then split into two tools with tags
``{'size':'large'}`` and ``{'size':'small'}``, plus the ``color`` and ``shape``
of their parents.
"""
        parent_tools = self.active_tools
        splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]

        for group_tags,parent_tool_group in groupby(parent_tools,lambda t: dict([(k,t.tags[k]) for k in keywords])):
            parent_tool_group = list(parent_tool_group)
            for new_tags in it.product(*splits):
                tags = group_tags.copy()
                tags.update(tag)
                tags.update(dict(new_tags))
                new_tool = tool_class(stage_name=stage_name,dag=self,tags=tags)
                for parent_tool in parent_tool_group:
                    self.G.add_edge(parent_tool,new_tool)
                yield new_tool


    def apply_(self,*flowlist,**kwargs):
        """
Applies each flowfxn in \*flowlist to current dag.active_tools.

For example, at a high level, apply_(B,C,D) translates to B(active_tools),C(active_tools),D(active_tools).

:param \*flowlist: A sequence of flowfxns
:param combine: Combines all tools produced by flowlist and sets the self.active_tools to the union of them.
:returns: (DAG) this dag

>>> dag.sequence_(map_(ToolX), sequence_([ reduce_(['a'],ToolA), map_(,ToolB]), split_(['b',['2']],ToolC]) ]))

In the above example, ToolA, ToolB and ToolC will all be applied to the instances generated by ToolX.
The next flowfxn will only apply to the tools generated by ToolC.
"""
        if not isinstance(flowlist,tuple):
            raise TypeError, "flowlist must be a tuple, flowlist is a {0}".format(flowlist.__class_)

        combine = kwargs.get('combine',False)

        combined_result_tools = []
        original_active_tools = self.active_tools
        for flowclass in flowlist:
            fxn_name = flowclass.__class__.__name__
            fxn = getattr(self,fxn_name)
            self.active_tools = original_active_tools
            fxn(*flowclass.args,**flowclass.kwargs)
            combined_result_tools.extend(self.active_tools)
        if combine:
            self.active_tools = combined_result_tools
        return self

    def sequence_(self,*flowlist,**kwargs):
        """
Applies each flowfxn in \*flowlist sequentially to each other. Very similar to python's builtin :py:meth:`reduce`
function (not to be confused with :py:meth:`DAG.reduce_`), initialized with the current active_nodes.

For example, at a high level sequence_(B,C,D) translates to D(C(B(active_tools))).

:param \*flowlist: A sequence of flowfxns
:param combine: Combines all tools produced by flowlist and sets the self.active_tools to the union of them.
:returns: (DAG) this dag

>>> dag.sequence_(map_(ToolX), seq_([ reduce_(['a'],ToolA), map_(,ToolB]), split_(['b',['2']],ToolC]) ]))

In the above example, ToolA will be applied to Toolx, ToolB to ToolA, and ToolC applied to ToolB. ToolC
will be set as dag.active_tools
"""
        combine = kwargs.get('combine',False)

        if not isinstance(flowlist,tuple):
            raise TypeError, "flowlist must be a tuple, flowlist is a {0}".format(flowlist.__class_)

        combined_result_tools = []
        for flowclass in flowlist:
            fxn_name = flowclass.__class__.__name__
            fxn = getattr(self,fxn_name)
            fxn(*flowclass.args,**flowclass.kwargs)
            combined_result_tools.extend(self.active_tools)
        if combine:
            self.active_tools = combined_result_tools
        return self


class MethodStore(object):
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs

class add_(MethodStore):pass
class map_(MethodStore):pass
class split_(MethodStore):pass
class reduce_(MethodStore): pass
class reduce_split_(MethodStore):pass
class branch_(MethodStore):pass

class sequence_(MethodStore):pass
class apply_(MethodStore):pass

class configure(MethodStore):pass
class add_run(MethodStore):pass

import types
#TODO: this might work really well with named pipes
# def pipe_(*tool_classes):
    # if len(tool_classes) <2:
    # raise FlowFxnValidationError, 'pipe_ requires at least two tools'
    # if not issubclass(tool_classes[0],Tool):
    # raise FlowFxnValidationError, 'pipe_ requires its inputs to be a list of Tools'
    # if len(tool_classes[0].outputs) > 1 or len(tool_classes[0].inputs) > 1:
    # raise FlowFxnValidationError, 'pipe_ does not currently support tools with multiple inputs or outputs'
    #
    #
    # class Piped(tool_classes[-1]):
    # def cmd(self,i,s,p):
    # minidag = DAG()
    # cmds = []
    # last_parents = self.parents
    # for tool_num,tc in enumerate(self.tool_classes):
    # tc = tc(dag=minidag,stage_name=self.stage_name,tags=self.tags)
    # for parent in last_parents: #should only be more than one for the first tool
    # minidag.G.add_edge(parent,tc)
    # i2 = {tc.inputs[0]:['/dev/stdin']} if tool_num>0 else i
    # cmd = tc.cmd(i2,s,p)
    # if tool_num<len(tool_classes)-1:
    # cmd = cmd.replace('$OUT.'+tc.outputs[0],'/dev/stdout')
    # cmds.append(cmd)
    # return '\n|\n'.join(cmds)
    #
    # Piped.tool_classes = tool_classes
    # Piped.inputs = tool_classes[0].inputs
    # Piped.outputs = tool_classes[-1].outputs
    # Piped.name = 'Pipetest'
    # return Piped
