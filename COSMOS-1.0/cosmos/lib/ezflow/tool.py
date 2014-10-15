from helpers import getcallargs,cosmos_format
import re
from cosmos.Workflow.models import TaskFile
from cosmos.utils.helpers import parse_cmd
import itertools, copy

i = 0
def get_id():
    global i
    i +=1
    return i

files = []

class ExpectedError(Exception): pass
class ToolError(Exception): pass
class ToolValidationError(Exception):pass
class GetOutputError(Exception): pass


class Tool(object):
    """
A Tool is a class who's instances represent a command that gets executed. It also contains properties which
define the resources that are required.

:property stage_name: (str) The name of this Tool's stage. Defaults to the name of the class.
:property dag: (DAG) The dag that is keeping track of this Tool
:property id: (int) A unique identifier. Useful for debugging.
:property input_files: (list) This Tool's input TaskFiles
:property output_files: (list) This Tool's output TaskFiles. A tool's output taskfile names should be unique.
:property tags: (dict) This Tool's tags.
"""
    #TODO props that cant be overridden should be private

    #: (list of strs) a list of input names.
    inputs = []
    #: (list of strs or TaskFiles) a list of output names. Default is [].
    outputs = []
    #: (int) Number of megabytes of memory to request. Default is 1024.
    mem_req = 1*1024
    #: (int) Number of cores to request. Default is 1.
    cpu_req = 1
    #: (int) Number of minutes to request. Default is 1.
    time_req = None #(mins)
    #: (bool) If True, these tasks do not contain commands that are executed. Used for INPUT. Default is False.
    NOOP = False
    #: (bool) If True, the input files of this tool will also be input files of children of this tool. Default is False.
    forward_input = False
    #: (bool) If True, if this tool's tasks' job attempts fail, the task will still be considered successful. Default is False.
    succeed_on_failure = False
    #: (dict) A dictionary of default parameters. Default is {}.
    default_params = {}
    #: (bool) If True, output_files described as a str in outputs will be by default be created with persist=True.
    #: If delete_interemediates is on, they will not be deleted.
    persist=False

    def __init__(self,stage_name=None,tags={},dag=None):
        """
:param stage_name: (str) The name of the stage this tool belongs to. Required.
:param tags: (dict) A dictionary of tags.
:param dag: The dag this task belongs to.
:param parents: A list of tool instances which this tool is dependent on
"""
        #if len(tags)==0: raise ToolValidationError('Empty tag dictionary. All tools should have at least one tag.')
        if not hasattr(self,'name'): self.name = self.__class__.__name__
        if not hasattr(self,'output_files'): self.output_files = []
        if not hasattr(self,'settings'): self.settings = {}
        if not hasattr(self,'parameters'): self.parameters = {}

        self.stage_name = stage_name if stage_name else self.name
        self.tags = tags
        self.dag = dag

        # Because defining attributes in python creates a reference to a single instance across all class instance
        # any taskfile instances in self.outputs is used as a template for instantiating a new class
        self.outputs = [ copy.copy(o) if isinstance(o,TaskFile) else o for o in self.outputs ]
        self.id = get_id()

        # Create empty output TaskFiles
        for output in self.outputs:
            if isinstance(output, TaskFile):
                self.add_output(output)
            elif isinstance(output,str):
                tf = TaskFile(fmt=output,persist=self.persist)
                self.add_output(tf)
            else:
                raise ToolValidationError, "{0}.outputs must be a list strs or Taskfile instances.".format(self)

        #validate inputs are strs
        if any([ not isinstance(i,str) for i in self.inputs]):
            raise ToolValidationError,"{0} has elements in self.inputs that are not of type str".format(self)

        if len(self.inputs) != len(set(self.inputs)):
            raise ToolValidationError('Duplicate names in tool.inputs detected in {0}. Perhaps try using [1.ext,2.ext,...]'.format(self))

        output_names = [ o.name for o in self.output_files ]
        if len(output_names) != len(set(output_names)):
            raise ToolValidationError('Duplicate names in tool.output_files detected in {0}. Perhaps try using [1.ext,2.ext,...] when defining outputs'.format(self))

    @property
    def children(self):
        return self.dag.G.successors(self)

    @property
    def child(self):
        cs = self.children
        if len(cs) > 1:
            raise ToolError('{0} has more than one parent. The parents are: {1}'.format(self,self.parents))
        elif len(cs) == 0:
            raise ToolError('{0} has no parents'.format(self))
        else:
            return cs[0]

    @property
    def parents(self):
        return self.dag.G.predecessors(self)
    
    @property
    def parent(self):
        ps = self.parents
        if len(ps) > 1:
            raise ToolError('{0} has more than one parent. The parents are: {1}'.format(self,self.parents))
        elif len(ps) == 0:
            raise ToolError('{0} has no parents'.format(self))
        else:
            return ps[0]
    
    def get_output(self,name,error_if_missing=True):
        """
Returns the output TaskFiles who's name == name. This should always be one element.
:param name: the name of the output file.
:param error_if_missing: (bool) Raises a GetOutputError if the output cannot be found
"""

        output_files = filter(lambda x: x.name == name, self.output_files)

        if len(output_files) > 1: raise GetOutputError('More than one output with name {0} in {1}'.format(name,self),name)

        if len(output_files) == 0 and self.forward_input:
            try:
                output_files += [ p.get_output(name) for p in self.parents ]
            except GetOutputError as e:
                pass

        if len(output_files) == 0:
            if error_if_missing:
                raise GetOutputError('No output file in {0} with name {1}.'.format(self,name),name)
            else:
                return None
        else:
            return output_files[0]
    
    def get_output_file_names(self):
        return set(map(lambda x: x.name, self.output_files))
        
    def add_output(self,taskfile):
        """
Adds an taskfile to self.output_files
:param taskfile: an instance of a TaskFile
"""
        self.output_files.append(taskfile)
        
    @property
    def input_files(self):
        "A list of input TaskFiles"
        return list(itertools.chain(*[ tf for tf in self.map_inputs().values() ]))

    @property
    def label(self):
        "Label used for the DAG image"
        tags = '' if len(self.tags) == 0 else "\\n {0}".format("\\n".join(["{0}: {1}".format(k,v) for k,v in self.tags.items() ]))
        return "[{3}] {0}{1}\\n{2}".format(self.name,tags,self.pcmd,self.id)

    def map_inputs(self):
        """
Default method to map inputs. Can be overriden if a different behavior is desired
:returns: (dict) A dictionary of taskfiles which are inputs to this tool. Keys are names of the taskfiles, values are a list of taskfiles.
"""
        if not self.inputs:
            return {}

        else:
            all_inputs = []
            if '*' in self.inputs:
                return {'*':[ o for p in self.parents for o in p.output_files ]}

            for name in self.inputs:
                for p in self.parents:
                    all_inputs += filter(lambda x: x,[ p.get_output(name,error_if_missing=False) ]) #filter out Nones

            input_dict = {}
            for input_file in set(all_inputs):
                input_dict.setdefault(input_file.name,[]).append(input_file)

            for k,v in input_dict.items():
                if len(v) == 0:
                    raise ToolValidationError, "Could not find input '{0}' in {1}".format(k,self)

            return input_dict
        
        
    @property
    def pcmd(self):
        return self.process_cmd() if not self.NOOP else ''
    
    def process_cmd(self):
        """
Calls map_inputs() and processes the output of cmd()
"""
        p = self.parameters.copy()
        p.update(self.tags)
        callargs = getcallargs(self.cmd,i=self.map_inputs(),s=self.settings,p=p)
        del callargs['self']
        r = self.cmd(**callargs)
        
        #if tuple is returned, second element is a dict to format with
        extra_format_dict = r[1] if len(r) == 2 and r else {}
        pcmd = r[0] if len(r) == 2 else r

        #replace $OUT with a string representation of a taskfile
        out_names = re.findall('\$OUT\.([\.\w]+)',pcmd)
        for out_name in out_names:
            try:
                pcmd = pcmd.replace('$OUT.{0}'.format(out_name),str(self.get_output(out_name)))
            except GetOutputError as e:
                raise ToolValidationError('Invalid key in $OUT.key ({0}), available output_file keys in {1} are {2}'.format(out_name,self,self.get_output_file_names()))

        #Validate all output_files have an $OUT
        for tf in self.output_files:
            if tf.name not in out_names:
                raise ToolValidationError,\
                    'An output taskfile with name {1} is in {0}.output_files but not referenced with $OUT in the tool\'s command: out_names= {2}'.\
                        format(self,tf.name,out_names)
                
        #format() return string with callargs
        callargs['self'] = self
        callargs.update(extra_format_dict)
        return parse_cmd(cosmos_format(*self.post_cmd(pcmd,callargs)))

    def post_cmd(self,cmd_str,format_dict):
        """
Provides an opportunity to make any last minute changes to the cmd generated.

:param cmd_str: (str) the string returned by cmd
:param format_dict: (str) the dictionary that cmd was about to be .format()ed with
:returns: (str,dict) the post_processed cmd_str and format_dict
"""
        return cmd_str,format_dict


    def cmd(self, i, s, p):
        """
Constructs the preformatted command string. The string will be .format()ed with the i,s,p dictionaries,
and later, $OUT.outname will be replaced with a TaskFile associated with the output name `outname`

:param i: (dict) Input TaskFiles.
:param s: (dict) Settings. The settings dictionary, set by using :py:meth:`contrib.ezflow.dag.configure`
:param p: (dict) Parameters.
:returns: (str|tuple(str,dict)) A preformatted command string, or a tuple of the former and a dict with extra values to use for
formatting
"""
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))

    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__,self.tags)
    
class INPUT(Tool):
    """
An Input File.

Does not actually execute anything, but provides a way to load an input file.

>>> INPUT('/path/to/file.ext',tags={'key':'val'})
>>> INPUT(path='/path/to/file.ext.gz',name='ext',fmt='ext.gz',tags={'key':'val'})
"""
    name = "Load Input Files"
    NOOP = True
    mem_req = 0
    cpu_req = 0
    
    def __init__(self,path,name=None,fmt=None,*args,**kwargs):
        """
:param path: the path to the input file
:param name: the name or keyword for the input file
:param fmt: the format of the input file
"""
        super(INPUT,self).__init__(*args,**kwargs)
        self.add_output(TaskFile(path=path,name=name,fmt=fmt,persist=True))

    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)

