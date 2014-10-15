import os,sys
import pprint

from cosmos.config import settings

from models import Workflow


def add_workflow_args(parser):
    parser.add_argument('-n','--name',help="A unique name for this workflow. All spaces are converted to underscores.",required=True)
    parser.add_argument('-q','--default_queue',type=str,help="Deletes unsuccessful tasks in the workflow.  Defaults to the value in cosmos.session.settings.")
    parser.add_argument('-o','--root_output_dir',type=str,default=settings['default_root_output_dir'],help="The root output directory.  Output will be stored in root_output_dir/{workflow.name}.  Defaults to the value in cosmos.session.settings.")
    parser.add_argument('-r','--restart',action='store_true',help="Complete restart the workflow by deleting it and creating a new one.")
    parser.add_argument('-di','--delete_intermediates',action='store_true',help="Deletes intermediate files to save scratch space.")
    parser.add_argument('-y','--prompt_confirm',action='store_false',help="Do not use confirmation prompts before restarting or deleting, and assume answer is always yes.")
    parser.add_argument('-dry','--dry_run',action='store_true',help="Don't actually run any jobs.  Experimental.")

def parse_args(parser):
    """
    Runs the argument parser

    :returns: a workflow instance and kwargs parsed by argparse
    """
    parsed_args = parser.parse_args()
    kwargs = dict(parsed_args._get_kwargs())

    #extract wf_kwargs from kwargs
    wf_kwargs = dict([ (k,kwargs[k]) for k
                       in ['name','default_queue','root_output_dir','restart','delete_intermediates','prompt_confirm','dry_run'] ])
    wf_kwargs['comments'] = '$ ' +' '.join([os.path.basename(sys.argv[0])]+sys.argv[1:])

    wf = Workflow.start(**wf_kwargs)

    wf.log.info('Parsed kwargs:\n{0}'.format(pprint.pformat(kwargs)))
    return wf,kwargs
