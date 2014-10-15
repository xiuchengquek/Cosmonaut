from cosmos.Workflow.models import Workflow
from cosmos.lib.ezflow.dag import DAG, split_,add_,map_,reduce_
from tools import ECHO, CAT, WC, PASTE, Sleep

####################
# Workflow
####################

dag = DAG().sequence_(
    add_([ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]),
    map_(Sleep),
    split_([('i',[1,2])], CAT),
    reduce_([], PASTE),
    map_(WC),
)

dag.create_dag_img('/tmp/ex.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example 3',restart=True,delete_intermediates=True)
dag.add_to_workflow(WF)
WF.run()
