from cosmos.Workflow.models import Workflow
from cosmos.lib.ezflow.dag import DAG, add_,split_
from tools import ECHO, CAT

####################
# Workflow
####################

dag = DAG().sequence_(
    add_([ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]),
    split_([('i',[1,2])],CAT)
)
dag.create_dag_img('/tmp/ex.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example 1',restart=True)
dag.add_to_workflow(WF)
WF.run()
