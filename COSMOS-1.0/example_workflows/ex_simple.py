"""
A Simple Workflow
"""
from cosmos.Workflow.models import Workflow

wf = Workflow.start('Simple')
stage = wf.add_stage('My Stage')
task = stage.add_task('echo "hello world"')
wf.run()
