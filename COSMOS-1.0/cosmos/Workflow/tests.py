from Workflow.models import Workflow, Stage, Task
from django.test import TestCase
from django.core.exceptions import ValidationError


class Test_Workflow(TestCase):
    def setUp(self):
        self.wF = Workflow.__create(name='Test_WF',root_output_dir='/tmp')
        
    def tearDown(self):
        self.wF.delete()
        
    def test_no_duplicate_stage_names(self):
        self.wF.add_stage(name='Test_Stage')
        #import ipdb; ipdb.set_trace()
        self.assertRaises(ValidationError,self.wF.add_stage,name='Test_Stage')
        
    def test_no_duplicate_task_names(self):
        b = self.wF.add_stage(name='Test_Stage')
        b.new_task(pre_command='',outputs='',name='test_task')
        self.assertRaises(ValidationError,b.new_task,pre_command='',outputs='',name='test_task')
    
    
    def test_one_command(self):
        b = self.wF.add_stage(name='Test_Stage')
        b.new_task(pre_command='ls / > {output_dir}/{outputs[output_file]}',outputs={'output_file':'myls.out'},name='ls_test1')
        self.wF.run_stage(b)
        
    def test_resume(self):
        #run once
        b = self.wF.add_stage(name='Test_Stage')
        b.new_task(pre_command='ls / > {output_dir}/{outputs[output_file]}',outputs={'output_file':'myls.out'},name='ls_test1')
        self.wF.run_stage(b)
 
        #run second time, have to setup again
        self.wF  = Workflow.__resume(name='Test_WF')
        assert self.wF.stages.count() == 1
        
        #next command shouldn't __create a new task since it already exists
        b = self.wF.add_stage(name='Test_Stage')
        b.new_task(pre_command='ls / > {output_dir}/{outputs[output_file]}',outputs={'output_file':'myls.out'},name='ls_test1')
        
        assert self.wF.stages.count() == 1
        #next command should skip execution
        self.wF.run_stage(b)
         



#from JobManager.models import JobManager,Job
#from Tools.models.Echo import Echo
#from Tools.models.Cat import Cat
#from Workflow.models import Workflow
#from django.test import LiveServerTestCase
#
#def slow(f):
#  def decorated(self):
#    f(self) #comment to skip slow functions
#    pass 
#  return decorated
#
#
#class Test_Echo(LiveServerTestCase):
#    def setUp(self):
#        self.JM = JobManager.objects.__create()
#        self.JM.init()
#        self.JM.save()
#        self.WF = Workflow.objects.__create()
#        self.WF.save()
#        
#    def tearDown(self):
#        self.JM.close()
#        
#    def test_add_task(self):
#        echo = Echo(text="test")
#        echo.save()
#        cat = Cat()
#        cat.save()
#        self.WF.new_task(cat)
#        self.WF.new_task(echo)
#        tasks = self.WF.get_tasks()
#        assert tasks[0] == echo
#        assert tasks[1] == cat
#        assert len(tasks) == 2
#        assert echo in tasks
#        assert cat in tasks
#        
#    
#    def test_add_edge(self):
#        echo = Echo(text="test")
#        echo.save()
#        cat = Cat()
#        cat.save()
#        self.WF.add_edge(echo,'output_file',cat,'input_file')
#        #check edges
#        edges = self.WF._DAG.edges(data=True)
#        assert edges[0][0] == echo
#        assert edges[0][1] == cat
#        assert edges[0][2] == {'source_field': u'output_file', 'destination_field': u'input_file'}
#        #check tasks
#        tasks = self.WF.get_tasks()
#        tasks = self.WF.get_tasks()
#        assert len(tasks) == 2
#        assert echo in tasks
#        assert cat in tasks
#        import ipdb; ipdb.set_trace()
        
    
        