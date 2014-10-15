import django.dispatch

stage_status_change = django.dispatch.Signal(providing_args=["status"])
task_status_change = django.dispatch.Signal(providing_args=["status"])

#
# from django.db.models.signals import post_save
# from django.dispatch import receiver
# from models import Task, Stage
#
# @receiver(post_save, sender=Task)
# def post_task_save(sender, **kwargs):
#     pass
#
# @receiver(post_save, sender=Stage)
# def post_stage_save(sender, **kwargs):
#     pass