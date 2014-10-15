from django import template
import re
import sys 
import datetime
from django.utils.safestring import mark_safe
register = template.Library()

@register.filter
def aslist(o):
    return [o]

@register.simple_tag
def get_sjob_stat(stage,field,statistic,pipe=None):
    r = stage.get_sjob_stat(field,statistic)
    if r is None:  return ''
    return getattr(sys.modules[__name__],pipe)(r) if pipe else r

@register.simple_tag
def get_task_stat(stage,field,statistic,pipe=None,*args):
    r =  stage.get_task_stat(field,statistic)
    return getattr(sys.modules[__name__],pipe)(r,*args) if pipe else r

@register.simple_tag
def convert2int(x):
    if x == None: return ''
    if x:
        return int(x)
    else: return x


def format_percent(x):
    if x:
        return '{0}%'.format(int(x))
    elif x == 0:
        return '0%'
    else: return ''

@register.filter
def underscore2space(s):
    return re.sub('_',' ',s)

@register.filter
def key2val(d, key_name):
    return d[key_name]

@register.simple_tag
def navactive(request, name):
    if name=='home' and request.path=='/':
        return 'active'
    elif name in request.path.split('/'):
        return "active"
    return ""


@register.filter
def pprint(s):
    "Returns a '&nbsp' if s is None or blank"
    import pprint
    return pprint.pformat(s)

@register.filter
def b2e(s):
    "Returns a '&nbsp' if s is None or blank"
    return mark_safe('&nbsp;') if s in [None,''] else s

@register.simple_tag
def status2csstype(status):
    d = {'failed':'danger',
     'successful':'success',
     'no_attempt':'info',
     'in_progress':'warning'}
    return d[status]

@register.filter
def mult(value, arg):
    "Multiplies the arg and the value"
    return int(value) * int(arg)

@register.simple_tag
def format_resource_usage(field_name,val,help_txt):
    if val == None: return ''
    elif re.search(r"\(Kb\)",help_txt):
        if val == 0: return '0'
        return "{0} ({1})".format(val,format_memory_kb(val))
    elif re.search(r"time",field_name):
        return "{1}".format(val,format_time(val))
    elif field_name=='percent_cpu':
        return "{0}%".format(val)
    elif type(val) in [int,long]:
        return intWithCommas(val)
    return str(val)

def intWithCommas(x):
    if x == None: return ''
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)
            

@register.filter
def format_memory_kb(kb):
    """converts kb to human readible"""
    if kb == None: return ''
    mb = kb/1024.0
    gb = mb/1024.0
    if gb > 1:
        return "%sGB" % round(gb,1)
    else:
        return "%sMB" % round(mb,1)
@register.filter
def format_memory_mb(mb):
    """converts mb to human readible"""
    return format_memory_kb(mb*1024.0) if mb else ""

@register.filter
def format_time(amount,type="seconds"):
    if amount == None or amount == '': return ''
    if type == 'minutes':
        amount = amount*60
    return datetime.timedelta(seconds=int(amount))
        