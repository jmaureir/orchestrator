#!/usr/bin/env python3
# coding: utf-8

# # Orchestrator Jupyter Simple Test

# In[2]:


import orchestrator as orch
import itertools 
import time

from promise import Promise

# In[3]:


# Asyncronous Tasks
@orch.Async
def task_1(arg):
    from random import randint
    import time
    
    sleep = randint(1,10)
    sleep = arg

    print("task 1 with arg: %s sleep %d" % (arg, sleep))
    time.sleep(sleep)
    print("task 1 with arg: %s done sleep %d" % (arg, sleep))

    return "task_1(%s)" % arg 

@orch.Async
def task_2(pair):
    import sys 

    print("task_2 with args:", pair)

    return "%s+%s" % (pair[0],pair[1])


# In[8]:

@orch.Async
def joiner(lst,n):
    print("joiner waiting for ",lst)
    resolved_lst = [ x.get() for x in lst ]
    print("joiner ",resolved_lst)
    l = list(zip(*[itertools.islice(resolved_lst, i, None, n) for i in range(n)]))
    return [ (a,b) for a,b in l ]


# Local workflow with async calls only
do_local = True

args=[1,2,3,4,5,6,7,8,9,10,11,12]

if do_local:
    # wrapping the function in an asyncronous call
    @orch.Async
    def process_arg(arg):
        p = task_1(arg)
        return p

    res = []
    for arg in args:
        # run each task_1 as a promise 
        p = Promise.resolve(process_arg(arg))
        res.append(p)

    # get all the promise responses and *then*, the join each output in tuples and *then*, call task_2
    p = Promise.all(res).then(lambda x: joiner(x,2) ).then(lambda x : [task_2(p) for p in x.get()]).get()

    print("final promise: ",p)
    
    result = [ x.get() for x in p ]
    
    print(result)

    print("Local Tasks example done")


# In[46]:
print("=====================================================================")

# Remote Tasks (Jobs) example with barrier on the first task
do_example_2 = True

args=[1,2,3,4,5,6,7,8,9,10,11,12]

def job_1(arg):
    job=orch.Job(params={"ntasks":"1",
        "nodes":"1",
        "job-name":"task_1",
        "cpus-per-task":"1"
    })
    job.setVerbose(True)
    ret = job.run(task_1, arg)
    return(ret)

@orch.Async
def job_2(arg):
    job=orch.Job(params={"ntasks":"1",
        "nodes":"1",
        "job-name":"task_2",
        "cpus-per-task":"1",
    })
    job.setVerbose(True)
    ret = job.run(task_2, arg)
    return(ret)

args=[1,2,3,4,5,6,7,8,9,10,11,12]

if do_example_2:
    res = []
    
    for arg in args:
        res.append(Promise.resolve(job_1(arg)))

    results = Promise.all(res).then(lambda x: joiner(x,2) ).then(lambda x : [job_2(p) for p in x.get() ]).get()


    print("results: ",results)
    
    resolved_result = [x.get() for x in results]
    print("resolved result: ",resolved_result)
    
    print("example 2 done")


# In[17]:

print("=====================================================================")
do_example_3 = True

# remote tasks with parallel workflows
args=[[1,2,3,4],[5,6,7,8],[9,10,11,12]]

def job_t1(arg):
    job=orch.Job(params={        "ntasks":"1",
        "nodes":"1",
        "job-name":"task_1",
        "cpus-per-task":"1"
    })
    print("job_t1 starting for ",arg)
    job.setVerbose(True)
    ret = job.run(task_1, arg)
    return(ret)

@orch.Async
def job_t2(arg):
    arg = arg.get()
    print("job_t2: ", arg)
    job=orch.Job(params={        "ntasks":"1",
        "nodes":"1",
        "job-name":"task_2",
        "cpus-per-task":"1"
    })
    print("job_t2 starting for ",arg)
    job.setVerbose(True)
    ret = job.run(task_2, arg)
    return(ret)

if do_example_3:
    def process_list(partial_list):
        stage_1 = []
        for data in partial_list:
            stage_1.append(Promise.resolve(job_t1(data)))  
        return Promise.all(stage_1).then(lambda x: joiner(x,2)).then(lambda x: job_t2(x)).get()

    # start processing the partial lists in args
    results = [ process_list(x) for x in args ]


# In[19]:

if do_example_3:
    print("result: ",results)

    resolved_result = [x.get() for x in results]
    print("resolved result: ",resolved_result)
    print("Done")


