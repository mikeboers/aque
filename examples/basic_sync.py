from aque import Job, handler

@handler
def get_jid(job):
    print 'get_jid:', job['id']
    job.success(job['id'])

deps = [Job(type='get_jid') for i in xrange(5)]
children = [Job(type='get_jid') for i in xrange(5)]
job = Job(type='get_jid', dependencies=deps, children=children)
print job.run()
