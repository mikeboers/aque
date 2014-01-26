
def main(job):

    func = job['func']
    args = job.get('args', ())
    kwargs = job.get('kwargs', {})

    print 'running', func, 'with', args, kwargs

    job.success(func(*args, **kwargs))
