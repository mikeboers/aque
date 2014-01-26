import grp
import os
import pwd

from aque.job import Job


class Queue(object):

    def __init__(self, redis, name='aque'):

        self.redis = redis
        self.name = name

        self._dbid = redis.connection_pool.connection_kwargs['db']

    def _format(self, format, *args, **kwargs):
        if kwargs.pop('_db', None):
            return ('{}@{}:' + format).format(self.name, self._dbid, *args)
        else:
            return ('{}:' + format).format(self.name, *args)

    def submit(self, job):

        if not isinstance(job, Job):
            job = Job(job)

        user = pwd.getpwuid(os.getuid())
        group = grp.getgrgid(user.pw_gid)

        job.setdefaults(
            priority=1000,
            user=user.pw_name,
            group=group.gr_name,
        )

        id_num = self.redis.incr(self._format('job_counter'))

        jid = job['id'] = self._format('job:{}', id_num)
        job['status'] = 'pending'

        self.redis.hmset(jid, job)
        self.redis.rpush(self._format('pending_jobs'), jid)
        self.redis.publish(self._format('{}:status', jid, _db=True), job['status'])

        return jid

