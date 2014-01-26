import collections
import grp
import itertools
import os
import pwd

class Job(dict):

    def __init__(self, *args, **kwargs):
        for x in itertools.chain(args, (kwargs, )):
            self.update(x)

        user = pwd.getpwuid(os.getuid())
        group = grp.getgrgid(user.pw_gid)

        self.setdefaults(
            type='generic',
            priority=1000,
            user=user.pw_name,
            group=group.gr_name,
        )

    def setdefaults(self, *args, **kwargs):
        res = {}
        for x in itertools.chain(args, (kwargs, )):
            for k, v in x.iteritems():
                res[k] = self.setdefault(k, v)
        return res

