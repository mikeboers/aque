# Nobody puts us in the corner... except us.

# This is as clean of an execution environment as we can muster for apps like
# Maya which will execute arbitrary code in the __main__ module.

from aque.worker import procjob_execute as _aque_worker_procjob_execute
_aque_worker_procjob_execute()
