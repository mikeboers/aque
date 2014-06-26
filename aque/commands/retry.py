"""aque retry - Retry errored or killed tasks.

Reschedules tasks to run. They remain on the same ID.

"""

import os
import sys

from aque.commands.main import command, argument


@command(
    argument('-s', '--success', action='store_true', help='also retry successful jobs'),
    argument('tids', nargs='+', type=int, metavar='TID', help='specific tasks to display'),
    help='retry errored or killed tasks',
    description=__doc__,
)
def retry(args):

    tasks_by_id = args.broker.fetch(args.tids)
    for _, task in sorted(tasks_by_id.iteritems()):
        if task['status'] in ('error', 'killed') or args.success and task['status'] == 'success':
            args.broker.set_status_and_notify(task['id'], 'pending')
            task['status'] = 'pending'
        print task['id'], task['status']
