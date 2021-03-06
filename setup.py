from setuptools import setup, find_packages

setup(
    
    name='aque',
    version='0.1.0',
    description='Async Python work queue.',
    url='http://github.com/mikeboers/aque',
    
    packages=find_packages(exclude=['tests', 'tests.*']),
    
    author='Mike Boers',
    author_email='aque@mikeboers.com',
    license='BSD-3',

    install_requires=[
        'futures',
        'psutil',
        'psycopg2',
    ],
    
    entry_points={
        'aque_patterns': [
            'generic = aque.patterns.generic:do_generic_task',
            'reduce = aque.patterns.reduce:do_reduce_task',
            'shell = aque.patterns.shell:do_shell_task',
        ],
        'aque_brokers': [
            'memory = aque.brokers.memory:MemoryBroker',
            'postgres = aque.brokers.postgres:PostgresBroker',
            'redis = aque.brokers.redis:RedisBroker',
        ],
        'aque_commands': [
            'kill = aque.commands.kill:kill',
            'init = aque.commands.init:init',
            'output = aque.commands.output:output',
            'retry = aque.commands.retry:retry',
            'rm = aque.commands.rm:rm',
            'status = aque.commands.status:status',
            'submit = aque.commands.submit:submit',
            'worker = aque.commands.worker:worker',
            'xargs = aque.commands.xargs:xargs',
        ],
        'console_scripts': [
            'aque = aque.commands.main:main',
        ],
    },

)
