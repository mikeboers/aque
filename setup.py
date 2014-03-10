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
        'redis',
        'futures',
    ],
    
    scripts=[
        'scripts/aque',
        'scripts/aque-init',
        'scripts/aque-worker',
    ],
    
    entry_points={
        'aque_patterns': [
            'generic = aque.patterns.generic:do_generic_task',
            'reduce = aque.patterns.reduce:do_reduce_task',
            'shell = aque.patterns.shell:do_shell_task',
        ],
        'aque_brokers': [
            'memory = aque.brokers.memory:MemoryBroker',
            'redis = aque.brokers.redis:RedisBroker',
            'postgres = aque.brokers.postgres:PostgresBroker',
        ],
    },

)
