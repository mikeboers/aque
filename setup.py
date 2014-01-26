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
    ],
    
    entry_points={
        'aque_handlers': [
            'generic = aque.handlers.generic:handle_generic',
        ],
    },

)
