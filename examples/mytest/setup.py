from setuptools import setup

setup(
    name='click-example-mytest',
    version='0.1',
    py_modules=['mytest'],
    include_package_data=True,
    install_requires=[
        'boto',
        'click',
        'click-log',
        'csv',
        'logging',
        'Queue',
        'random',
        'threading',
        'time'
    ],
    entry_points='''
        [console_scripts]
        mytest=mytest:cli
    ''',
)
