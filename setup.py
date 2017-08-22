from setuptools import setup

setup(name='pyparty',
      version='0.1',
      description='Python eventing engine',
      url='http://github.com/philusky/pyparty',
      author='Hultaj',
      author_email='philusky@gmail.com',
      license='MIT',
      packages=['pyparty'],
      install_requires=[
          'pymongo',
      ],
      zip_safe=False)