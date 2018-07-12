from setuptools import setup

setup(name='iot_api_poller',
      version='0.4',
      description='Polling app that fetches data from IoT hubs',
      url='https://github.com/AnnAnnFryingPan/iot_api_poller',
      author='Ann Gledson',
      author_email='anngledson@gmail.com',
      license='MIT',
      packages=['iot_api_poller'],
	  install_requires=[
          'certifi==2018.4.16',
	      'chardet==3.0.4',
          'data-hub-call==0.3',
          'idna==2.7',
          'poller==0.1',
          'python-dateutil==2.7.3',
          'requests==2.19.1',
          'six==1.11.0',
          'urllib3==1.23'
      ],
      zip_safe=False)

