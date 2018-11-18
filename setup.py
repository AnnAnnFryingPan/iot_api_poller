from setuptools import setup

setup(name='iot_api_poller',
      version='0.9',
      description='Polling app that fetches data from IoT hubs',
      url='https://github.com/AnnAnnFryingPan/iot_api_poller',
      author='Ann Gledson',
      author_email='anngledson@gmail.com',
      license='MIT',
      packages=['iot_api_poller'],
	  install_requires=[
          'certifi',
	      'chardet',
          'data-hub-call',
          'idna',
          'scheduled_poller',
          'python-dateutil',
          'requests',
          'six',
          'urllib3'
      ],
      zip_safe=False)

