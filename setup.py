from distutils.core import setup

setup(name='groundmotion-database',
      version='0.1dev',
      description='USGS ShakeMap Ground Motion Database Tools',
      author='Mike Hearne',
      author_email='mhearne@usgs.gov',
      url='https://github.com/usgs/groundmotion-database',
      packages=['gmdb'],
      scripts=['bin/getpgm'],
      )
