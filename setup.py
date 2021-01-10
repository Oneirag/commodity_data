from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='commodity_data',
    version='0.1.3',
    packages=['commodity_data', 'commodity_data.omip'],
    url='https://github.com/Oneirag/commodity_data.git',
    license='GNU GPLv3',
    author='Oscar Neira',
    author_email='oneirag@yahoo.es',
    description='Tool to download commodity prices from Market websites',
    install_requires=requirements,
)
