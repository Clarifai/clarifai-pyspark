from setuptools import setup, find_packages

with open("requirements.txt", "r") as fh:
  install_requires = fh.read().split('\n')

if install_requires and install_requires[-1] == '':
  # Remove the last empty line
  install_requires = install_requires[:-1]

setup(
    name='clarifaipyspark',
    version='0.1',
    packages=find_packages(),
    install_requires=install_requires,
)
