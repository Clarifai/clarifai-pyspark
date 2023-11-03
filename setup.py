from setuptools import find_packages, setup

with open("README.md", "r") as fh:
  long_description = fh.read()

with open("VERSION", "r") as f:
  version = f.read().strip()

with open("requirements.txt", "r") as fh:
  install_requires = fh.read().split('\n')

if install_requires and install_requires[-1] == '':
  # Remove the last empty line
  install_requires = install_requires[:-1]

setup(
    name='clarifai-pyspark',
    version=f"{version}",
    author="Clarifai",
    author_email="support@clarifai.com",
    description="Clarifai PySpark Python SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Clarifai/clarifai-pyspark",
    packages=find_packages(),
    classifiers=[
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    license="Apache 2.0",
    python_requires='>=3.8',
    install_requires=install_requires,
    include_package_data=True)
