import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='metroholidays',
     version='0.11',
     scripts=[],
     author="Dmitry Vazhenin",
     author_email="dmitry.vazhenin@metroengines.jp",
     description="Python client module to access Metroengines Holidays API",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/jpmobiletanaka/holidays_api_python",
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "Operating System :: OS Independent",
     ],
     install_requires=[
       'pandas',
       'requests',
       'redo',
       'assertpy'
     ]
 )
