#!/bin/bash

rm -rf build dist
python3 setup.py sdist bdist_wheel

cp dist/metroholidays-0.11-py3-none-any.whl metroholidays-0.11-py3-none-any.whl 
