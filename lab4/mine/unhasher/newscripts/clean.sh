#!/bin/bash
rm *.{result,out,pid,err}
cd ../src
make clean
make
cd -
