#!/bin/bash

gcc \
    -o libSystemProcess.so \
    -shared  \
    -fPIC \
    -I/usr/lib/jvm/java-6-sun/include \
    -I/usr/lib/jvm/java-6-sun/include/linux \
    utils_SystemProcess.c

strip libSystemProcess.so
