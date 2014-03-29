#!/bin/bash

export AQUE_BROKER=${AQUE_TEST_BROKER-postgres:///aquetest}
aque init --reset || exit
nosetests "$@"
