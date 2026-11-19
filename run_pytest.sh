#!/bin/bash

if [ -f "pre-commit.log" ]
then
  echo "Error in earlier pre-commit! We skip 'pytest'."
  exit 1
else
  pytest
fi
