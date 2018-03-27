#!/bin/bash
# Remove passwordless access due to security reasons.
# FIXME: at the moment every ssh configuration will be deleted!
# Author: jobusam
# Work: Masterthesis

echo "Passwordless ssh connect to localhost will be removed!"
rm -r ~/.ssh/*
