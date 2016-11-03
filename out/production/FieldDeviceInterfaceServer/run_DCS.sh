#!/bin/bash

until java -cp :../../../kafka_2.11-0.9.0.1/libs/*:../../../json-simple-1.1.1.jar FieldDeviceInterfaceServer; do
	echo "DCS crashed with exit code $?.  respawning..." >&2
	sleep 5
done