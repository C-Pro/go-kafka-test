#!/bin/bash
echo
consumer/consumer -port 9092 &
sleep 1
iostat
producer/producer -port 9093 -size 1000 &
sleep 20
iostat