#!/bin/bash
NUM_MESSAGES=1000000

consumer/consumer -brokers localhost:9092,localhost:9093,localhost:9094  &
CONSUMER_PID=$!
sleep 1
START=$(date +%s)
producer/producer -brokers localhost:9092,localhost:9093,localhost:9094 -size 100 -threads 50 -number $NUM_MESSAGES  &
iostat -y /dev/sda 30 &
IOSTAT_PID=$!
wait $CONSUMER_PID
END=$(date +%s)
kill $IOSTAT_PID
echo "Average $(( $NUM_MESSAGES / ($END - $START) )) messages per second"
