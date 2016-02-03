#!/bin/bash
NUM_MESSAGES=1000000

consumer/consumer -port 9092  &
CONSUMER_PID=$!
sleep 1
START=$(date +%s)
producer/producer -port 9093 -size 100 -number $NUM_MESSAGES  &
iostat -y /dev/sda 30 &
IOSTAT_PID=$!
wait $CONSUMER_PID
END=$(date +%s)
kill $IOSTAT_PID
echo "Average $(( $NUM_MESSAGES / ($END - $START) )) messages per second"
