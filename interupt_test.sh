#!/bin/bash

cargo build --release --all-features
rm -r ./test_db
counter=1
step=20000
kill=15
# TODO also test with 9 when 15 get better.
while [ $counter -le 100 ]
do
				counter2=$(($counter * $step))
				./target/release/admin stress \
								--nb-columns 1 \
								--no-sync \
								--with-stats \
								--start-commit $counter2 \
								--commits $step \
								--append \
								&
				sleep 2
				pkill -$kill -f "admin stress"
				sleep 1
				((counter++))
done
# flush logs
./target/release/admin run \
				--nb-columns 1 \
				-d ./test_db \
				&
sleep 2
pkill -15 -f "admin run"
sleep 1
./target/release/admin check \
				--nb-columns 1 \
				--index-value \
				-d ./test_db
