#!/bin/bash

cargo build --release --all-features
rm -r ./test_db
counter=1
iteration=10
step=30000
kill=15
while [ $counter -le $iteration ]
do
				echo "step $counter"
				counter2=$(($counter * $step))
				./target/release/admin stress \
								--nb-columns 1 \
								--no-sync \
								--with-stats \
								--start-commit $counter2 \
								--commits $step \
								--append \
								--archive \
								&
				sleep 3
				pkill -$kill -f "admin stress"
				sleep 2
				((counter++))
done
# flush logs
./target/release/admin run \
				--nb-columns 1 \
				-d ./test_db \
				&
sleep 2
pkill -15 -f "admin run"
sleep 2
./target/release/admin check \
				--nb-columns 1 \
				--index-value \
				-d ./test_db
