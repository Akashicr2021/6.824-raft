rm -rf res/
mkdir res

for i in `seq 2`
do
	date +%F_%T
	go test > res/all.$i
	echo "iter $i: result is $?"
done
