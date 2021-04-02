cp filesample.txt data.txt

# generate 2^$1 times file filesample.txt
for ((n=0;n<$1;n++))
do
cat data.txt data.txt > temp
mv temp data.txt
done
