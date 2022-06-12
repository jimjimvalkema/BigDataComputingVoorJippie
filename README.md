# quick test
server  
``scp -P 4222  opdracht2.py jjvalkema@bioinf.nl:~/opdracht2.py ; ssh jjvalkema@bioinf.nl -p 4222 "ssh assemblix "python3 opdracht2.py -f rnaseq_smol.fastq -n 16 -w 10 -b 1000 -p 4223 --hosts assemblix -s""``

client  
``scp -P 4222  opdracht2.py jjvalkema@bioinf.nl:~/opdracht3.py ; ssh jjvalkema@bioinf.nl -p 4222 "ssh bin212 "python3 opdracht2.py -p 4223 -H assemblix -c""``

# test opdracht 3
``scp -P 4222  opdracht3.py jjvalkema@bioinf.nl:~/opdracht3.py ; ssh jjvalkema@bioinf.nl -p 4222 "ssh bin205 "python3 opdracht3.py -f rnaseq_smol.fastq -n 16 -w 5 -b 1000 -p 4728 --portrange 4728 8100 --hosts bin205 bin206 -s""``

at home  
``scp opdracht3.py ethereum@192.168.1.7:~/opdracht3.py ; ssh ethereum@192.168.1.7 "ssh 192.168.1.2 python3 opdracht3.py -f rnaseq_smol.fastq -n 16 -w 5 -b 1000 -p 4728 --portrange 4728 8100 --hosts 192.168.1.7 192.168.1.2 -s"``