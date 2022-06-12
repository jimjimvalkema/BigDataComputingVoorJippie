import multiprocessing as mp
from time import sleep
from random import randint


def todo(p, outputQ):
    sleeptime = randint(1, 5)
    sleep(sleeptime)
    outputQ.put("process {} slept {} seconds".format(p, sleeptime)) # gedeelde resource


processes = []
outputQ = mp.Queue()

for p in range(5):
    temP = mp.Process(target=todo, args=(p, outputQ))
    processes.append(temP)
    temP.start()

for p in processes:
    p.join() # check dat process klaar is en sluit ze af om te verkomen dat data corrupt

while not outputQ.empty():
    msg = outputQ.get()
    print(msg)