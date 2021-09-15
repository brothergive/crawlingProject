import datetime
start = datetime.datetime.now()
for i in range(10000):
    print(i)

print((start-datetime.datetime.now()).time())