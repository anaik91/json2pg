import sys
data = open('test.log').read()
#print(data)
#sys.exit()
data = data.splitlines()
out = {}
targetip = '83.149.9.191'
for i in data:
    ip = i.split()[0]
    status = i.split()[8]
    if ip == targetip and status == '500':
        print(i)
    # if ip in out.keys():
    #     value = out[ip]
    #     value += 1
    #     out[ip] = value
    # else:
    #     out[ip] = 1

#print(out)
sorted_out = sorted(out.items(),reverse=True,key=lambda x: x[1])
print(sorted_out)