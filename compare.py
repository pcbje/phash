
cache = {}
with open('res-sd.txt') as inp:
    for line in inp:
        a, b, c = line.strip().split('|')
        cache[(a[2::].split('.')[0], b[2::].split('.')[0])] = int(c)

hits = {}
miss = {}
i = 0
m = 0
n = 0
with open('res.txt') as inp:
    for line in inp:
        parts = line.split()
        a = parts[0].split('.')[0]
        b = parts[2].split('.')[0]
        c = parts[4]

        if (a, b) in cache or (b, a) in cache:
            if (a, b) in cache:
                hits[(a,b)] = int(c)
                print "<",  cache[(a, b)]
                i+=1
                del cache[(a, b)]

            if (b, a) in cache:
                hits[(b,a)] = int(c)
                print ">", cache[(b, a)]
                i+=1
                del cache[(b, a)]

        else:
            m += 1

for p, c in cache.items():
    if int(c) > 50:
        #print p, c
        pass
        n += 1

print i,m ,n
