
cache = {}
with open('res-sd.txt') as inp:
    for line in inp:
        a, b, c = line.strip().split('|')
        cache[(a[2::], b[2::])] = int(c)

hits = {}
miss = {}
with open('res.txt') as inp:
    for line in inp:
        parts = line.split()
        a = parts[0]
        b = parts[2]
        c = parts[4]

        if (a, b) in cache or (b, a) in cache:
            if (a, b) in cache:
                hits[(a,b)] = int(c)
                print cache[(a, b)]
                del cache[(a, b)]

            if (b, a) in cache:
                hits[(b,a)] = int(c)
                print cache[(b, a)]
                del cache[(b, a)]

        else:
            print "!!"


for p, c in hits.items():
    #if int(c) > 50:
    print p, c
