
cache = {}
with open('res-sd.txt') as inp:
    for line in inp:
        a, b, c = line.strip().split('|')
        cache[(a[2::], b[2::])] = int(c)

hits = {}
miss = {}
with open('res-pb.txt') as inp:
    for line in inp:
        parts = line.split()
        a = parts[0]
        b = parts[2]
        c = parts[4]

        if (a, b) in cache or (b, a) in cache:
            if (a, b) in cache:
                hits[(a,b)] = int(c)
                del cache[(a, b)]

            if (b, a) in cache:
                hits[(b,a)] = int(c)
                del cache[(b, a)]

        else:
            pass


for p, c in cache.items():
    print p, c
