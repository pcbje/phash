import os
import hashlib
import json

out = 't5-gold'
if not os.path.exists(out):
    os.makedirs(out)
files = []
root = '/Users/pcbje/Downloads/t5'
for p in os.listdir(root):
    if p.startswith('.'):
        continue

    hashes = []
    with open(os.path.join(root, p), 'rb') as inp:
        data = inp.read()
        for i in range(64, len(data)):
            m = hashlib.sha1()
            m.update(data[i-64:i])
            hashes.append([i, m.hexdigest()[-12::]])

    with open(os.path.join(out, '%s.sha1.txt' % p), 'w') as o:
        o.write(json.dumps(hashes))

    print p
