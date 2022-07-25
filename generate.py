import os 
import string
import random
import base64
import math
from more_itertools import take, unique_everseen
import zipfile
import timeit

'''get a key, easy version'''
def gen_keys0(sz=16):
    while True:
        yield ''.join(random.choices(string.ascii_letters, k=sz))

'''get a key, involving base64'''
def gen_keys(sz=16):
    # random.randbytes is slower
    bsz = int((sz + 1) / math.log(256, 64)) # min number of bytes
    while True:
        yield base64.urlsafe_b64encode(os.urandom(bsz))[:sz].decode('ascii')

'''build a set of unique keys, easy version'''
def make_unique_keys(amount):
    keys = set()
    while len(keys) < amount:
        keys |= {next(gen_keys0(16)) for _ in range(amount - len(keys))}
    return keys

'''build a set of unique keys, work fast with itertools'''
def make_unique_keys_it(amount, sz=16):
    return take(amount, unique_everseen(gen_keys(sz)))

'''build a random xml with id'''
def make_xml(key):
    level = random.randint(1, 100)
    content = f'<root><var name="id" value="{key}"/><var name="level" value="{level}"/><objects>'
    n = random.randint(1, 10)
    for _ in range(n):
        content += '<object name="' + next(gen_keys(16)) + '"/>'
    content += '</objects></root>'
    return content

'''build a set of xml by list of keys, pack to zip'''
def make_zip(i, keys):
    zipname = str(i) + '.zip'
    with zipfile.ZipFile(zipname, 'w') as izip:
        for k in keys:
            izip.writestr(k + '.xml', make_xml(k))

def make_all():
    zips_amount = 50
    xmls_amount = 1000
    keys = make_unique_keys_it(zips_amount * xmls_amount)
#    with open('keys.txt', 'w') as fk:
#        fk.write('\n'.join(keys))
    for i in range(zips_amount):
        make_zip(i, keys[i*xmls_amount:(i+1)*xmls_amount])

if __name__ == '__main__':
    testdir = 'test'
    if os.path.isdir(testdir):
        print('clear temporary work dir:', testdir)
        os.rmdir(testdir)
    else:
        os.mkdir(testdir)
    os.chdir(testdir)
    import timeit
    print(timeit.timeit("g()", setup='from __main__ import make_all as g', number=1))
#    print(timeit.timeit("k(5000)", setup='from __main__ import make_unique_keys as k', number=100))
#    print(timeit.timeit("k(5000)", setup='from __main__ import make_unique_keys_it as k', number=100))
    os.chdir('..')
