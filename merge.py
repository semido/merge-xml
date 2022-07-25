import os
import zipfile
from lxml import etree as etree
import concurrent.futures
from multiprocessing import Process, Pool, Pipe, Queue
from itertools import repeat
import timeit

'''extract vars and objects'''
def parse_xml(bytes):
    root = etree.fromstring(bytes)
    vrs = {e.get('name', ''): e.get('value', '') for e in root.findall('var')}
    objects = [e.get('name', '') for e in root.findall('.//object')]
    return (vrs['id'], vrs['level'], objects)

'''load one zip and parse all xmls from that'''
def read_zip1(zipname):
    alldata = []
    with zipfile.ZipFile(zipname, 'r') as z:
        names = z.namelist()
        alldata = [parse_xml(z.read(name)) for name in names]
    return alldata

'''load one zip and parse all xmls from that, return result in param'''
def read_zip2(zipname, alldata):
    with zipfile.ZipFile(zipname, 'r') as z:
        names = z.namelist()
        alldata += [parse_xml(z.read(name)) for name in names]

'''sequentially read all zips'''
def read_zips(zips):
    alldata = []
    [read_zip2(name, alldata) for name in zips]
    return alldata

'''process pool and futures'''
def read_zips_par_pool(zips):
    alldata = []
    chunk = -(-len(zips)//os.cpu_count())
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(read_zips, zips[s:s+chunk],) for s in range(0, len(zips), chunk)]
        for future in concurrent.futures.as_completed(futures):
            alldata += future.result()
    return alldata

def read_zips_pipes(pi_parent, pi_cli, zips):
    #pi_parent.close()
    pi_cli.send(read_zips(zips))
    pi_cli.close()

'''processes and pipes'''
def read_zips_par_pipes(zips):
    chunk = -((-2*len(zips))//os.cpu_count())
    packs = [zips[s:s+chunk] for s in range(0, len(zips), chunk)]
    pipes = [Pipe() for p in packs]
    procs = [Process(target=read_zips_pipes, args=(pp, pc, z)) for (pp, pc), z in zip(pipes, packs)]
    [p.start() for p in procs]
    alldata = []
    for (pp,pc) in pipes:
        alldata += pp.recv()
    [p.join() for p in procs]
    return alldata

'''read zips and put into queue'''
def read_zips_to_1queue(zips, q):
    q.put(read_zips(zips))

'''direct processes and 1 queue'''
def read_zips_par_1queue(zips):
    chunk = -(-len(zips)//os.cpu_count())
    queue = Queue()
    procs = [Process(target=read_zips_to_1queue, args=(zips[s:s+chunk], queue)) for s in range(0, len(zips), chunk)]
    [p.start() for p in procs]
    alldata = []
    for p in procs:
        alldata += queue.get()
    return alldata

'''format merged tables'''
def make_merged_tables(alldata, tname1, tname2):
    with open(tname1, 'w') as t1, open(tname2, 'w') as t2:
        for line in alldata:
            t1.write(line[0] + ',' + line[1] + '\n')
            for obj in line[2]:
                t2.write(line[0] + ',' + obj + '\n') 

def scan_dir(d):
    return [e.name for e in os.scandir(d) if e.is_file() and e.name.casefold().endswith('.zip')]

def do_all_seq():
    make_merged_tables(read_zips(scan_dir('.')), 'id-level-seq.csv', 'id-object-seq.csv')

def do_all_pool():
    make_merged_tables(read_zips_par_pool(scan_dir('.')), 'id-level-par1.csv', 'id-object-par1.csv')

def do_all_pipes():
    make_merged_tables(read_zips_par_pipes(scan_dir('.')), 'id-level-par2.csv', 'id-object-par2.csv')

def do_all_queue():
    make_merged_tables(read_zips_par_1queue(scan_dir('.')), 'id-level-par3.csv', 'id-object-par3.csv')

if __name__ == '__main__':
    testdir = 'test'
    if not os.path.isdir(testdir):
        print('work dir not found:', testdir)
        exit(1)
    os.chdir(testdir)
    import timeit
    setups = ['do_all_seq', 'do_all_pool', 'do_all_pipes', 'do_all_queue']
    times = 5
    for s in setups:
        t = min([timeit.timeit("f()", setup='from __main__ import '+s+' as f', number=1) for _ in range(times)])
        print('call', s, 'repeat', times, 'best', t, 'sec')
    os.chdir('..')
