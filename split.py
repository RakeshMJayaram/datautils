import gzip
import os
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
import argparse

files = []
compress_counter = 0


def compress_file(filepath):
    inf = open(filepath, "rb")
    outf = gzip.open(filepath+".gz", "wb")
    outf.write(inf.read())
    outf.close()
    inf.close()
    os.remove(filepath)
    compress_counter +=1
    print(f' {compress_counter} of {files.size} compressed...')
    
def split_file(infilepath, chunksize ,file_has_header):
    fname, ext = infilepath.rsplit('.',1)
    i = 0
    written = False
    with open(infilepath) as infile:
        if(file_has_header ) :
            header = infile.readline()
            print(header)
        while True:
            outfilepath = "{}{}{}.{}".format(fname,"_",i, ext)
            files.append(outfilepath)
            print(f'file {outfilepath} is currently saved...')
            with open(outfilepath, 'wb') as outfile:
                if(file_has_header ) :
                    outfile.write(header.encode())
                for line in (infile.readline() for _ in range(chunksize)):
                    outfile.write(line.encode())
                written = bool(line)
            if not written:
                break
            i += 1
if __name__ == '__main__': 
 
    parser = argparse.ArgumentParser(description='Process schema validation.')
    parser.add_argument('--filename',help='file name to split and compress')
    parser.add_argument('--file_has_header',help='boolean of file has header', default = True)
    parser.add_argument('--uncompressed_size',help='Approximate uncompressed size of each file (bytes)', default =1000000)
    parser.add_argument('--thread_size',help='Number of threads', default = 8)
    args = parser.parse_args() 
    
    print("*************************************************")
    print("               SPLIT AND COMPRESS                ") 
    print("*************************************************")
    
    print("Process Start Time : " + str(datetime.now(tz=None)))
    split_file(args.filename,args.uncompressed_size , args.file_has_header)
    print(f'split process complete Time : ' + str(datetime.now(tz=None)))

    print(f'gz process start Time : ' +   str(datetime.now(tz=None)))
    pool = ThreadPool(args.thread_size)
    results = pool.map(compress_file, files)
    pool.close()
    pool.join()
    
    
    print("Process End Time : " + str(datetime.now(tz=None)))
    print("*************************************************")