from multiprocessing import Pool, Manager, cpu_count
import requests
import gzip
import json
import itertools as it
import tarfile
from tqdm import tqdm

def wikipedia_pages_from_tarfile(tar):
    for tarinfo in tar:
        if tarinfo.isfile():
            f = tar.extractfile(tarinfo)
            for lb in f:
                l = lb.decode('utf-8')
                obj = json.loads(l)
                yield obj

def wikipedia_pages_from_path(path):
    with tarfile.open(path, mode='r|gz') as tar:
        for page in wikipedia_pages_from_tarfile(tar):
            yield page

def wikipedia_pages_from_url(url):
    with requests.get(url, stream=True) as r:
            with tarfile.open(fileobj=r.raw, mode='r|gz') as tar:
                for page in wikipedia_pages_from_tarfile(tar):
                    yield page

def worker(page):
    wikidata_id = None
    wikidata_url = None
    if 'main_entity' in page:
        wikidata_id = page['main_entity']['identifier']
        wikidata_url = page['main_entity']['url']
        
    o = {
        'wikipedia_id': page['identifier'],
        'wikidata_id': wikidata_id,
        'name': page['name'],
        'wikipedia_url': page['url'],
        'wikidata_url': wikidata_url,
        'text': page['article_body']['html'],
    }
    return o

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='Path to Wikimedia Enterprise dump file.')
    parser.add_argument('-u', '--url', help='URL to a Wikimedia Enterprise dump. Overrides --input')
    parser.add_argument('-o', '--output')
    parser.add_argument('-L', '--limit', type=int, help='Number of pages to process')
    parser.add_argument('--nproc', type=int, default=cpu_count() - 1)
    parser.add_argument('--disable-tqdm', action='store_true')

    args = parser.parse_args()
    
    # wikipedia_path = Path('D:/', 'Documents', 'caillaut', 'BRGM', 'CARNOT19_03 GEOTWEET4CATNAT - 04-Données Scientifiques du Projet', 'Data', 'wikidumps', 'frwiki-NS0-20220601-ENTERPRISE-HTML.json.tar.gz')
    # wikipedia_out = Path('output', 'wikipedia_pages.jsonl.gz')
    
    if args.url is not None:
        pages_iterator = wikipedia_pages_from_url(args.url)
    else:
        pages_iterator = wikipedia_pages_from_path(args.input)
    
    with gzip.open(args.output, 'wt', encoding='utf-8') as outf:
        with Pool(processes=args.nproc) as pool:
            for obj in tqdm(it.islice(pool.imap_unordered(worker, pages_iterator), args.limit), disable=args.disable_tqdm):
                # wikidata_id = None
                # wikipedia_url = None
                # if 'main_entity' in page:
                #     wikidata_id = page['main_entity']['identifier']
                #     wikidata_url = page['main_entity']['url']
                # o = {
                #     'wikipedia_id': page['identifier'],
                #     'wikidata_id': wikidata_id,
                #     'name': page['name'],
                #     'wikipedia_url': page['url'],
                #     'wikidata_url': wikidata_url,
                #     'text': page['article_body']['html'],
                # }
                json.dump(obj, outf)
                outf.write('\n')
                
        