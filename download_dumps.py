import requests
from pathlib import Path
from tqdm import tqdm

def download_file(url, outpath):
    outpath = Path(outpath)
    # Streaming, so we can iterate over the response.
    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 8192
    progress_bar = tqdm(desc=outpath.name, total=total_size_in_bytes, unit='iB', unit_scale=True)
    try:
        with open(outpath, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR, something went wrong")
    except KeyboardInterrupt as e:
        if outpath.is_file():
            outpath.unlink()
        raise e
    except Exception as e:
        if outpath.is_file():
            outpath.unlink()
        raise e
            
def download_file_if_not_exists(url, outpath):
    outpath = Path(outpath)
    if not outpath.is_file():
        download_file(url, outpath)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--wiki', default='frwiki')
    parser.add_argument('--date', default='20220601')
    parser.add_argument('--dl-html-dump', action='store_true', help='Also download the Wikimedia Enterprise dump file')
    parser.add_argument('--mirror', default='https://dumps.wikimedia.org/')
    parser.add_argument('--outdir', default='wikidumps')
    
    args = parser.parse_args()
    
    outdir = Path(args.outdir)
    outdir.mkdir(exist_ok=True, parents=True)
    
    wiki, date = args.wiki, args.date
    
    base_url = f'{args.mirror}/{wiki}/{date}'
    filenames = [
        f'{wiki}-{date}-page_props.sql.gz',
        f'{wiki}-{date}-page.sql.gz',
        f'{wiki}-{date}-redirect.sql.gz',
    ]
    
    urls = [f'{base_url}/{filename}' for filename in filenames]
    
    if args.dl_html_dump:
        html_filename = f'{wiki}-NS0-{date}-ENTERPRISE-HTML.json.tar.gz'
        filenames.append(html_filename)
        urls.append(f'{args.mirror}/other/enterprise_html/runs/{date}/{html_filename}')
    
    try:
        for filename, url in zip(filenames, urls):
            outpath = Path(outdir, filename)
            download_file_if_not_exists(url, outpath)
    except KeyboardInterrupt:
        pass