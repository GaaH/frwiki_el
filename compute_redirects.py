import gzip
import re
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import csv

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input-redirect')
    parser.add_argument('--output-redirect')
    parser.add_argument('--input-page')
    parser.add_argument('--output-page')
    parser.add_argument('--output-all-redirect')
    parser.add_argument('--input-props')
    parser.add_argument('--output-props')

    args = parser.parse_args()

    redirect_path = Path(args.input_redirect)
    page_path = Path(args.input_page)
    props_path = Path(args.input_props)
    
    df_props_path = Path(args.output_props)
    if df_props_path.is_file():
        df_props = pd.read_csv(df_props_path, encoding='utf-8', dtype=object)
    else:
        header = 'INSERT INTO `page_props` VALUES ('
        cols = {'page_id': [], 'wikidata_id': []}
        with gzip.open(props_path, 'rt', encoding='cp850') as f:
            
            for line in tqdm(f):
                if not line.startswith(header):
                    continue
                    
                items = line[len(header):].split('),(')
                for x in items:
                    m = re.match(r'(\d+),\'(.+?)\',\'(.*?)\',.+', x)
                    if m is None:
                        print(x)
                    prop_name = m[2]
                    
                    if prop_name == 'wikibase_item':
                        wikipedia_id = m[1]
                        prop_value = m[3]
                        cols['page_id'].append(wikipedia_id)
                        cols['wikidata_id'].append(prop_value)
        df_props = pd.DataFrame(cols)
        df_props.to_csv(df_props_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')

    df_pages_path = Path(args.output_page)
    if df_pages_path.is_file():
        df_pages = pd.read_csv(df_pages_path, encoding='utf-8', dtype=object)
    else:
        with gzip.open(page_path, 'rt', encoding='utf-8') as f:
            header = 'INSERT INTO `page` VALUES ('
            cols = {'page_id': [], 'page_title': [], 'is_redirect': []}
            
            for line in tqdm(f):
                if not line.startswith(header):
                    continue
                    
                items = line[len(header):].split('),(')
                for x in items:
                    m = re.match(r'(\d+),(.+),\'(.+?)\',(\d),.+', x)
                    ns = m[2]
                    # Skip non main ns
                    if ns != '0':
                        continue
                    id = m[1]
                    title = m[3].replace('\\', '').replace('_', ' ')
                    is_redirect = int(m[4]) == 1
                    cols['page_id'].append(id)
                    cols['page_title'].append(title)
                    cols['is_redirect'].append(is_redirect)
                
        df_pages = pd.DataFrame(cols)
        df_pages = df_pages.merge(df_props, left_on='page_id', right_on='page_id', how='left')
        df_pages.to_csv(df_pages_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')
        
    df_redirect_path = Path(args.output_redirect)
    if df_redirect_path.is_file():
        df_redirect = pd.read_csv(df_redirect_path, encoding='utf-8', dtype=object)
    else:
        with gzip.open(redirect_path, 'rt', encoding='utf-8') as f:
            header = 'INSERT INTO `redirect` VALUES ('
            cols = {'source': [], 'target': []}
            for line in tqdm(f):
                if not line.startswith(header):
                    continue
                    
                items = line[len(header):].split('),(')
                for x in items:
                    m = re.match(r'(\d+),.+,\'(.+?)\',.+?,.+?', x)
                    src_page = m[1]
                    dest_page = m[2].replace('\\', '').replace('_', ' ')
                    cols['source'].append(src_page)
                    cols['target'].append(dest_page)
                
        df_redirect = pd.DataFrame(cols)
        df_redirect = df_redirect.merge(df_pages[['page_id','page_title']], left_on='source', right_on='page_id')
        df_redirect = df_redirect.loc[df_redirect['page_title'] != df_redirect['target']]
        df_redirect = df_redirect.drop(columns=['source'])
        df_redirect.to_csv(df_redirect_path, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')

    src2target = dict(zip(df_redirect['page_title'], df_redirect['target']))

    def find_last_target(title, src2target):
        target = src2target.get(title, title)
        while title != target:
            title = target
            target = src2target.get(title, title)
        return target

    all_redirect_cols = {
        'source': [],
        'target': []
    }
    for title, is_redirect in tqdm(zip(df_pages['page_title'], df_pages['is_redirect']), total=len(df_pages)):
        target = find_last_target(title, src2target) if is_redirect else title   
        all_redirect_cols['source'].append(title)
        all_redirect_cols['target'].append(target)

    df_all_redirect = pd.DataFrame(all_redirect_cols)
    df_all_redirect.to_csv(Path(args.output_all_redirect), index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')