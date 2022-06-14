from multiprocessing import Pool, Manager, cpu_count
import json
from scrapping import is_editsection, is_headline, is_infobox, is_section_to_remove, is_internal_link, is_editsection
from tqdm import tqdm
from bs4 import BeautifulSoup
import gzip
import itertools as it
import pandas as pd

def remove_non_text_tags(root):
    _tags_names_to_remove = {'sup', 'figure', 'table'}
    def _tag_to_remove_selector(t):
        
        return is_headline(t) or is_editsection(t) or is_infobox(t) or t.name in _tags_names_to_remove

    # Rm sections
    for t in root.find_all(is_section_to_remove):
        for t2 in t.find_all_next():
            t2.extract()
        t.extract()
    for t in root.find_all(_tag_to_remove_selector):
        t.extract()

    # p_tags = root.find_all("p", recursive=False)
    # for p in p_tags:
    #     for t in p.find_all("sup"):
    #         t.extract()


def replace_math_elements(root):
    for t in root.select("span.mwe-math-element"):
        if t.img is not None:
            alt = t.img.attrs["alt"].replace('\\\\', '\\').strip()
            if alt.startswith('{'):
                alt = alt[1:]
            if alt.endswith('}'):
                alt = alt[:-1]
            t.replace_with(alt)
        else:
            t.replace_with('')
            
def clean_html(html, page_title, title2target):
    soup = BeautifulSoup(html, "lxml")

    body = soup.select("body")[0]

    remove_non_text_tags(body)    
    replace_math_elements(body)
    
    # p_tags = body.find_all("p", recursive=True)
    # for p in p_tags:
    bold = body.find('b')
    if bold is not None:
        entity_name = page_title.replace('_', ' ')
        bold.replace_with(f"[E={entity_name}]{bold.text}[/E]")
    
    for t in body.find_all(is_internal_link):
        link_text = t.text
        target_entity = t["title"].replace("_", " ")
        target_entity = title2target.get(target_entity, target_entity)
        t.replace_with(f"[E={target_entity}]{link_text}[/E]")

    # content = "\n".join(p.text for p in body).strip()
    # return content
    return body.text.strip()

def worker(args):
    line, title2target = args
    page = json.loads(line)
    page['text'] = clean_html(page['text'], page['name'], title2target)
    return page


if __name__ == "__main__":
    
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-r', '--redirect')
    parser.add_argument('-o', '--output')
    parser.add_argument('--nproc', type=int, default=cpu_count() - 1)
    
    args = parser.parse_args()
    
    df_redirect = pd.read_csv(args.redirect, encoding='utf-8')
    manager = Manager()
    title2target = manager.dict(zip(df_redirect['source'], df_redirect['target']))
    del df_redirect
    
    with Pool(processes=args.nproc) as pool:
        with gzip.open(args.input, 'rt', encoding='utf-8') as inf, gzip.open(args.output, 'wt', encoding='utf-8') as outf:
            for page in tqdm(pool.imap_unordered(worker, zip(inf, it.repeat(title2target)))):
                json.dump(page, outf)
                outf.write('\n')