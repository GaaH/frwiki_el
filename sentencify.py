from multiprocessing import Pool, cpu_count
from pathlib import Path
import json
from tqdm import tqdm
from sentence_splitter import SentenceSplitter, split_text_into_sentences
import re
import itertools as it
import gzip

def remove_tagged_entities(txt):
    return re.sub(r'\[E=.+?\](.+?)\[/E\]', r'\1', txt)

def word_count(sent):
    return remove_tagged_entities(sent).count(' ')

def worker(args):
    line, lang = args
    page = json.loads(line)
    sentences = split_text_into_sentences(text=page['text'], language=lang)
    
    if len(sentences) == 0:
        return None

    description = remove_tagged_entities(sentences[0])
    entity = {
        'wikipedia_id': page['wikipedia_id'],
        'wikidata_id': page['wikidata_id'],
        'name': page['name'],
        'description': description,
        'wikipedia_url': page['wikipedia_url'],
        'wikidata_url': page['wikidata_url'],
    }
    
    return entity, page, sentences

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input')
    parser.add_argument('-o', '--output')
    parser.add_argument('-e', '--entities-output')
    parser.add_argument('-l', '--lang', default='fr')
    parser.add_argument('--nproc', type=int, default=cpu_count() - 1)
    
    args = parser.parse_args()
    
    inp_file = Path('output', 'wikipedia_cleaned_pages.jsonl.gz')
    out_file = Path('output', 'wikipedia_sentences.jsonl.gz')
    entities_out_file = Path('output', 'wikipedia_entities.jsonl.gz')
    
    splitter = SentenceSplitter(language='fr')
    with gzip.open(args.input, 'rt', encoding='utf-8') as inf, gzip.open(args.output, 'wt', encoding='utf-8') as outf, gzip.open(args.entities_output, 'wt', encoding='utf-8') as ent_outf:
        with Pool(processes=args.nproc) as pool:
            for ret in tqdm(pool.imap_unordered(worker, zip(inf, it.repeat(args.lang)))):
                if ret is not None:
                    entity, page, sentences = ret
                    json.dump(entity, ent_outf)
                    ent_outf.write('\n')
                    
                    for sent in sentences:
                        if word_count(sent) >= 16:
                            page['text'] = sent
                            json.dump(page, outf)
                            outf.write('\n')
