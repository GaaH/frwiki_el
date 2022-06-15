A set of scripts to make an Entity Linking (EL) dataset from the Wikipedia corpus. It relies on Wikimedia Enterprise dumps.

## Prerequisites

Download the `<wiki name>-<date>-page.sql.gz`, `<wiki name>-<date>-redirect.sql.gz` and `<wiki name>-<date>-page_props.sql.gz`. These files will be used to compute the final page in a chain of redirections.
You can also download a Wikimedia Enterprise dump (see [here](https://dumps.wikimedia.org/other/enterprise_html/)).

The script `download_dumps.py` can be used to download the dumps.

## Building the corpus

### Downloading the Wikipedia pages

Pages are extracted from the Wikipedia Enterpise dump of the _Main_ namespace, that is to say the namespace 0. Hence, the dump file must contains _NS0_ in its name.
If you don’t want to download the whole dump, you can stream it and etract the html page with the command below.

```
python extract_wikipedia.py --url https://dumps.wikimedia.org/other/enterprise_html/runs/20220601/frwiki-NS0-20220601-ENTERPRISE-HTML.json.tar.gz --output output/wikipedia_pages.jsonl.gz
```

If you already downloaded a dump, the script also accepts a path to the dump file. Just replace the `--url` parameter with the `--input` one.

### Processing the html

Sometimes, links in wikipedia pages target _redirection pages_, wich are used as alias of the true article. For instance, the page _République Française_ redirect to _France_. We need to replace the redirection page by the target one, since we don’t want to label entities with the redirect pages. So, first, it is required to compute the whole redirection chaine for all pages.

```
python compute_redirects.py --input-redirect frwiki-20220601-redirect.sql.gz --input-page frwiki-20220601-page.sql.gz --input-props frwiki-20220601-page_props.sql.gz --output-page output/page.csv --output-redirect output/redirect.csv --output-all-redirect output/all_redirect.csv
```

Then, the pages can be processed and clean to remove the html tags.

```
python clean_html_pages.py --input output/wikipedia_pages.jsonl.gz --redirect output/all_redirect.csv --output output/wikipedia_cleaned_pages.jsonl.gz
```

And finally, the cleaned pages can be split into sentences

```
python sentencify.py --input output/wikipedia_cleaned_pages.jsonl.gz --output output/wikipedia_sentences.jsonl.gz --entities-output output/wikipedia_entities.jsonl.gz --lang fr
```