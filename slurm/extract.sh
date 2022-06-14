#!/bin/bash

#SBATCH -n 32
#SBATCH -p cpu
#SBATCH --job-name wikipedia
#SBATCH --time 5-0
#SBATCH -o slurm/logs/log-%j.txt
#SBATCH -e slurm/logs/err-%j.txt
#SBATCH --mail-type=ALL
#SBATCH --mail-user=g.caillaut@brgm.fr

SCRATCH=/scratch/$USER/$SLURM_JOB_ID

srun -m arbitrary -w $SLURM_NODELIST mkdir -p $SCRATCH
srun -m arbitrary -w $SLURM_NODELIST cp -r $SLURM_SUBMIT_DIR/* $SCRATCH
cd $SCRATCH

eval "$(conda shell.bash hook)"
conda activate geoloc

DUMPS_DIR="wikidumps"
OUT_DIR="output"
WIKI="frwiki"
DATE="20220601"
NPROC=31

python download_dumps.py --wiki ${WIKI} --date ${DATE} --outdir ${OUT_DIR}

python extract_wikipedia.py \
    --url https://dumps.wikimedia.org/other/enterprise_html/runs/${DATE}/${WIKI}-NS0-${DATE}-ENTERPRISE-HTML.json.tar.gz \
    --output "${OUT_DIR}/wikipedia_pages.jsonl.gz" \
    --nproc ${NPROC}

python compute_redirects.py --input-redirect "${DUMP_DIR}/frwiki-20220601-redirect.sql.gz" \
    --input-page "${DUMP_DIR}/frwiki-20220601-page.sql.gz" \
    --input-props "${DUMP_DIR}/frwiki-20220601-page_props.sql.gz" \
    --output-page "${OUT_DIR}/page.csv" \
    --output-redirect "${OUT_DIR}/redirect.csv" \
    --output-props "$OUT_DIR/page_props.csv" \
    --output-all-redirect "${OUT_DIR}/all_redirect.csv"


python clean_html_pages.py \
    --input "${OUT_DIR}/wikipedia_page.jsonl.gz" \
    --redirect "${OUT_DIR}/all_redirect.csv" \
    --output "${OUT_DIR}/wikipedia_cleaned_pages.jsonl.gz" \
    --entities-output "${OUT_DIR}/wikipedia_entities.jsonl.gz" \
    --nproc ${NPROC}

python sentencify.py \
    --input "${OUT_DIR}/wikipedia_cleaned_pages.jsonl.gz" \
    --output "${OUT_DIR}/wikipedia_sentences.jsonl.gz" \
    --entities-output "${OUT_DIR}/wikipedia_entities.jsonl.gz" \
    --lang fr \
    --nproc ${NPROC}


srun -m arbitrary -w $SLURM_NODELIST cp -r ${DUMP_DIR} ${OUT_DIR} ${SLURM_SUBMIT_DIR}
srun -m arbitrary -w $SLURM_NODELIST rm -rf $SCRATCH