# omgs_external_evidence

Processing code for OMGS external evidence assets.

Fixed date: `2025-10-29`

## Sources

PubMed:

- `https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/`
- `https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/`

FDA:

- `https://api.fda.gov/drug/label.json`
- `https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm`

Conferences:

- See `examples/conference_records_example.json` for the record shape.

## Environment

```bash
conda create -n omgs_external_evidence python=3.10 pip -y
conda activate omgs_external_evidence
PYTHONNOUSERSITE=1 python -m pip install -r requirements.txt
```

Alternative:

```bash
conda env create -f environment.yml
conda activate omgs_external_evidence
PYTHONNOUSERSITE=1 python -m pip install -r requirements.txt
```

## PubMed

```bash
bash scripts/01_download_pubmed_baseline.sh
bash scripts/02_download_pubmed_updatefiles.sh
bash scripts/03_build_pubmed_parquet.sh
```

Output:

- `data/processed/pubmed_mainline/gold.parquet`

PubMed filter:

- `MIN_PUB_DATE = "2015-10-29"`
- `MAX_PUB_DATE = "2025-10-29"`

## FDA

```bash
bash scripts/06_download_fda_openfda.sh
bash scripts/07_download_fda_dailymed.sh
bash scripts/08_build_fda_sqlite.sh
```

Output:

- `data/processed/fda/fda_effective_date_le_20251029.sqlite`

FDA labels are filtered to effective dates on or before `2025-10-29`.

## Conferences

Full conference records are not included in this release package.

See:

- `examples/conference_records_example.json`

## Checksums

```bash
bash scripts/09_compute_sha256_manifest.sh
```

Default checksum targets:

- `data/processed/pubmed_mainline/gold.parquet`
- `data/processed/fda/fda_effective_date_le_20251029.sqlite`
- `data/processed/conferences/ovarian_cancer_multiconference_2025_cutoff.json`

Conference JSON is a local verification target and is not included in this
release package. Raw downloads and embeddings are not checksum targets.

## PostgreSQL

```bash
cp .env.example .env
# Edit .env with your local PostgreSQL password.
set -a
source .env
set +a
bash scripts/05_upload_to_postgres.sh
```

## Scope

This repository releases processing code only. Retrieval, BM25 indexing,
reranking, and serving code are out of scope.

## Source Terms

This repository releases processing code and lightweight examples only. It does
not redistribute PubMed XML corpora, article full text, FDA/label source
packages, or full conference evidence records.

- PubMed: obtain files from NLM/NCBI and follow NLM terms. PubMed metadata,
  abstracts, and linked full text can have separate copyright or license terms.
- FDA/openFDA: openFDA data are generally made available under CC0/public-domain
  terms. DailyMed labels are company-submitted labeling and may include
  third-party content; check applicable source terms before redistribution.
- Conferences: meeting abstracts and related content may be copyrighted or
  access-restricted. Full conference records are not released here; use
  `examples/conference_records_example.json` only as a formatting template.
- Users are responsible for obtaining lawful access to source files and for
  complying with institutional, research, clinical, and commercial-use terms.

Official terms and reference pages:

- PubMed/NLM: `https://pubmed.ncbi.nlm.nih.gov/disclaimer/`
- openFDA: `https://open.fda.gov/terms/`
- DailyMed: `https://dailymed.nlm.nih.gov/dailymed/about-dailymed.cfm`
- ASCO permissions: `https://ascopubs.org/about/permissions`
- ESMO terms: `https://www.esmo.org/terms-of-use/website-terms-conditions/`
