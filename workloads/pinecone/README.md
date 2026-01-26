# load-msmarco.py

## Overview

`load-msmarco.py` ingests the **MS MARCO v2 Passage Ranking dataset** into an existing Pinecone index. The script streams gzipped JSONL files, embeds each passage using a SentenceTransformers model (`all-MiniLM-L6-v2`, 384 dimensions), and upserts the resulting vectors into Pinecone in configurable batches.

The script is designed to be memory-efficient and supports resuming ingestion via a line-offset (`--skip`).

## Dataset: MS MARCO (Passage Ranking)

**MS MARCO (Microsoft MAchine Reading COmprehension)** is a large-scale dataset commonly used for training and evaluating information retrieval and semantic search systems. The passage ranking subset contains short text passages extracted from web documents, each identified by a stable passage ID.

Project page (background and documentation): [https://microsoft.github.io/msmarco/](https://microsoft.github.io/msmarco/)

## Download

The MS MARCO v2 passage dataset can be downloaded as a tar archive:

[https://msmarco.z22.web.core.windows.net/msmarcoranking/msmarco_v2_passage.tar](https://msmarco.z22.web.core.windows.net/msmarcoranking/msmarco_v2_passage.tar)

## Extracting the dataset

After downloading, extract the archive:

```bash
tar -xvf msmarco_v2_passage.tar
```

This creates a directory named `msmarco_v2_passage/` containing multiple gzipped files:

```text
msmarco_v2_passage/
├── msmarco_passage_00.gz
├── msmarco_passage_01.gz
├── msmarco_passage_02.gz
├── msmarco_passage_03.gz
├── msmarco_passage_04.gz
├── msmarco_passage_05.gz
├── msmarco_passage_06.gz
└── msmarco_passage_07.gz
```

Each `.gz` file contains roughly **2 million JSONL records**.

## File format

Each gzipped file is a UTF-8 encoded **JSON Lines** file. Each line represents a single passage and has the following structure:

```json
{
  "pid": "msmarco_passage_00_0",
  "passage": "0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews.",
  "spans": "(0,75)",
  "docid": "msmarco_doc_00_0"
}
```

Relevant fields used by the script:

`pid`: unique passage identifier (used as the vector ID)

`passage`: passage text (embedded and indexed)

Other fields (`spans`, `docid`) are ignored by the loader.

## Preparing for ingestion

Before running `load-msmarco.py`, ensure that:

1. A Pinecone index already exists.
2. The index dimension is **384**, matching the embedding model.
3. The index similarity metric is compatible with normalized sentence embeddings (commonly cosine or dot-product).

Each `.gz` file can be ingested independently by pointing the script at the file path. This allows parallel or incremental loading across multiple runs.

## Usage

The script operates on **one MS MARCO `.gz` file at a time** and processes the file incrementally in fixed-size batches.

### Batch loading behavior

* Passages are read sequentially from the compressed file.
* Records are accumulated into batches (default: 1000 passages).
* Each batch is embedded and upserted into the Pinecone index before continuing.

This streaming approach keeps memory usage bounded even for large files (~2M passages per file).

### Progress reporting

After each successful batch upsert, the script prints the **current line number** in the input file:

```text
[line 1583000]
[line 1584000]
[line 1585000]
[line 1586000]
```

This output can be used to monitor ingestion progress and identify a safe restart point if the process is interrupted.

### Restarting a failed or interrupted load

Because each `.gz` file is large and ingestion can take a long time, failures may occur (e.g., network issues or process restarts).

The `--skip` option allows resuming ingestion from a specific line number:

`--skip N` causes the script to ignore the first `N` lines of the file.

Use the **last printed line number** as the restart offset.

This makes it possible to resume loading without reprocessing already-ingested passages.

### CLI options summary

The script accepts the following arguments:

`--msmarco-path`: path to a single `msmarco_passage_*.gz` file

`--index-name`: target Pinecone index name

`--api-key`: Pinecone API key

`--batch-size`: number of passages per upsert (default: 1000)

`--skip`: number of initial lines to skip when starting
