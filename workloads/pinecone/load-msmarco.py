import argparse
import gzip
import json
from pinecone import Pinecone
from pinecone.db_data.index import Index
from sentence_transformers import SentenceTransformer


# --- config ---
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"  # dim = 384


def embed(model: SentenceTransformer, texts: list[str]) -> list[list[float]]:
    embeddings = model.encode(texts)
    return [emb.tolist() for emb in embeddings]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import MS MARCO passages (.gz, JSONL) directly into a Pinecone index"
    )

    parser.add_argument(
        "-m", "--msmarco-path",
        required=True,
        help="Path to msmarco_passage_*.gz file",
    )

    parser.add_argument(
        "-i", "--index-name",
        required=True,
        help="Pinecone index name",
    )

    parser.add_argument(
        "-k", "--api-key",
        required=True,
        help="Pinecone API key",
    )

    parser.add_argument(
        "-b", "--batch-size",
        type=int,
        default=100,
        help="Batch size for upserts (default: 100)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # --- clients ---
    pc = Pinecone(api_key=args.api_key)
    index: Index = pc.Index(args.index_name)

    # --- embedding model ---
    model = SentenceTransformer(EMBED_MODEL)

    batch_ids: list[str] = []
    batch_texts: list[str] = []

    # --- stream MS MARCO .gz ---
    with gzip.open(args.msmarco_path, "rt", encoding="utf-8") as fin:
        for line_num, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                pid = record["pid"].replace("msmarco_passage_", "")
                passage = record["passage"]
            except Exception as e:
                raise RuntimeError(
                    f"Failed to parse line {line_num} in {args.msmarco_path}: {e}"
                ) from e

            batch_ids.append(pid)
            batch_texts.append(passage)

            if len(batch_ids) >= args.batch_size:
                embeddings = embed(model, batch_texts)

                vectors = [
                    (
                        pid,
                        emb
                    )
                    for pid, emb in zip(batch_ids, embeddings)
                ]

                index.upsert(vectors=vectors)

                batch_ids.clear()
                batch_texts.clear()

    # --- flush remainder ---
    if batch_ids:
        embeddings = embed(model, batch_texts)

        vectors = [
            (
                pid,
                emb
            )
            for pid, emb in zip(batch_ids, embeddings)
        ]

        index.upsert(vectors=vectors)

    print("Done.")


if __name__ == "__main__":
    main()
