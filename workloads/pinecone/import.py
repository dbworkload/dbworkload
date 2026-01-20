import argparse
import pandas as pd
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
        description="Import CSV passages into a Pinecone index"
    )

    parser.add_argument(
        "-c", "--csv-path",
        required=True,
        help="Path to CSV file containing pid, passage columns",
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

    # --- load csv ---
    df = pd.read_csv(args.csv_path)

    # --- embedding model ---
    model = SentenceTransformer(EMBED_MODEL)

    # --- upsert ---
    for i in range(0, len(df), args.batch_size):
        batch = df.iloc[i:i + args.batch_size]

        texts = batch["passage"].astype(str).tolist()
        embeddings = embed(model, texts)

        vectors = [
            (
                str(pid),
                emb,
                {
                    "passage": passage
                }
            )
            for pid, emb, passage in zip(
                batch["pid"],
                embeddings,
                batch["passage"].astype(str).tolist()
            )
        ]

        index.upsert(vectors=vectors)

    print("Done.")


if __name__ == "__main__":
    main()
