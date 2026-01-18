import pandas as pd
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer


# --- config ---
CSV_PATH = "workloads/pinecone/pinecone.csv"
INDEX_NAME = "my-index"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2" # dim = 384
BATCH_SIZE = 100

# --- clients ---
pc = Pinecone(api_key="")
index = pc.Index(INDEX_NAME)

# --- load csv ---
df = pd.read_csv(CSV_PATH)

# --- embedding helper ---
_model = SentenceTransformer(EMBED_MODEL)

def embed(texts: list[str]) -> list[list[float]]:
    embeddings = _model.encode(texts)
    return [emb.tolist() for emb in embeddings]


# --- upsert ---
for i in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[i:i + BATCH_SIZE]

    texts = batch["passage"].astype(str).tolist()
    embeddings = embed(texts)

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
