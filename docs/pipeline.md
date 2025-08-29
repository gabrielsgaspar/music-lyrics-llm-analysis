# Topics Scoring Pipeline

This document explains, step by step, how we transform raw song lyrics into **topic scores** per song. It includes the data flow, models, parameters, and prompts. The pipeline transforms raw song lyrics into interpretable topic scores through the following major steps:

1. **Summarization**: Generate a short, factual summary of each song’s lyrics to standardize length and content.
2. **Embedding**: Convert summaries into numerical embeddings that capture semantic meaning.
3. **Dimensionality reduction**: Use UMAP to reduce high-dimensional embeddings into a compact representation suitable for clustering.
4. **Clustering**: Apply HDBSCAN to discover natural groupings of songs based on lyrical themes.
5. **Representative songs**: Identify the most representative songs for each cluster by measuring closeness to cluster centroids.
6. **Topic extraction**: Ask a language model to read the representative songs and propose high-level topics that best describe them.
7. **Topic consolidation**: Normalize, merge, and organize extracted topics into a global taxonomy of coherent, non-redundant topics.
8. **Topic scoring**: For each song, score how strongly each topic appears (0 = not present, 1 = weakly present, 2 = strongly present).

The end result is a topic score matrix where each song is represented by interpretable scores across a set of coherent topics.

---

## Step 1: Summarization

The first step is to reduce the variability and verbosity of lyrics by summarizing them into around four concise sentences. The summaries capture the main story or imagery of the song without adding information. Summaries are factual, neutral, and help make downstream analysis more consistent.

For example, the song ```I'll Be Home for Christmas``` by the singer ```Kelly Clarkson``` has the following lyrics:
 
 ```text
I'll be home for Christmas
You can count on me
Please have snow and, and mistletoe
And presents under the tree

Christmas Eve will find you
Where the love light gleams
I'll be home for Christmas
If only in my dreams

Christmas Eve will find me
Where the love light gleams
I'll be home for Christmas
If only in my dreams
```

Using ```ChatGPT``` model ```gpt-4o-mini``` we obtain the following summary:

```text
The singer expresses a longing to be home for Christmas, emphasizing the importance of snow, mistletoe, and presents. They convey a sense of hope and anticipation for Christmas Eve, where love is present. Despite the desire to be home, there is an acknowledgment that it may only be a dream. The recurring theme highlights the emotional connection to home and the festive season.
```

We use the text above to obtain embeddings and perform the clustering steps.

---

## Step 2: Embedding

Once summarized, the texts are embedded into dense numerical vectors. These embeddings represent the semantic meaning of the summaries in a high-dimensional space, enabling similarity and clustering analyses. Summaries are used instead of full lyrics to keep embeddings compact and focused.

Using our previous summary as an example we get the embedding vector:

```text
[1.00358119e-02, 9.62350238e-03, -2.67681796e-02, ... , 1.25810150e-02, 1.62283387e-02, -6.39386624e-02]
```

In our baseline sentence embedding model ```all-mpnet-base-v2``` this is ```768``` dimension vector.

---

## Step 3: Dimensionality Reduction (UMAP)

Embeddings live in a high-dimensional space (hundreds of dimensions). UMAP reduces these to a lower-dimensional manifold (e.g., 10–20 dimensions) while preserving local semantic structure. This step makes the data easier to cluster and visualize.

---

## Step 4: Clustering (HDBSCAN)

On the reduced embeddings, HDBSCAN is applied to discover natural clusters of songs. HDBSCAN is density-based, meaning it can find clusters of varying size and shape, while labeling noise or outliers. Each cluster represents a group of songs with similar lyrical themes.

---

## Step 5: Representative Songs

For each cluster, we identify the “centroid” embedding and select the top songs closest to it. These representative songs are the most central to the cluster and will serve as the examples we use to infer what each cluster is about.

---

## Step 6: Topic Extraction

Using the representative songs, we prompt a language model to identify recurring themes across them. The model proposes a list of high-level topics (e.g., “romantic loss”, “youth rebellion”, “nostalgia”). These topics describe what unites the songs in each cluster.

---

## Step 7: Topic Consolidation

Since different clusters may generate overlapping or redundant topics, another step is used to merge similar topics into a global taxonomy. The goal is to have a consistent set of topic labels that are coherent, distinct, and interpretable. For example, “romantic loss” and “breakup pain” might both map to “heartbreak”.

---

## Step 8: Topic Scoring

Finally, we return to the level of individual songs. For each song’s summary, the model scores each global topic:
- **0** = topic not present
- **1** = topic weakly present
- **2** = topic strongly present

This produces a matrix of songs × topics, with interpretable numeric values.

---

## Outputs and Deliverables

The pipeline produces:

- **Summaries** of lyrics for each song.
- **Embeddings** aligned with each song summary.
- **UMAP representations** and clustering labels.
- **Representative song lists** for each cluster.
- **Cluster-level topics** and descriptions.
- **A consolidated global topic taxonomy**.
- **Topic scores per song**, forming the final interpretable dataset.

---