# Movie Engine
This is a repository for a movie recommendation engine that I'm building to learn more about AI/ML.

This code is licensed under the BSD-3-Clause license, so you _could_ use it for your own project if you give me credit. I strongly recommend you not use it for anything.

Here, read an XKCD about Python instead:

![Python XKCD](python.png)

[https://xkcd.com/353/](https://xkcd.com/353/)


## Setup
To set up the environment, run:

```bash
pip install -r requirements.txt
```

To get the data for processing, download the datasets and place them in the `movie-engine-data/raw/` directory.

- MovieLens 100K Dataset [link](https://files.grouplens.org/datasets/movielens/ml-latest-small.zip)
- MovieLens 32M Dataset [link](https://files.grouplens.org/datasets/movielens/ml-32m.zip)
- Other MovieLens datasets can be found [here](https://grouplens.org/datasets/movielens/).

## Organization
- `movie-engine-data/`: Contains datasets used for training and evaluation.
- `movie-engine-data/raw/`: Raw, unprocessed data files.
- `movie-engine-data/processed/`: Processed data files ready for use.

