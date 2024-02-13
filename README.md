
![checks status](https://github.com/dcaribou/transfermarkt-scraper/workflows/Scrapy%20Contracts%20Checks/badge.svg)
![docker build status](https://github.com/dcaribou/transfermarkt-scraper/workflows/Dockerhub%20Image/badge.svg)
# transfermarkt-scraper

A web scraper for collecting data from [Transfermarkt](https://www.transfermarkt.co.uk/) website. It recurses into the Transfermarkt hierarchy to find
[competitions](https://www.transfermarkt.co.uk/wettbewerbe/europa), 
[games](https://www.transfermarkt.co.uk/premier-league/gesamtspielplan/wettbewerb/GB1/saison_id/2020),
[clubs](https://www.transfermarkt.co.uk/premier-league/startseite/wettbewerb/GB1),
[players](https://www.transfermarkt.co.uk/manchester-city/kader/verein/281/saison_id/2019) and [appearances](https://www.transfermarkt.co.uk/sergio-aguero/leistungsdaten/spieler/26399), and extract them as JSON objects. 

```console
====> Confederations ====> Competitions ====> (Clubs, Games) ====> Players ====> Appearances
```

Each one of these entities can be discovered and refreshed separately by invoking the corresponding crawler.

## Installation

This is a [scrapy](https://scrapy.org/) project, so it needs to be run with the
`scrapy` command line util. This and all other required dependencies can be installed using [poetry](https://python-poetry.org/docs/).

```console
cd transfermarkt-scraper
poetry install
poetry shell
```

## Usage

These are some usage examples for how the scraper may be run.

```console
# discover confederantions and competitions on separate invokations
scrapy crawl confederations > confederations.json
scrapy crawl competitions -a parents=confederations.json > competitions.json

# you can use intermediate files or pipe crawlers one after the other to traverse the hierarchy 
cat competitions.json | head -2 \
    | scrapy crawl clubs \
    | scrapy crawl players \
    | scrapy crawl appearances
```

Items are extracted in JSON format with one JSON object per item (confederation, league, club, player or appearance), which get printed to the `stdout`. Samples of extracted data are provided in the [samples](samples) folder.

### arguments
- `parents`: Crawler "parents" are either a file or a piped output with the parent entities. For example, `competitions` is parent of `clubs`, which in turn is a parent of `players`.
- `seasons`: The season that the crawler is to run for. It defaults to the most recent season.

## config
Check [setting.py](tfmkt/settings.py) for a reference of available configuration options
