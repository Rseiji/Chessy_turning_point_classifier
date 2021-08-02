# How to use this crawler

* Run `get_game_urls.py` to scrape game pages urls. It will search for these urls in the site's list of game, distributed among several pages. You must inform initial and final page numbers and also a `.txt` file to save game urls.

<br>

```
python get_game_urls.py initial_page final_page file_game_urls.txt
```


* To get the raw data, you must run `get_pgn.py`, having a `.txt` file containing game urls to scrape and an output path as arguments.

```
python get_pgn.py game_links.txt output_folder/ 
```

<br>

* As a result, all url games are stored, saved in `.pgn` files.
