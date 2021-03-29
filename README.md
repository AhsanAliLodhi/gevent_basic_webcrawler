## A very basic python gevent based webcrawler
Uses concurrency based light weight greenlets to achieve high performance.
Uses Beautiful soap with `htlm.parser` to parse a webpage.

### Setup

```
pip install requirements.txt
```

### Usage

To run the basic example to crawl "http://news.ycombinator.com".
```
python web_crawler.py
```

To crawl your custom webpage.
```
python web_crawler.py -u "http://url"
```

To count query string in urls as unique urls. i.e to treat www.website.com?asd=123 different from www.website.com?asd=213 or www.website.com?cda=123 use `-q` or `--qs` flag.

```
python web_crawler.py -u "http://url" -q
```