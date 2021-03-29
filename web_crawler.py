#!/usr/bin/env python
"""A very simple gevent based webcrawler."""
import gevent
from gevent import monkey, queue, event, pool
monkey.patch_all()
import requests
import uuid
import argparse
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from logger import get_logger


class WebCrawler:
    """Main class for the webcrawler."""

    def __init__(self, worker_pool_size=20, queue_size=10000, timeout=2):
        """Initialize crawler with general settings."""
        self.worker_pool_size = worker_pool_size
        self.queue_size = queue_size
        self.timeout = timeout
        self.base_url = urlparse('')

    def start_crawling(self, url: str, show_urls=False, allow_query_string=False):
        """Initialize job specific variables and start a scheduler to assign crawling jobs. Return all unique urls."""
        self.base_url = urlparse(url)
        assert self.base_url.netloc != '', "Error: no url found in url {}".format(url)
        self.show_urls = show_urls  # set to true if want to show urls on console
        self.allow_query_string = allow_query_string  # set to true if a query string should be counted as a unique url
        self.worker_pool = pool.Pool(self.worker_pool_size)
        self.urlQ = queue.Queue(maxsize=self.queue_size)  # Queue to keep track of next urls to crawl, #TODO use database for more scalability.
        self.urlQ.put(url)  # initialize queue with the first url
        self.crawled_urls = set()  # Set to keep track of visited urls, #TODO use database for more scalability.
        self.crawled_urls_file = open("urls.txt", "w+")  # A file to keep saving intermediate results, # TODO use as cache to resume a crawler.
        self.url_crawled = event.Event()  # Event to announce crawling finished by a gevent, consumed by scheduler to reassign jobs if new urls
        self._scheduler = gevent.spawn(self.scheduler)  # start the scheduler
        self._scheduler.join()  # wait for scheduler to finish
        return self.crawled_urls  # return unique urls

    def is_valid_url(self, url: str):
        """Verification of a url based on what we treat as valid, only internat links are valid for this exercise."""
        if url is None or url == '':  # proceed only if url is not empty
            return False
        url = urlparse(url)
        # list of rules for a valid url, general rule: our url's path must be a branch of base path
        if self.base_url.path in url.path and (
                url.scheme == "" or url.scheme == "http" or url.scheme == "https") and (
                url.netloc == "" or  # rule1: empty netloc means same host, hence valid
                url.netloc == self.base_url.netloc):  # rule2: if same base path that's also valid
            return True
        else:
            # every thing else is an invalid url
            return False

    def normalize_url(self, url, allow_query_string=False):
        """Standerdize various forms of url for easy storage and verification."""
        url = urlparse(url)
        if url.netloc == "":
            url = url._replace(netloc=self.base_url.netloc)
        if url.scheme == "":
            url = url._replace(scheme=self.base_url.scheme)
        url = url._replace(fragment="")
        if not allow_query_string:
            url = url._replace(query="")
        return url.geturl()

    def __add_url_to_q__(self, url):
        """Add url to queue if its a valid url and also record it."""
        if self.is_valid_url(url):
            url = self.normalize_url(url, self.allow_query_string)
            if url not in self.crawled_urls:
                self.urlQ.put(url, timeout=self.timeout)
                self.crawled_urls.add(url)
                self.crawled_urls_file.write(url+"\n")
                if self.show_urls:
                    print(url)
            return True
        return False

    def scheduler(self):
        """Schedule a worker to crawl a url if there is any job in the queue. If no worker is busy and no job in queue then exit."""
        LOGGER = get_logger("scheduler")
        while True:
            for worker in list(self.worker_pool):
                if worker.dead:
                    self.worker_pool.discard(worker)
            LOGGER.debug("alive workers: {workers}".format(workers=len(list(self.worker_pool))))
            try:
                url = self.urlQ.get_nowait()
                LOGGER.debug("found {url} in queue".format(url=url))
            except queue.Empty:
                LOGGER.warn("queue empty")
                if self.worker_pool_size == self.worker_pool.free_count():
                    LOGGER.debug("no worker busy, closing crawler")
                    self.worker_pool.join()
                    self.crawled_urls_file.close()
                    LOGGER.debug("closed!")
                    return
                else:
                    LOGGER.debug("some worker(s) busy, waiting for them to finish")
                    self.url_crawled.wait()
                    LOGGER.debug("some worker(s) finished working, checking if new url in queue")
                    self.url_crawled.clear()
                    continue
            worker_id = str(uuid.uuid4())
            LOGGER.debug("spawing worker: {idx} to crawl url {url}".format(idx=worker_id, url=url))
            self.worker_pool.spawn(self.__url_crawler__, url, worker_id)

    def __url_crawler__(self, url, idx):
        """Worker to crawl a url. Downloads the url, parses it and stores any new outgoing links. #TODO add a seperate pipeline for postprocessing."""
        LOGGER = get_logger("__url_crawler__")
        LOGGER.debug("worker {idx}: starting to crawl {url}".format(url=url, idx=idx))
        try:
            req = requests.get(url)
            LOGGER.debug("worker {idx}: download_success {url}".format(url=url, idx=idx))
        except Exception as e:
            LOGGER.warn("worker {idx}: download_failed {url}".format(url=url, idx=idx))
            req = None
            pass
        if req and req.status_code == 200:
            try:
                soup = BeautifulSoup(req.text, "html.parser")
                LOGGER.debug("worker {idx}: parsed {url}".format(url=url, idx=idx))
                anchors = soup.find_all('a', href=True)
                LOGGER.debug("worker {idx}: found {anchors}".format(idx=idx, anchors=len(anchors)))
                anchors = [self.__add_url_to_q__(anchor['href']) for anchor in anchors]
                LOGGER.debug("worker {idx}: added {anchors}".format(idx=idx, anchors=sum(anchors)))
            except Exception as e:
                LOGGER.warn("adding urls failed")
        LOGGER.debug("exiting worker {idx}".format(idx=idx))
        self.url_crawled.set()
        raise gevent.GreenletExit('success')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", help="url you want to crawl.", default="http://news.ycombinator.com")
    parser.add_argument("-q", "--qs", help="allow query string as unique url.", default=False, action='store_true')

    args = parser.parse_args()
    my_crawler = WebCrawler()
    urls = my_crawler.start_crawling(url=args.url, show_urls=True, allow_query_string=args.qs)
