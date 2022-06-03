import abc
import urllib.request


class HttpBase(object):
    """
    Object using urllib.request to obtain
    a HTML page given a url dependent on
    some arguments.
    """
    @property
    @abc.abstractmethod
    def url(self):
        return ""

    def get_html_by_url(self):
        fp = urllib.request.urlopen(self.url)
        mybytes = fp.read()
        html = mybytes.decode("utf8")
        fp.close()
        return html

    @abc.abstractmethod
    def get_url(self, *args, **kargs):
        pass

    def get_html(self, *args, **kargs):
        fp = urllib.request.urlopen(self.get_url(*args, **kargs))
        mybytes = fp.read()
        html = mybytes.decode("utf8")
        fp.close()
        return html
