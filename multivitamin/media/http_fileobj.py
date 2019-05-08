"""Adaptation of Martin Valgur's SeekableHTTPFile.

https://github.com/valgur/pyhttpio
"""

import cgi
import time
import urllib.request
from io import IOBase
from sys import stderr


class HTTPFile(IOBase):
    """Turns URLs into Filelike objects."""

    def __init__(self, url, name=None, repeat_time=-1, debug=False):
        """Allow a file accessible via HTTP to be used like a local file by
        utilities that use `seek()` to read arbitrary parts of the file, such
        as `ZipFile`. Seeking is done via the 'range: bytes=xx-yy' HTTP header.

        Parameters
        ----------
        url : str
            A HTTP or HTTPS URL
        name : str, optional
            The filename of the file.
            Will be filled from the Content-Disposition header if not provided.
        repeat_time : int, optional
            In case of HTTP errors wait `repeat_time` seconds before trying
            again.
            Negative value or `None` disables retrying and simply passes on
            the exception (the default).
        """
        super().__init__()
        self.url = url
        self.name = name
        self.repeat_time = repeat_time
        self.debug = debug
        self._pos = 0
        self._seekable = True
        with self._urlopen() as f:
            if self.debug:
                print(f.getheaders())
            self.content_length = int(f.getheader("Content-Length", -1))
            if self.content_length < 0:
                self._seekable = False
            if f.getheader("Accept-Ranges", "none").lower() != "bytes":
                self._seekable = False
            if name is None:
                header = f.getheader("Content-Disposition")
                if header:
                    value, params = cgi.parse_header(header)
                    self.name = params.get("filename")

    def seek(self, offset, whence=0):
        if not self.seekable():
            raise OSError
        if whence == 0:
            self._pos = 0
        elif whence == 1:
            pass
        elif whence == 2:
            self._pos = self.content_length
        self._pos += offset
        return self._pos

    def seekable(self, *args, **kwargs):
        return self._seekable

    def readable(self, *args, **kwargs):
        return not self.closed

    def writable(self, *args, **kwargs):
        return False

    def read(self, amt=-1):
        if self._pos >= self.content_length:
            return b""
        if amt < 0:
            end = self.content_length - 1
        else:
            end = min(self._pos + amt - 1, self.content_length - 1)
        byte_range = (self._pos, end)
        self._pos = end + 1
        with self._urlopen(byte_range) as f:
            return f.read()

    def readall(self):
        return self.read(-1)

    def tell(self):
        return self._pos

    def __getattribute__(self, item):
        attr = object.__getattribute__(self, item)
        if not object.__getattribute__(self, "debug"):
            return attr

        if hasattr(attr, '__call__'):
            def trace(*args, **kwargs):
                a = ", ".join(map(str, args))
                if kwargs:
                    a += ", ".join(["{}={}".format(k, v) for k, v in kwargs.items()])
                print("Calling: {}({})".format(item, a))
                return attr(*args, **kwargs)

            return trace
        else:
            return attr

    def _urlopen(self, byte_range=None):
        header = {}
        if byte_range:
            header = {"range": "bytes={}-{}".format(*byte_range)}
        while True:
            try:
                r = urllib.request.Request(self.url, headers=header)
                return urllib.request.urlopen(r)
            except urllib.error.HTTPError as e:
                if self.repeat_time is None or self.repeat_time < 0:
                    raise
                print("Server responded with " + str(e), file=stderr)
                print("Sleeping for {} seconds before trying again".format(self.repeat_time), file=stderr)
                time.sleep(self.repeat_time)
