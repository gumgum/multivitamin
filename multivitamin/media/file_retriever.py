import os
import requests
import magic
from io import BytesIO
import urllib.parse
import glog as log
from imohash import hashfileobject
from .http_fileobj import HTTPFile


class FileRetriever:
    """A generic class for retrieving files."""

    def __init__(self, url=None):
        """Init FileRetriever.

        Args:
            url (str | optional): A local or remote url to a file.

        """
        self._url = None
        self._is_local = None
        self._content_type = None
        self._hash = None
        if url is not None:
            self.url = url

    @property
    def url(self):
        """Get the url to the file."""
        return self._url

    @url.setter
    def url(self, value):
        self._content_type = None
        url_scheme = urllib.parse.urlparse(value).scheme

        self._url = value
        if url_scheme in ["", "file"]:
            self._is_local = True
        else:
            self._is_local = False

        if not self.exists:
            self._is_local = None
            self._url = None
            raise FileNotFoundError(value)

    @property
    def exists(self):
        """Check if the url points to a real file."""
        if self.is_local:
            return self._does_local_file_exist()
        else:
            return self._does_remote_file_exist()

    def _does_local_file_exist(self):
        if os.path.isfile(self.filepath):
            return True
        return False

    def _does_remote_file_exist(self):
        try:
            requests.head(self.url)
        except Exception as e:
            log.warning("Failed to retrieve url: {}".format(e))
            return False
        return True

    @property
    def filepath(self):
        """Get the local filepath.

        Returns:
            `None` if the file is remote
            else will return the local filepath (not the local file url)

        """
        return self.url.replace("file://", "") if self.is_local else None

    @property
    def content_type(self):
        """Get the MIME type of the file."""
        if self._content_type is None and self.is_local and self.exists:
            mime = magic.Magic(mime=True)
            self._content_type = mime.from_file(self.filepath)

        if self._content_type is None and self.is_remote and self.exists:
            resp = requests.head(self.url)
            self._content_type = resp.headers["Content-Type"]

        return self._content_type

    @property
    def is_local(self):
        """Check if file is local."""
        return self._is_local

    @property
    def is_remote(self):
        """Check if file is remote."""
        return not self.is_local if isinstance(self.is_local, bool) else False

    @property
    def filename(self):
        """Get the filename of the url."""
        return os.path.basename(self.url)

    @property
    def hash(self):
        """Get quick hash of file bytes."""
        if self._hash is None:
            filelike = HTTPFile(self.url)
            self._hash = hashfileobject(filelike, hexdigest=True)

        return self._hash

    def download(self, filepath=None, return_filelike=False):
        """Download file to filepath.

        Args:
            filepath (str | optional): Filepath to write file to.
                                If directory, it will take it's original
                                filename and save to that directory
            filelike (bool | optional): To return a filelike object or not

        Returns:
            filelike_obj: A BytesIO object containing the file bytes
                            (only if return_filelike is True)
        """

        if self.is_remote:
            response = requests.get(self.url)
            filelike_obj = BytesIO(response.content)
        else:
            with open(self.filepath, "rb") as f:
                filelike_obj = BytesIO(f.read())

        path = None
        if isinstance(filepath, str):
            if os.path.isdir(filepath):
                path = "{}/{}".format(filepath, self.filename)
            else:
                path = filepath
            with open(path, "wb") as f:
                f.write(filelike_obj.read())

        if return_filelike is True:
            filelike_obj.seek(0)
            return filelike_obj
