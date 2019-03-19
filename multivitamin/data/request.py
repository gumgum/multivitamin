import glog as log

DEFAULT_SAMPLE_RATE = 1.0


class Request:
    def __init__(self, request_dict, request_id=None):
        """Data object to encapsulate and cleanse request

        Args:
            request_dict (dict): input request json 
            request_id (str): ID tied to request (esp from AWS SQS)
        """
        if not isinstance(request_dict, dict):
            raise ValueError(
                f"request_dict is type: {type(request_dict)}, should be of type dict"
            )

        self.request = request_dict
        self.request_id = request_id

    def get(self, key, default=None):
        """Get method for any arbitrary key. Allows user to set default val

        Args:
            key (str): key in request dict
            default (Any): default value if key not in dict
        
        Returns:
            Any: value
        """
        return self.request.get(key, default)

    @property
    def url(self):
        """Convenience getter for the request URLj

        Returns:
            str: url
        """
        return self.request.get("url")

    @property
    def sample_rate(self):
        """Convenience getter for the request's defined sample_rate

        Returns:
            float: sample_rate
        """
        return self.request.get("sample_rate", DEFAULT_SAMPLE_RATE)

    @property
    def bin_encoding(self):
        """Getter for bin_encoding flag. Defaults to False
        
        Returns:
            bool: bin_encoding flag
        """
        be = self.request.get("bin_encoding", False)
        if isinstance(be, str):
            be = be.lower() == "true"
        elif isinstance(be, int):
            be = be == 1
        return be

    @property
    def bin_decoding(self):
        """Getter for bin_decoding flag. Defaults to False
        
        Returns:
            bool: bin_decoding flag
        """
        de = self.request.get("bin_decoding", False)
        if isinstance(de, str):
            de = de.lower() == "true"
        elif isinstance(de, int):
            de = de == 1
        return de

    @property
    def base64_encoding(self):
        """Getter for base64_encoding flag. Defaults to False
        
        Returns:
            bool: base64_encoding flag
        """
        be = self.request.get("base64_encoding", False)
        if isinstance(be, str):
            be = be.lower() == "true"
        elif isinstance(be, int):
            be = be == 1
        return be

    @property
    def prev_response(self):
        """Getter for prev_response
        
        Returns:
            str: prev_response
        """
        return self.request.get("prev_response")

    @property
    def prev_response_url(self):
        """Getter for prev_response_url
        
        Returns:
            str: prev_response_url
        """
        return self.request.get("prev_response_url")

    @property
    def dst_url(self):
        """Getter for destination url for response
        
        Returns:
            str: destination url
        """
        return self.request.get("dst_url")

    @property
    def flags(self):
        """Getter for arbitrary flags
        
        Returns:
            dict: flags
        """
        return self.request.get("flags")

    @property
    def kill_flag(self):
        """Getter for kill flag
        
        Returns:
            bool: kill flag
        """
        kf = self.request.get("kill_flag", False)
        if isinstance(kf, str):
            kf = kf.lower() == "true"
        elif isinstance(kf, int):
            kf = kf == 1
        return kf

    def __repr__(self):
        return f"request: {self.request}; request_id: {self.request_id}"


def _standardize_url(url):
    log.info("Formatting urls in request")
    if not url:
        raise ValueError("url is None")
    # url=url.replace("&amp;", "&")
    # url=url.replace(" ", "\\ ")
    # url=url.replace("https://", "http://")
    # url=url.replace("s://", "http://")
    # url=url.replace("s:", "http://")
    # url=url.replace("https://", "")
    # url=url.replace("http://s.yimg.com", "https://s.yimg.com")
    # url=url.replace(" ", "%20")
    return url
