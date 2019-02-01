class LimitedDict(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        for attr in self.__dir__():
            if attr.startswith("_"):
                continue
            value = getattr(self, attr)
            if callable(value):
                continue
            self._set_dict_attr(attr, value)

    def pop(self, key):
        return self[key]

    def clear(self):
        print(super(LimitedDict, self).__dict__.keys())

    def popitem(self):
        return None

    def update(self, *args, **kwargs):
        if len(args) == 1:
            args = args[0]

        if isinstance(args, dict):
            args = args.items()

        for key, value in args:
            if hasattr(self, key):
                setattr(self, key, value)

        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def setdefault(self, d):
        pass

    def _set_dict_attr(self, name, value):
        if hasattr(self, name):
            super(LimitedDict, self).__setitem__(name, value)

    def __getitem__(self, x):
        return getattr(self, x)

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)

    def __delitem__(self, key):
        if hasattr(self, key):
            delattr(self, key)
            self._set_dict_attr(key, getattr(self, key))

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if not key.startswith("_"):
            self._set_dict_attr(key, getattr(self, key))

    def __contains__(self, key):
        if key.startswith("_"):
            return False
        if hasattr(self, key):
            return True
