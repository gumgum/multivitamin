![](https://i.imgur.com/EEpn6iv.png)

# vitaminCV

## Installation
```
git clone git@bitbucket.org:gumgum/cvapis.git
cd cvapis
sudo pip3 install .
```

**Requirements**
```
python 3.5+
```


##Communication APIs

Interfaces that pull and push CV data
CommAPI defines a generic interface with the following methods:
```
pull(...)
push(...)
```

Implementations will be responsible for instantiating the particular type (WebAPI, ESAPI, SQSAPI, etc)

**If you need to connect to AWS**

*Install Assume role sx*

```
sudo apt install golang-go
```

Add this to `~/.bashrc`
```
export GOPATH=$HOME/go
PATH=$PATH:$GOPATH/bin
```
Run:
```
go get -u github.com/remind101/assume-role
```

To run assume-role:
```
eval $(assume-role -duration=8h sx)
```

