import json
import pytest
import glog as log
from multivitamin.data.request import Request


r1 = """{"url":"https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/A_blank_black_picture.jpg/1536px-A_blank_black_picture.jpg",
    "dst_url":"https://dummy.url",
    "prev_response":"","bin_encoding":"true","bin_decoding":"true"}"""
r2 = """{"url":"http://pbs.twimg.com/media/*&&jesMessageWeird&&f---Char:large"}"""
r3 = "faulty_message"


def test_r1():
    req = Request(json.loads(r1))
    assert (
        req.url
        == "https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/A_blank_black_picture.jpg/1536px-A_blank_black_picture.jpg"
    )


def test_r2():
    # test standardize_url
    pass


def test_r3():
    with pytest.raises(ValueError):
        Request(json.loads(r3))
