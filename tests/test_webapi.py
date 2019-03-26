import context
from vitamincv.comm_apis.web_api import WebAPI
from vitamincv.comm_apis.dummy_module import DummyCVModule

wa = WebAPI(DummyCVModule())
wa.pull()
