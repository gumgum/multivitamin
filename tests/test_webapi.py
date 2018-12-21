import context
from cvapis.comm_apis.web_api import WebAPI
from cvapis.comm_apis.dummy_module import DummyCVModule

wa = WebAPI(DummyCVModule())
wa.pull()