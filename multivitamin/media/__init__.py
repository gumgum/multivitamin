# import glog as log
# import inspect
# media_retriever_deprecation = [
#     "from multivitamin.media.media_api import MediaRetriever"
#     ]
# if inspect.stack()[-1].code_context in media_retriever_deprecation:
#     log.warning("Importing MediaRetriever will be deprecated. Please import "
#                 "get_media_retriever from multivitamin.media")

from .opencv_media_retriever import OpenCVMediaRetriever
from .pims_media_retriever import PIMSMediaRetriever
from .file_retriever import FileRetriever
from .opencv_media_retriever import OpenCVMediaRetriever as MediaRetriever
