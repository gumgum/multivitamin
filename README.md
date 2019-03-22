# Multivitamin

*Python supplements for serving your computer vision & machine learning models.*

![](https://i.imgur.com/ll70SQO.png)

**Multivitamin** is python framework for serving computer vision (CV) and machine learning (ML) models in the cloud, aimed at It is intended to define the infrastructure around a single service.

## Main Features

* Asynchronous APIs sharing a common interface (`CommAPI`) for pulling requests and pushing responses
* An interface (via the `Module` class) for processing images, video, text, or any form of data
* A data model for storing the computed data

## Getting Started

To start an asynchronous service, construct a `Server` object, which accepts 3 input parameters:
* An input `CommAPI`, which is an abstract base class that defines the `push()` and `pull()` interface
* An output `CommAPI`
* A `Module`, an abstract base class that defines the interface for `process(Request)`, `process_properties()` or `process_images(...)`

Defining input and output `CommAPI`s:
```
from multivitamin.apis import SQSAPI, S3API

sqs_api = SQSAPI(queue_name='SQS-ObjectDetector')
s3_api = S3API(s3_bucket='od-output', s3_key='2019-03-22')
```

Both `SQSAPI` and `S3API` are concrete implementations of `CommAPI`.

Defining a `Module`:

For convenience, we provide several example modules (which are concrete implementations of `Module`) that you can import for your purposes. Let's say we want a object detector built using [TensorFlow's object detection API](https://github.com/tensorflow/models/tree/master/research/object_detection):
```
from multivitamin.applications.images.detectors import TFDetector

obj_det_module = TFDetector(name="IG_obj_det", ver="1.0.0", model="models_dir/")
```

Now, let's put it all together and construct a `Server` which will pull requests from the AWS SQS queue `queue_name=SQS-ObjectDetector` and push the responses to `s3://aws.amazon.com/od-output/2019-03-22/`

```
from multivitamin.server import Server

obj_det_server = Server(
    modules=[obj_det_module],
    input_comm=sqs_api,
    output_comms=[s3_api],
)

obj_det_server.start()
```

## Installation

Using [conda](https://conda.io/en/latest/):
```
conda install multivitamin
```

Using [nvidia-docker](https://github.com/NVIDIA/nvidia-docker):
```
docker run --runtime=nvidia multivitamin:cuda9-cudnn7 /bin/bash
```

## Documentation

For API documentation and full details, see [https://multivitamin.readthedocs.io](https://multivitamin.readthedocs.io)

### High-level overview

**Data flow**: 
1. JSON request is "pulled" by a `CommAPI` object
1. JSON request is used to construct a `Request` class
1. `Server` creates a (typically) empty `Response` from the `Request`. If the `Request`contains a previous module's `Response` (for modules run in a sequence), that is pre-populated in the `Response`
1. `process_request()` sends the `Response` through all `Module`s
1. Each `Module` appends/modifies the `Response`
1. `process_request()` returns the `Response` back to the `Server`
1. `Server` sends the `Response` to the output `CommAPI`(s) and calls the `push(Response)` method

![](https://i.imgur.com/NwpdShq.png)

**Data structures:**
* data/  
  * **Request:** data object encapsulating request JSON
  * response/
    * **Response:** data object encapsulating response that reflects the schema. Contains methods for serialization, modifying internal data 
    * **ResponseInternal:** Python dataclasses with typechecking that matches the schema
* module/
    * **Module:** abstract parent class that defines an interface for processing requests
    * **ImagesModule:** abstract child class of `Module` that defines an interface for processing requests with images or video, `process_images(...)` and handles retrieval of media.  
    * **PropertiesModule:** abstract child class of `Module` that defines an interface `process_properties()`
![](https://i.imgur.com/hfF4Ong.png)
* apis/
    * **CommAPI:** abstract parent class that defines an interface, i.e. `push()` and `pull()`
    * **SQSAPI:** pulls requests from an SQS queue, pushes requests to a queue
    * **VertexAPI:** pulls requests from an SQS queue, pushes Responses by posting to a HTTP endpoint (provided in the request)
    * **LocalAPI:** pulls requests from a local directory of JSONs, pushes Responses to a local directory
    * **S3API:** pulls requests from an S3 bucket of JSONs, pushes Responses to an S3 bucket
