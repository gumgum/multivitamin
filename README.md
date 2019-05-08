# Multivitamin
![](https://i.imgur.com/ll70SQO.png)

[![Build Status](https://travis-ci.org/gumgum/multivitamin.svg?branch=master)](https://travis-ci.org/gumgum/multivitamin)
[![PyPI version](https://badge.fury.io/py/multivitamin.svg)](https://badge.fury.io/py/multivitamin)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Multivitamin** is python framework built for serving computer vision (CV), natural language processing (NLP), and machine learning (ML) models. It aims to provide the serving infrastructure around a single service and to allow the flexibility to use any python framework for prediction.

## Main Features

* Asynchronous APIs sharing a common interface (`CommAPI`) for pulling requests and pushing responses
* An interface (via the `Module` class) for processing images, video, text, or any form of data
* A data model for storing the output of the modules

## Getting Started

To start an asynchronous service, construct a `Server` object, which accepts 3 input parameters:

* An input `CommAPI`, which is an abstract base class that defines the `push()` and `pull()` interface
* An output `CommAPI`
* A `Module` or sequence of `Module`s, which is an abstract base class that defines the interface for `process(Request)`, `process_properties()` or `process_images(...)`

### Defining input and output `CommAPI`s:
```
from multivitamin.apis import SQSAPI, S3API

sqs_api = SQSAPI(queue_name='SQS-ObjectDetector')
s3_api = S3API(s3_bucket='od-output', s3_key='2019-03-22')
```

Both `SQSAPI` and `S3API` are concrete implementations of `CommAPI`.

### Defining a `Module`:

For convenience, we provide several example modules (which are concrete implementations of `Module`) that you can import for your purposes. Let's say we want a object detector built using [TensorFlow's object detection API](https://github.com/tensorflow/models/tree/master/research/object_detection):
```
from multivitamin.applications.images.detectors import TFDetector

obj_det_module = TFDetector(name="IG_obj_det", ver="1.0.0", model="models_dir/")
```

### Constructing a `Server`
Which will pull requests from the AWS SQS queue `queue_name=SQS-ObjectDetector` and push the responses to `s3://aws.amazon.com/od-output/2019-03-22/`

```
from multivitamin.server import Server

obj_det_server = Server(
    modules=[obj_det_module],
    input_comm=sqs_api,
    output_comms=[s3_api],
)

obj_det_server.start()
```

If we wanted to **send our responses to multiple endpoints**, we could add a second output `CommAPI` like so:
```
from multivitamin.apis import HTTPAPI

http_api = HTTPAPI()
```
and modifying the above `Server` we created like:
```
obj_det_server = Server(
    modules=[obj_det_module],
    input_comm=sqs_api,
    output_comms=[
        s3_api,
        http_api,
    ],
)
```
*note: the `HTTPAPI` assumes that the `Request` has a field called `dst_url`. `HTTPAPI` will send a POST request to that destination URL.*

### Chaining `Modules` 
If we wanted to **run a sequence of `Module`s**, we could add a second `Module`. Say, we had an image classifier written in [pytorch](https://github.com/pytorch/pytorch) that predicted the make and model of a vehicle. A pytorch image classifier is another example application we provide in `multivitamin.applications.images`
```
from multivitamin.applications.images.classifiers.pyt_classifier import PYTClassifier

make_model_clf = PYTClassifier(name="make-model", ver="1.0.0", model="models/mm.pth")
make_model_clf.set_previous_properties_of_interest([
    {"value":"car"},
    {"value":"truck"},
])
```
The `set_previous_properties_of_interest` is a method to tell this `make_model_clf` module to only run its `predict_images` function for predictions of `car` OR `truck` found in the previous module (the 600 class TensorFlow object detector).


And now, creating a `Server`:
```
vehicle_mm_server = Server(
    modules=[
        obj_det_module,
        make_model_clf,
    ],
    input_comm=sqs_api,
    output_comms=[
        s3_api,
        http_api,
    ],
)
vehicle_mm_server.start()
```
## Installation

Using [conda](https://conda.io/en/latest/):
```
conda install multivitamin
```

Using [pip](https://pip.pypa.io/en/stable/installing/)
*Note: this requires opencv be already installed. We highly recommend installing with conda instead*
```
pip install multivitamin
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
2. JSON request is used to construct a `Request` class
3. `Server` creates a (typically) empty `Response` from the `Request`. If the `Request`contains a previous module's `Response` (for modules run in a sequence), that is pre-populated in the `Response`
4. `process_request()` sends the `Response` through all `Module`s
5. Each `Module` appends/modifies the `Response`
6. `process_request()` returns the `Response` back to the `Server`
7. `Server` sends the `Response` to the output `CommAPI`(s) and calls the `push(Response)` method

![](https://i.imgur.com/NwpdShq.png)

**Repository organization:**

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
    * **HTTPAPI:** pushes Responses by posting to a HTTP endpoint (provided in the request)
    * **LocalAPI:** pulls requests from a local directory of JSONs, pushes Responses to a local directory
    * **S3API:** pulls requests from an S3 bucket of JSONs, pushes Responses to an S3 bucket

## Contributing

To file a bug or request a feature, please file a GitHub issue. Pull requests are welcome. 

## The Team

Multivitamin is currently maintained by [Greg Chu](https://github.com/gregchu), [Matthew Greenberg](https://github.com/magreenberg1), and [Javier Molina](https://github.com/javimol), with contributions from [Divyaa Ravichandran](https://github.com/stalagmite7), and Rohit Annigeri, and with collaboration from Cambron Carter, Shankar Chatterjee, Nandakishore Puttashamachar, Nishita Sant, and Iris Fu.
