# Multivitamin

*Supplements for your computer vision & machine learning models.*

![](https://i.imgur.com/EEpn6iv.png)

Multivitamin is python framework for serving computer vision (CV) and machine learning (ML) models in the cloud. It handles the infrastructure around a microservice and enforces an interface for processing media.

### Motivation

At GumGum, we needed a way to serve CV & ML models in the cloud while being agnostic to the specific framework used for prediction, be it PyTorch, Caffe/2, TensorFlow, MxNet, or just plain python. 

## Usage

(TODO:)

Create a class that inherits from Module, ImagesModule, or PropertiesModule. Implement a process_images or process_properties or process method.

ImagesModule: a child of Module. ...

PropertiesModule: a child of Module. ...

Self.request has everything from request
Self.response

User: must create a Server object with (in_api, out_api, modules_list)


## Installation

### Requirements
```
python 3.6+
```

## What's in Multivitamin?

![](https://i.imgur.com/ACSbj0M.png)

* APIs: Interaction w/ AWS’s SQS, standardization of incoming requests
* data: How we build JSONs, our schema for representing ML data
* module: Interface for implementing ML modules
* media: A common interface for reading video and images from remote & local locations

