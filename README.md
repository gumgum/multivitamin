# Multivitamin

*Supplements for your computer vision & machine learning models.*

![](https://i.imgur.com/EEpn6iv.png)

Multivitamin is python framework for serving computer vision (CV) and machine learning (ML) models in the cloud. It handles the infrastructure for a single microservice and enforces an interface for processing media.

## User Guide

Multivitamin enforces an interface via inheritance for defining a module. A module is defined as a class which does some processing on a request message. There are 3 module-related classes: Module, ImagesModule, and PropertiesModule. ImagesModule and PropertiesModule are children of Module.

Create a class that inherits from Module, ImagesModule, or PropertiesModule. Implement a process_images or process_properties or process method.

ImagesModule: a child of Module. ...

PropertiesModule: a child of Module. ...

Self.request has everything from request
Self.response

User: must create a Server object with (in_api, out_api, modules_list)


## Installation

```
conda install multivitamin
```

### Requirements
```
python 3.6+
```

## Documentation

![](https://i.imgur.com/ACSbj0M.png)

