from cvapis.applications.classifiers.caffe_classifier import CaffeClassfier

def test_init():
    global cc

    server_name = "TestClassifier"
    version = "0.0.0"
    net_data_dir = "/tmp/net_data"
    # prop_type = "label"
    # prop_id_map = {}
    # module_id_map
    cc = CaffeClassfier(server_name, version, net_data_dir)

def test_preprocess_images():
    global images_, images_cropped

    # Load Batch of Images
    images = []
    sample_prev_detections = []

    assert(len(images) == len(sample_prev_detections))

    # Test preprocessing
    images_ = cc.preprocess_images(images)
    assert(len(images) == images_.shape[0])

    images_cropped = cc.preprocess_images(images)
    assert(images_.shape == images_cropped.shape)
    assert(images_ != images_cropped)

def test_process_images():
    global preds, preds_from_crops
    preds = cc.process_images(images_)
    preds_from_crops = cc.process_images(images_cropped)

    assert(preds.shape == preds_from_crops.shape)
    assert(preds.shape[0] == images_.shape[0])

def test_postprocess_predictions():
    global postprocessed_preds, postprocessed_preds_from_crops
    postprocessed_preds = cc.postprocess_predictions(preds)
    postprocessed_preds_from_crops = cc.postprocess_predictions(preds_from_crops)

def test_append_detections():
    current_detections = cc.detections.copy()
    
    # Vanilla Append
    cc.append_detections(postprocessed_preds)
    cc.detections = current_detections.copy()

    cc.append_detections(postprocessed_preds_from_crops)
    cc.detections = current_detections.copy()

    # Append with Tstamps

    # Append with previous_detections

    # Append with both


def test_process():
    # Test on short video
    # message = None
    # cc.process(message)
    pass