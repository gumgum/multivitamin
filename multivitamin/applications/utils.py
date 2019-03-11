import os


def load_idmap(idmap_file):
    """Load tab-separated idmap file containing label index and label string

    Args:
        idmap_file (str): filepath to idmap

    Returns:
        dict: labelmap (key=index, value=string)
    """
    if not os.path.exists(idmap_file):
        raise FileExistsError(idmap_file)

    labelmap = {}
    with open(idmap_file, "r") as rf:
        for row in rf:
            row = row.split("\t")
            labelmap[int(row[0])] = row[1].strip()
    return labelmap


def load_label_prototxt(prototxt_file):
    from google.protobuf import text_format
    from caffe.proto import caffe_pb2 as cpb2

    with open(prototxt_file) as f:
        labelmap_aux = cpb2.LabelMap()
        text_format.Merge(str(f.read()), labelmap_aux)
        num_labels = len(labelmap_aux.item)
        labelmap = {}
        for item in labelmap_aux.item:
            index = item.label
            label = item.display_name
            labelmap[index] = label
    return labelmap
