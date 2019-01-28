import os
import glog as log

def load_idmap(idmap_filepath):
    """Load idmap
    
    Each row corresponds to an int_id, label_str pairing, tab-separated    
    
    Args:
    idmap_filepath (str): path to idmap
    
    Raises:
    FileNotFoundError: if idmap does not exist
    ValueError: Problems parsing 
    """
    idmap={}
    if not os.path.exists(idmap_filepath):
        raise FileNotFoundError("{} not found.".format(idmap_filepath))
    try:
        with open(idmap_filepath, 'r') as rf:
            for row_in in rf:
                try:
                    row = row_in.split('\t')
                    assert(len(row)>=2)
                except:
                    row = row_in.split(' ')
                    assert(len(row)>=2)
                id=int(row[0])
                label = row[1].strip()                    
                idmap[label] = id
    except:
        log.error("Problems parsing " + idmap_filepath)
        raise ValueError("Problems parsing " + idmap_filepath)
    return idmap

def min_conf_filter_predictions(filter_dict, preds, confs, label_dict=None):
    """ Filter our predictions based on per label confidence thresholds

    Args:
        filter_dict (dict): A dict of strings or ints that get mapped a minimum confidence 
                            value. If the keys are strings, label_dict is required.
        preds (list): A list of predicted labels (strings) or classes (ints)
        confs (list): A list of floats associated to confidence 
                        values for each predicted class 
        label_dict (dict): A dict mapping the classifier output index to a string

    Returns:
        qualifying_preds (list): A list of elements from preds 
                                    that have qualifying confidence
    """
    if label_dict is None:
        label_dict = {}
        
    qualifying_preds = []
    for pred, conf in zip(preds, confs):
        min_conf = filter_dict.get(pred)
        if min_conf is None:
            min_conf = filter_dict.get(label_dict.get(pred))

        if min_conf is None:
            min_conf = 0
        if conf >= min_conf:
            qualifying_preds.append(pred)
    return qualifying_preds

def list_contains_only_none(l):
    return l==[None]*len(l)

def p0p1_from_bbox_contour(contour, w=1, h=1, dtype=int):
    """Convert `contour` into p0 and p1 of a bounding box.

    Args:
        contour (list): list dict of points x, y
        w (int): width
        h (int): height

    Returns:
        Two points dict(x, y): p0 (upper left) and p1 (lower right)
    """
    if (len(contour) != 4):
        log.error("To use p0p1_from_bbox_contour(), input must be a 4 point bbox contour")
        return None

    # Convert number of pixel to max pixel index
    w_max_px_ind = max(w-1, 1)
    h_max_px_ind = max(h-1, 1)

    x0 = contour[0]['x']
    y0 = contour[0]['y']
    x1 = contour[0]['x']
    y1 = contour[0]['y']
    for pt in contour:
        x0 = min(x0, pt['x'])
        y0 = min(y0, pt['y'])
        x1 = max(x1, pt['x'])
        y1 = max(y1, pt['y'])

    x0 = dtype(x0 * w_max_px_ind)
    y0 = dtype(y0 * h_max_px_ind)
    x1 = dtype(x1 * w_max_px_ind)
    y1 = dtype(y1 * h_max_px_ind)
    return (x0, y0), (x1, y1)