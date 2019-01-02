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

def min_conf_filter_predictions(filter_dict, preds, confs):
    """ Filter our predictions based on per label confidence thresholds

    Args:
        filter_dict (dict): A dict of strings or ints that get mapped an int id
        preds (list): A list of predicted labels (strings) or classes (ints)
        confs (list): A list of floats associated to confidence 
                        values for each predicted class 

    Returns:
        qualifying_preds (list): A list of elements from preds 
                                    that have qualifying confidence
    """
    qualifying_preds = []
    for pred, conf in zip(preds, confs):
        min_conf = filter_dict.get(pred)
        if min_conf is None:
            min_conf = filter_dict.get(self.labels.get(pred))

        if min_conf is None:
            min_conf = 0
        if conf >= min_conf:
            qualifying_preds.append(pred)
    return qualifying_preds