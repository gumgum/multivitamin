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
