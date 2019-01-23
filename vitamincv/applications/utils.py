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
    with open(idmap_file, 'r') as rf:
        for row in rf:
            row = row.split("\t")
            labelmap[int(row[0])] = row[1].strip()
    return labelmap
