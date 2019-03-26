import os
import json
import itertools as it

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
    idmap = {}
    if not os.path.exists(idmap_filepath):
        raise FileNotFoundError("{} not found.".format(idmap_filepath))
    try:
        with open(idmap_filepath, "r") as rf:
            for row_in in rf:
                try:
                    row = row_in.split("\t")
                    assert len(row) >= 2
                except:
                    row = row_in.split(" ")
                    assert len(row) >= 2
                id = int(row[0])
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


def pandas_query_matches_props(bool_exp, props):
    """Evaluates the boolean expression on a list of properties
    
        Note: list of properties must be a pandas.DataFrame
        See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    Args:
        bool_exp (str): boolean expression
        props (pd.DataFrame): DataFrame containing list of dicts (properties)
    
    Returns:
        bool: True if bool exp evals to True, else False
    """
    log.debug(f"bool_exp: {bool_exp}")
    log.debug(f"against properties: {json.dumps(props.to_dict(), indent=2)}")
    queried_pois = props.query(bool_exp)
    log.debug(f"matches? : {not queried_pois.empty}")
    return not queried_pois.empty


def convert_props_to_pandas_query(query_props):
    """Convert a list of query dicts to a boolean expression to be used in pandas.DataFrame.query

        E.g.
            [
                {"property_type":"object", "value":"face"}, 
                {"value":"car"}
            ]
            
            is converted to
            
            '(property_type == "object") & (value == "face") | (value == "car")'
    
    Args:
        query (list[dict]): list of dictionaries containing key/values of interest
    
    Returns:
        str: boolean expression
    """
    assert isinstance(query_props, list)
    log.debug(query_props)
    bool_exp = ""
    for i, q in enumerate(query_props):
        assert isinstance(q, dict)
        for j, (k, v) in enumerate(q.items()):
            bool_exp += f'({k} == "{v}")'
            if j != len(q) - 1:
                bool_exp += " & "
        if i != len(query_props) - 1:
            bool_exp += " | "
    log.debug(f"query: {query_props} transformed into boolean expression: {bool_exp}")
    return bool_exp


def batch_generator(iterator, batch_size):
# def batch_generator(iterable, batch_size):
    """Take an iterator, convert it to a batching generator

    See: https://realpython.com/python-itertools/

    Args:
        iterator: Any iterable object where each element is a list or a tuple of length N

    Returns:
        list: A list of N batches of size `self.batch_size`. The last
                batch may be smaller than the others
    """
    # iters = [iter(iterable)] * batch_size
    # return it.zip_longest(*iters, fillvalue=None)
    batch = []
    for iteration in iterator:
        batch.append(iteration)
        if len(batch) >= batch_size:
            yield zip(*batch)
            batch = []
    if len(batch) > 0:
        yield zip(*batch)
