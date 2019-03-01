import os
import glog as log
import pandas as pd


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


def pandas_bool_exp_match_on_props(bool_exp, props):
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
    log.debug(f"against properties: {props}")
    queried_pois = props.query(bool_exp)
    log.debug(f"matches? : {not queried_pois.empty}")
    return not queried_pois.empty


def convert_list_of_query_dicts_to_bool_exp(query):
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
    assert isinstance(query, list)
    log.debug(query)
    bool_exp = ""
    for i, q in enumerate(query):
        assert isinstance(q, dict)
        for j, (k, v) in enumerate(q.items()):
            bool_exp += f'({k} == "{v}")'
            if j != len(q) - 1:
                bool_exp += " & "
        if i != len(query) - 1:
            bool_exp += " | "
    log.debug(f"query: {query} transformed into boolean expression: {bool_exp}")
    return bool_exp
