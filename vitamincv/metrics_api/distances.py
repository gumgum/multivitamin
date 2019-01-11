import numpy as np

def euclidean(X, Y):
    """Calcuates euclidean distance between two sets of contours

    Args:
        X (numpy.ndarray): List of points in contours. 
            Has shape (N-Samples, K-Dimensions)
        Y (numpy.ndarray): List of points in contours. 
            Has shape (M-Samples, K-Dimensions)

    Returns:
        N x M matrix of floats representing euclidean distances

    """
    X = check_dims(X)
    Y = check_dims(Y)
    assert(X.shape[1] == Y.shape[1])

    result = np.zeros((X.shape[0], Y.shape[0]))

    data = {
        True: X,
        False: Y
    }

    iterate_over_x = True
    if X.shape[0] < Y.shape[0]:
        iterate_over_x = False


    for idx in range(data[iterate_over_x].shape[0]):
        diff = data[not iterate_over_x]-data[iterate_over_x][idx]
        norm = np.linalg.norm(diff, axis=1)
        if iterate_over_x:
            result[idx, :] = norm
        else:
            result[:, idx] = norm
    return result


def _euclidean(X, Y):
    """Calcuates euclidean distance between two sets of contours

    FASTER BUT UBER MEMORY HOG

    Args:
        X (numpy.ndarray): List of points in contours. 
            Has shape (N-Samples, K-Dimensions)
        Y (numpy.ndarray): List of points in contours. 
            Has shape (M-Samples, K-Dimensions)

    Returns:
        N x M matrix of floats representing euclidean distances

    """
    X = check_dims(X)
    Y = check_dims(Y)
    assert(X.shape[1] == Y.shape[1])
    return  np.square(np.subtract.outer(X, Y).diagonal(0, 1, 3)).sum(axis=2)

def check_dims(X):
    if len(X.shape) == 1:
        X = X[np.newaxis, :]
    return X

def jaccard(X, Y, rectilinear=True):
    """Calcuates jaccard distance between two sets of contours

    ONLY RECTILINEAR CURRENTLY SUPPORTED

    Args:
        X (numpy.ndarray): List of points in contours. 
            Has shape (N-Samples, J-Points, K-Dimensions)
        Y (numpy.ndarray): List of points in contours. 
            Has shape (M-Samples, J-Points, K-Dimensions)
        rectilinear (bool): Use functions optimized of rectilinear contours.
            If True, J-Points must equal 2 and must be (top-left, bottom-right)

    Returns:
        N x M matrix of normalized floats representing jaccard distances

    """
    if rectilinear:
        jac = rectilinear_jaccard(X, Y)
    else:
        jac = None
    return jac

def rectilinear_jaccard(X, Y):
    X = check_dims(X)
    Y = check_dims(Y)
    assert(X.shape[1:] == Y.shape[1:])
    assert(X.shape[1] == 2)
    inter = intersection(X, Y)
    un = union(X, Y, inter=inter)
    return inter/un    

def rectilinear_intersection(X, Y):
    maxima = np.maximum.outer(X[:, 0, :], Y[:, 0, :]).diagonal(0, 1, 3)
    minima = np.minimum.outer(X[:, 1, :], Y[:, 1, :]).diagonal(0, 1, 3)
    delta = minima-maxima
    delta = (delta>0) * delta
    return delta.prod(axis=2)

def rectilinear_union(X, Y, inter=None):
    X_vol = rectilinear_n_volume(X)
    Y_vol = rectilinear_n_volume(Y)
    if inter == None:
        inter = intersection(X, Y)
    return X_vol*Y_vol-inter

def rectilinear_n_volume(X):
    for i in range(X.shape[2]):
        vol *= X[:, :, i]-X[:, :, i]
    return vol
