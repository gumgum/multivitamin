def p0p1_from_bbox_contour(contour, w=1, h=1, dtype=int):
    """Convert cv_schema `contour` into p0 and p1 of a bounding box.

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

def crop_image_from_bbox_contour(image, contour):
    """Crop an image given a bounding box contour
    
    Args:  
        image (np.array): image
        contour (dict[float]): points of a bounding box countour
    
    Returns:
        np.array: image
    """
    if contour is None:
        return image
    (x0, y0), (x1, y1) = p0p1_from_bbox_contour(contour)
    return image[y0:y1, x0:x1]