
def flatten(list_of_lists):
    """
    Takes a list of lists and flattens everything down to a single list
    :param list_of_lists: A list where the elements are lists
    """
    return [item for sublist in list_of_lists for item in sublist]
