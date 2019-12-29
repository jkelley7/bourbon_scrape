def remove_u200d(list_):
    """Remove u200d values in a list"""
    list_ = [i.strip('\u200d') for i in list_]
    return list_

def pop_empties(list_,len_var = 1):
    """
    Pops off un-neccasry spaces
    :Params
    ---------
    list_ - a list to cycle thru
    
    :returns
    ---------
    list_ - your cleaned up list
    """
    for idx, ls in enumerate(list_):
        if len(list_[idx]) <= len_var:
            list_.pop(idx)
    return list_

def check_list_for_zeros(list_):
    """Check list for any string with a length of 0
    returns:
    ----------
    a list"""
    if len(min(list_,key=len)) <= 1 and len(list_) > 0:
        return check_list_for_zeros(pop_empties(list_))
    else:
        return list_

def remove_bottle_label(list_, removal_val = 'bottlelabel'):
    """Removes the words bottle label from the list
    removal value must be one word
    """
    for idx, value in enumerate(list_):
        word = (list_[idx]
            .lower()
            .strip(' ')
            .replace(' ',''))
        if word == removal_val or word == removal_val + 's' :
            list_.pop(idx)
    return list_

def split_list_prod_prod_desc(list_, month):
    """This takes our list and split it into products and product descriptions"""
    sub_prod = []
    for idx, _ in enumerate(list_):
        if list_[idx][0] == '-':
            sub_prod.append(list_[idx])
            list_.pop(idx)
        elif idx > 0:
            #product.append(test_prod[idx])
            sub_prod.append('')
        else:
            pass

    dfs = (pd.DataFrame([[month]*len(list_),list_, sub_prod])
     .T
     .rename(columns = {0:'month',
             1:'product',
             2:'product_desc'})
    )
    return dfs