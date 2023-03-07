def get_free_key(suggest_key, some_dict):
    ans = suggest_key
    i = 0
    while ans in some_dict:
        i += 1
        ans = suggest_key+'_'+str(i)
    return ans


def set_on_free_key(d, k, v):
    freekey = get_free_key(k, d)
    d[freekey] = v

