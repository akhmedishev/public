def float_safe(s, valonex=0.0):
    if s is None:
        return valonex
    if isinstance(s, int) or isinstance(s, float):
        return s
    s = s.strip()
    if s == "":
        return valonex
    try:
        return float(s)
    except ValueError:
        try:
            return float(s.replace(',','.'))
        except ValueError:
            return valonex

