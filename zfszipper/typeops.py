"""
various operations on types
"""
import time

def asNameStrOrNone(obj):
    "return obj if it's a string, name field if exists or none if obj is none"
    if obj == None:
        return None
    elif isinstance(obj, str):
        return obj
    else:
        return getattr(obj, "name")

def asNameOrStr(obj):
    "return obj if obj is a string, or obj.name, error if none"
    if obj == None:
        raise ValueError("None not allowed")
    elif isinstance(obj, str):
        return obj
    else:
        return getattr(obj, "name")

def asStrOrEmpty(s):
    "return str(s) if it's not None, else empty"
    return str(s) if s != None else ""

def splitLinesToRows(lines):
    "split newline separate lines into tuple of lines"
    end = -1 if lines.endswith('\n') else len(lines)  # handle partial line
    return lines.split("\n")[0:end]

def splitTabLinesToRows(lines):
    "split newline separate lines with tab separated columns into a list of lists "
    return tuple(map(lambda l: tuple("\t".split(l)), splitLinesToRows(lines)))

def currentGmtTimeStr():
    return time.strftime("%Y-%m-%dT%T", time.gmtime())

