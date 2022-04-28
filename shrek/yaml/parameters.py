class ParameterBlock:
    def __init__(self):
        self.params = ""

def buildParameterBlock(key,params):
    output=""
    for (k,v) in params.items():
        v = str(v)
        v = ''.join(v.splitlines())
        output = output + "export %s=%s\n"%(k,str(v))
    p = ParameterBlock()
    p.params = output

    # And extend this class
    for (k,v) in params.items():
        setattr(p, k, v)
        
    return p
