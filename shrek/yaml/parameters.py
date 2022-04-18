class ParameterBlock:
    def __init__(self):
        self.params = ""

def buildParameterBlock(key,params):
    output=""
    for (k,v) in params.items():
        output = output + "export %s=%s\n"%(k,str(v))
    p = ParameterBlock()
    p.params = output
    return p
