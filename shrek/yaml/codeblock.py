class CodeBlock:
    def __init__(self):
        self.block = None

def buildCodeBlock(key,block):
    c = CodeBlock()
    c.block = block
    return c
        

