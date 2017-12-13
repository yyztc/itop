import json

class Dump():
    def __init__(self):        
        self.DUMPDIR=os.path.join(os.getcwd(),'dumpfiles')
        if not os.path.exists(self.DUMPDIR):
            os.mkdirs(self.DUMPDIR)

    def to_json(self, filename, data):
        filepath=os.path.join(self.DUMPDIR,filename)
        with open(filepath,'w') as wf:
            wf.write(json.dumps(data))

