'''
Created on 15 Jul 2020

@author: szilardbarany
'''

import pyTigerGraph as tg
from pytg.pyTGMeta import pyTGMeta

#conn = tg.TigerGraphConnection(host="http://127.0.0.1", restppPort="26900", gsPort="26240", graphname="MyGraph", username="tigergraph", password="tigergraph", apiToken="el1op7a9eqrlq4ape5t452lukv991k7h")
conn = tg.TigerGraphConnection(host="http://127.0.0.1", restppPort="9003", gsPort="14243", graphname="FraudGraph", username="tigergraph", password="tigergraph", apiToken="el1op7a9eqrlq4ape5t452lukv991k7h")
#conn = tg.TigerGraphConnection(host="http://127.0.0.1", restppPort="30900", gsPort="30240", graphname="MyGraph", username="tigergraph", password="tigergraph", apiToken="164ub0n0b9e5mvfi99ev2s4ke6ftq3dv")

meta = pyTGMeta(conn)

# print(json.dumps(meta.generateDDL(), indent=4))
res = meta.generateDDL()
print("SET exit_on_error = FALSE")
print("DROP ALL\n")
for r in res:
    first = False
    print("// " + "=" * 73)
    print("// " + r + "\n")
    
    first = True
    for s in res[r]:        
        if r in ["LoadingJobs", "Queries"] and not first:
            print("// " + "-" * 73 + "\n")
        first = False
        if isinstance(s, list):
            for s2 in s:
                print(s2)
            print()
        else:
            print(s + "\n")
print("\n// EOF")

# EOF