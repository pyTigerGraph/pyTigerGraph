# pyTigerGraph

pyTigerGraph is a Python package for connecting to TigerGraph databases. Check out the extended docs [here](https://pytigergraph.github.io/pyTigerGraph/)

## Getting Started
To download pyTigerGraph, simply run:
```pip3 install pyTigerGraph```
Once the package installs, you can import it and instantiate a connection to your database:
```py
import pyTigerGraph as tg

conn = tg.TigerGraphConnection(host="<hostname>", graphname="<graph_name>", username="<username>", password="<password>", apiToken="<api_token>")
```
If your database is not using the standard ports (or they are mapped), you can use the following arguments to specify those:
- restppPort (default 9000): [REST++ API port](https://docs.tigergraph.com/dev/restpp-api/restpp-requests)
- gsPort (default: 14240): [GraphStudio port](https://docs.tigergraph.com/ui/graphstudio/overview#TigerGraphGraphStudioUIGuide-GraphStudioOn-Premises)

For example, in case of using a local virtual machine with the ports mapped:
```py
conn = tg.TigerGraphConnection(host="localhost", restppPort=25900, gsPort=25240, graphname="MyGraph", username="tigergraph", password="tigergraph", apiToken="2aa016d747ede9gg6da3drslm98srfoj")
```

For more details on establishing a connection, read the [Getting Started](https://pytigergraph.github.io/pyTigerGraph/GettingStarted/) page.

## Example Projects

- [Connecting to TigerGraph Database with pyTigerGraph](https://colab.research.google.com/drive/1sYv3Jvc6KYsqC4D-Rxkvjh4iPnrp4rg7)

- [Predicting IPOs using Graph Convolutional Neural Networks](https://towardsdatascience.com/predicting-initial-public-offerings-using-graph-convolutional-neural-networks-42df5ce16006?source=friends_link&sk=17501f6534a0352951d118eb8b597599)

- [Using pyTigergraph With Plotly](https://colab.research.google.com/drive/1MwtdXlbxzUsVgiI2bv1U-QmV0r5ES_Q-)

- [TigerGraph to Tensorflow](https://colab.research.google.com/drive/1yXg1UTJynjLKmdCvVNm_ldvurTR6szGN)

- [Movie Prediction with Graph Convolutional Neural Networks](https://colab.research.google.com/drive/11tcL4KXXwY__TmUUTjOf6InFQMC-VsG6)

## Credits
pyTigerGraph was originally created by Parker Erickson, a Computer Science student at the University of Minnesota. Special thanks to contributors Jon Herke and Szilard Barany of TigerGraph. Read [this](docs/CONTRIBUTING.md) to learn more about how you can contribute.
