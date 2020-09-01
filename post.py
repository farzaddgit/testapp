import pandas as pd
import numpy as np
import networkx as nx

# load input data
links = pd.read_csv("ProcData\\links.csv") 

commodity = pd.read_csv("ProcData\\commodity.csv")
commodity["OD"] = commodity.Traffic_Origin + commodity.Traffic_Destination
commodity = commodity.drop_duplicates(subset=['OD'],keep='first')

new_blocks = pd.read_csv("ProcData\\New_Blocks_20_20.csv")
new_blocks['BPath'] = np.empty((len(new_blocks), 0)).tolist()
new_blocks["BDist"] = float("NaN")
new_blocks["BO"] = new_blocks["Block"].str[:11] 
new_blocks["BD"] = new_blocks["Block"].str[12:]


blocks = pd.read_csv("ProcData\\blocks.csv")
blocks = blocks.drop(['Block_PhyDist'],axis=1)
blocks["BOD"] = blocks.Block_Origin +"_"+ blocks.Block_Destination

locations = pd.read_csv("ProcData\\nodes.csv")
Nlocations = locations[locations.Location_Category_ID.isin([0,1,2])].reset_index(drop=True) 

Lblks = pd.DataFrame(list(blocks.Block_Origin)+list(blocks.Block_Destination),columns=["Location"])
Nblks = pd.DataFrame(Lblks["Location"].unique(),columns=["Location"])
Nblks = Nblks[Nblks["Location"].isin(Nlocations.Block_Destination)]

G = nx.Graph()
for i in range(len(links)): 
    G.add_edge(links.iloc[i].Link_Origin_Node_ID,links.iloc[i].Link_Destination_Node_ID,weight=links.iloc[i].Link_Impedence_Length)

for i in range(len(new_blocks)):
    new_blocks["BPath"][i] = nx.dijkstra_path(G,new_blocks.iloc[i].BO,new_blocks.iloc[i].BD)
    new_blocks["BDist"][i] = nx.dijkstra_path_length(G,new_blocks.iloc[i].BO,new_blocks.iloc[i].BD)
    tempO = new_blocks["BPath"][i].pop(0)
    tempD = new_blocks["BPath"][i].pop(-1)
    new_blocks["BPath"][i] = [x for x in new_blocks["BPath"][i] if x in list(Nlocations.Block_Destination)]
    new_blocks["BPath"][i].append(tempD)
    new_blocks["BPath"][i].insert(0, tempO) 

new_blocks.to_csv("ProcData\\new_blocksP.csv", encoding='utf-8', index=False) 