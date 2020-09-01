# load packages
import pandas as pd
import numpy as np
import networkx as nx
import pickle

# load and process input data

links = pd.read_csv("ProcData\\links.csv") 

commodity = pd.read_csv("ProcData\\commodity.csv")
commodity["OD"] = commodity.Traffic_Origin + commodity.Traffic_Destination
commodity = commodity.drop_duplicates(subset=['OD'],keep='first')

blocks = pd.read_csv("ProcData\\blocks.csv")
blocks = blocks.drop(['Block_PhyDist'],axis=1)
blocks["BOD"] = blocks.Block_Origin +"_"+ blocks.Block_Destination

locations = pd.read_csv("ProcData\\nodes.csv")
# sub-select major stations
Nlocations = locations[locations.Location_Category_ID.isin([0,1,2])] 

Lblks = pd.DataFrame(list(blocks.Block_Origin)+list(blocks.Block_Destination),columns=["Location"])
Nblks = pd.DataFrame(Lblks["Location"].unique(),columns=["Location"])
Nblks = Nblks[Nblks["Location"].isin(Nlocations.Block_Destination)]

# create the graph network
G = nx.Graph()
for i in range(len(links)): 
    G.add_edge(links.iloc[i].Link_Origin_Node_ID,links.iloc[i].Link_Destination_Node_ID,weight=links.iloc[i].Link_Impedence_Length)

# remove these commodities
commodity = commodity[(~commodity["Traffic_Origin"].isin(["LOVING   NM","OLIYARD  LA","PTVANCOUVWA","LOVING   NM","RACELAND LA","DENDNI   CO","OLIYARD  LA"])) & 
                      (~commodity["Traffic_Destination"].isin(["LOVING   NM","OLIYARD  LA","PTVANCOUVWA","LOVING   NM","RACELAND LA","DENDNI   CO","OLIYARD  LA"]))]
commodity.reset_index(drop=True)   

commodity['NPath'] = np.empty((len(commodity), 0)).tolist()
commodity["NDist"] = float("NaN")
commodity["Savings"] = float("NaN")

# remove low traffic volumes
commodity = commodity[commodity.Volume>10].reset_index(drop=True)

# determine shortest path and corresponding distance for each commodity
for i in range(len(commodity)):
    commodity["NPath"][i] = nx.dijkstra_path(G,commodity.iloc[i].Traffic_Origin,commodity.iloc[i].Traffic_Destination)
    commodity["NDist"][i] = nx.dijkstra_path_length(G,commodity.iloc[i].Traffic_Origin,commodity.iloc[i].Traffic_Destination)

# determine savings from new path for each commodity
commodity["Savings"] = (commodity["Commodity_Distance_Impeded"] - commodity["NDist"])*commodity.Volume
commodity["Savings_One"] = commodity["Commodity_Distance_Impeded"] - commodity["NDist"]

# either save or read the saved file (only one of the followings)
with open("ProcData\\commodity_shortest.txt", "wb") as myFile:
        pickle.dump(commodity, myFile, protocol=2)
with open("ProcData\\commodity_shortest.txt", "rb") as myFile:
        commodity = pickle.load(myFile)

# select data that could potentialy crete more savings
commodity["OD"] = commodity.Traffic_Origin + commodity.Traffic_Destination
df = commodity[(commodity.Savings>commodity["Savings"].quantile(0.75)) & (commodity.Savings_One>commodity.Savings_One.quantile(0.5))].reset_index(drop=True)

# create the dataframe to hold info about all commodities to process later on
df['HCostE'] = np.empty((len(df), 0)).tolist()
df['Eloc'] = np.empty((len(df), 0)).tolist()
df['BlkE'] = np.empty((len(df), 0)).tolist()
   
for i in range(len(df)):   
    for j in range(1,len(df.iloc[i].NPath)-1):
        cost = int(locations[locations.Block_Destination==df.iloc[i].NPath[j]].Intermediate_Handling_Cost)      
        if df['NPath'][i][j] in list(Nblks["Location"]):
            df['Eloc'][i].append(df['NPath'][i][j])
            df['HCostE'][i].append(cost) 
    df['BlkE'][i] = [df.Eloc[i][j] for j in sorted(list(np.argsort(df.HCostE[i]))[:df.Number_of_Handlings[i]])]
    df['HCostE'][i] = [df.HCostE[i][j] for j in sorted(list(np.argsort(df.HCostE[i]))[:df.Number_of_Handlings[i]])]
    df['HCostE'][i].append(int(locations[locations.Block_Destination==df.iloc[i].Traffic_Destination].Intermediate_Handling_Cost))
    df['BlkE'][i].append(df.iloc[i]['Traffic_Destination'])
    df['BlkE'][i].insert(0,df.iloc[i]['Traffic_Origin'])
    

with open("ProcData\\dfntwk.txt", "wb") as myFile:
        pickle.dump(df, myFile, protocol=2)
with open("ProcData\\dfntwk.txt", "rb") as myFile:
        df = pickle.load(myFile)

df = df[df.Volume>=20].reset_index(drop=True)

# create list of commodities and potential blocks (possible: multiple block sequences for one commodity)
Origins = []
NewBCNT = []
Destinations = []
DCost = []
HCost = []
BSavings = []
TrOD = []
TrVol = []
for i in range(len(df)):
    nbcnt = 0
    for j in range(len(df.iloc[i].BlkE)-1): 
        NBOD = df["BlkE"][i][j] +"_"+ df["BlkE"][i][j+1]
        if NBOD not in list(blocks.BOD):
            nbcnt = nbcnt + 1
            Origins.append(df["BlkE"][i][j])
            Destinations.append(df["BlkE"][i][j+1])                    
            DCost.append(nx.dijkstra_path_length(G,df["BlkE"][i][j],df["BlkE"][i][j+1])) 
            HCost.append(df["HCostE"][i][j])            
            TrOD.append(df.iloc[i].OD)
            TrVol.append(df.iloc[i].Volume)           
    if (nbcnt==2 and Origins[-1]==Destinations[-2]):
        Origins.append(Origins[-2])
        Destinations.append(Destinations[-1])
        HCost.append(HCost[-1])
        DCost.append(nx.dijkstra_path_length(G,Origins[-2],Destinations[-1]))        
        TrOD.append(TrOD[-1])
        TrVol.append(TrVol[-1])
        NewBCNT.append(1)
        BSavings.append(df["Savings"][i] + HCost[-2]) 
    if (nbcnt==3 and Origins[-1]==Destinations[-2] and Origins[-2]==Destinations[-3]):
        Origins.append(Origins[-3])
        Destinations.append(Destinations[-1])
        HCost.append(HCost[-1])
        DCost.append(nx.dijkstra_path_length(G,Origins[-3],Destinations[-1]))        
        TrOD.append(TrOD[-1])
        TrVol.append(TrVol[-1])
        NewBCNT.append(1)
        BSavings.append(df["Savings"][i] + HCost[-2] + HCost[-3]) 
    if (nbcnt==4 and Origins[-1]==Destinations[-2] and Origins[-2]==Destinations[-3] and Origins[-3]==Destinations[-4]):
        Origins.append(Origins[-4])
        Destinations.append(Destinations[-1])
        HCost.append(HCost[-1])
        DCost.append(nx.dijkstra_path_length(G,Origins[-4],Destinations[-1]))        
        TrOD.append(TrOD[-1])
        TrVol.append(TrVol[-1])
        NewBCNT.append(1)
        BSavings.append(df["Savings"][i]  + HCost[-2] + HCost[-3] + HCost[-4])         
#    else:
    NewBCNT.extend([nbcnt]*nbcnt)
    tempSave = int(df["Savings"][i])/(nbcnt+0.001)
    BSavings.extend([tempSave]*nbcnt)
#        BSavings.append((int(df["Savings"][i]/df["Number_of_Handlings"][i])))
NBlocks=pd.DataFrame(np.zeros,index=range(len(Origins)),columns=blocks.columns)
NBlocks['Block_Origin'] = Origins
NBlocks['Block_Destination'] = Destinations
NBlocks['Block_ImpDist'] = DCost
NBlocks['Intermediate_Handling_Cost'] = HCost
NBlocks['TSavings'] = BSavings
NBlocks['TOD'] = TrOD
NBlocks['TVol'] = TrVol
NBlocks["BOD"] = NBlocks['Block_Origin'] +"_"+NBlocks['Block_Destination']
NBlocks["NBCNT"] = NewBCNT

NBlocks = NBlocks[~NBlocks.BOD.isin(blocks.BOD)].reset_index(drop=True)

# save 
#NBlocks.to_csv("ProcData\\NBlocks.csv", encoding='utf-8', index=False) 
with open("ProcData\\NBlocks.txt", "wb") as myFile:
        pickle.dump(NBlocks, myFile, protocol=2)

def agg(x):
    x["Block_Origin"]=x.iloc[0].Block_Origin
    x["Block_Destination"]=x.iloc[0].Block_Destination
    x["Block_ImpDist"]=x.iloc[0].Block_ImpDist
    x["Intermediate_Handling_Cost"]=x.iloc[0].Intermediate_Handling_Cost
    x["Block_Saving"]=sum(x.TSavings)
    x["TC"]=x.TOD.count()
    x["Vol"] = sum(x.TVol)
    x["BSH"] = sum(x.NBCNT)
    
    return(x[['BOD','Block_Origin','Block_Destination','Block_ImpDist','Intermediate_Handling_Cost','Block_Saving','TC','Vol','BSH']].iloc[0])
            
pot_blocks = NBlocks.groupby("BOD", as_index=False).apply(agg)
# proccess and create final list of potential blocks
pot_blocks["BSH"] = pot_blocks["BSH"] - pot_blocks["TC"]
pblocks = pot_blocks[(~pot_blocks["BOD"].isin(blocks.BOD)) & (pot_blocks["TC"]>=2) ].reset_index(drop=True)
pblocks = pblocks[['Block_Origin','Block_Destination','Block_ImpDist','Intermediate_Handling_Cost','BOD']]
pblocks.insert(2,"Block_PhyDist",np.zeros(len(pblocks))) 
# save final list of potential blocks
pblocks.to_csv("ProcData\\pblocks.csv", encoding='utf-8', index=False) 
# save final list of potential blocks and corresponding commodities
NBlocksP = NBlocks[NBlocks.BOD.isin(pblocks.BOD)].reset_index(drop=True)
with open("ProcData\\NBlocksP.txt", "wb") as myFile:
        pickle.dump(NBlocksP, myFile, protocol=2)