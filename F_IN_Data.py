import pandas as pd 

# column names from raw input files: links, stations, traffics, blocks, and block sequences
colnames_OrglLink = ['Link_Origin_Node_ID',
 'Link_Destination_Node_ID',
 'Link_Physical_Length',
 'Link_Impedence_Length',
 'Weight_Restriction',
 'Plate_Restriction']
colnames_OrglLocation = ['Location_ID',
 'Location_Category_ID',
 'Intermediate_Handling_Cost',
 'Daily_Blocking_Capacity',
 'If_Use_Seed_Solution_Blocking_Capacity',
 'Daily_Volume_Capacity',
 'Allow_Optimization_Outbound',
 'Allow_Optimization_Inbound',
 'New_Outbound_Threshold',
 'New_Inbound_Threshold',
 'Intermediate_Handling_Flag',
 'Associated_Node_ID',
 'Location_Memo',
 'Scenario']
colnames_OrglTraffic = ['WB_ID',
 'Traffic_Origin_Location_ID',
 'Traffic_Destination_Location_ID',
 'Traffic_Cars_Count',
 'If_In_Study',
 'Weight',
 'Plate_Clearence',
 'Scenario']
colnames_SQL_BlkPhyPath = ['Block_Origin',
 'Block_Destination',
 'Block_PhyDist',
 'Block_ImpDist',
 'Link_Origin',
 'Link_Destination',
 'Link_PhyDist',
 'Link_ImpDist',
 'Restriction1',
 'Restriction2']
colnames_OrglBlkSeq = ['Traffic_ID',
 'Block_Sequence_Number',
 'Block_Origin_Location_ID',
 'Block_Destination_Location_ID',
 'To_Optimize',
 'Scenario']

# process raw input files to desired format for the other codes especially optimization
links = pd.read_csv('InData\\OrglLink.txt',sep='\t',names=colnames_OrglLink,index_col=False)
nodes = pd.read_csv('InData\\OrglLocation.txt',sep='\t',names=colnames_OrglLocation,index_col=False)
traffic = pd.read_csv('InData\\OrglTraffic.txt',sep='\t',names=colnames_OrglTraffic,index_col=False)
blocks= pd.read_csv('InData\\SQL_BlkPhyPath.txt',sep='\t',names=colnames_SQL_BlkPhyPath,index_col=False)
blk_seq = pd.read_csv('InData\\OrglBlkSeq.txt', sep="\t", names = colnames_OrglBlkSeq,index_col=False)
links=links.drop(['Weight_Restriction','Plate_Restriction'],axis=1)
nodes=nodes.drop(['Allow_Optimization_Outbound','Allow_Optimization_Inbound','New_Outbound_Threshold',
            'New_Inbound_Threshold', 'Intermediate_Handling_Flag', 'Associated_Node_ID','Location_Memo','Scenario','If_Use_Seed_Solution_Blocking_Capacity'],axis=1)
nodes['Location_ID']=nodes['Location_ID'].str[:11]
nodes.rename(columns = {"Location_ID":"Block_Destination"}, inplace=True)
nodes.sort_values(['Block_Destination','Location_Category_ID'],ascending=[True,True])
nodes=nodes.drop_duplicates(subset ='Block_Destination',keep='first')

traffic=traffic.drop([ 'If_In_Study', 'Weight', 'Plate_Clearence', 'Scenario'], axis=1)
traffic['Traffic_Origin_Location_ID']=traffic['Traffic_Origin_Location_ID'].str[:11]
traffic['Traffic_Destination_Location_ID']=traffic['Traffic_Destination_Location_ID'].str[:11]
blocks=blocks.drop([ 'Restriction1', 'Restriction2'],axis=1)
blk_seq=blk_seq.drop(['To_Optimize','Scenario'],axis=1)
blk_seq.rename(columns={'Block_Origin_Location_ID':'Block_Origin','Block_Destination_Location_ID':'Block_Destination', 'Traffic_ID':'WB_ID'}, inplace=True)

blk_seq['Block_Origin']=blk_seq['Block_Origin'].str[:11]
blk_seq['Block_Destination']=blk_seq['Block_Destination'].str[:11]
blk_com = pd.merge(blk_seq,traffic[['WB_ID','Traffic_Origin_Location_ID','Traffic_Destination_Location_ID']],on=['WB_ID'],how='left')

blocks.dropna(inplace = True)
blocks.drop_duplicates(['Block_Origin','Block_Destination'], inplace=True)
blocks=blocks.drop(['Link_Origin','Link_Destination','Link_PhyDist','Link_ImpDist'],axis=1)

shipment=pd.merge(blk_seq,blocks[['Block_Origin', 'Block_Destination', 'Block_PhyDist', 'Block_ImpDist']],on=['Block_Origin','Block_Destination'],how='left')
shipment=pd.merge(shipment, nodes[["Block_Destination", "Intermediate_Handling_Cost"]], on="Block_Destination", how="left")
shipment=pd.merge(shipment, traffic[["WB_ID","Traffic_Origin_Location_ID", "Traffic_Destination_Location_ID"]], on="WB_ID", how="left")
shipment["OD"] = shipment.Traffic_Origin_Location_ID + shipment.Traffic_Destination_Location_ID
shipment["Volume"]=shipment.groupby(["OD"],as_index=False)["WB_ID"].transform("nunique")
shipment=shipment.drop_duplicates(subset =['Block_Sequence_Number', 'Block_Origin', 'Block_Destination', 'Block_PhyDist', 'Block_ImpDist', 
                                     'Intermediate_Handling_Cost','Traffic_Origin_Location_ID', 'Traffic_Destination_Location_ID','OD','Volume'],keep='first')

def aggregate(x):
    x["Traffic_Origin"]=x.iloc[0].Block_Origin
    x["Traffic_Destination"]=x.iloc[-1].Block_Destination
    x["Commodity_Distance"]=sum(x.Block_PhyDist)
    x["Commodity_Distance_Impeded"]=sum(x.Block_ImpDist)
    x["Handling_Cost"]=sum(x.Intermediate_Handling_Cost.iloc[:-1])
    x["Commodity_Cost"]=x.Commodity_Distance+x.Handling_Cost
    x["Commodity_Cost_Impeded"]=x.Commodity_Distance_Impeded+x.Handling_Cost
    x["Number_of_Handlings"] = len(x)-1
    x["Volume"] = x.Volume.mean()     
    path=""
    for i in range(len(x)):
        path=path+x.iloc[i].Block_Origin+": "
    path=path+x.iloc[-1].Block_Destination
    x["Path"]=path     
    return(x[['WB_ID', 'Traffic_Origin', 'Traffic_Destination', 'Commodity_Distance', 'Commodity_Distance_Impeded', 'Handling_Cost','Commodity_Cost', 'Commodity_Cost_Impeded', 'Path', 'Number_of_Handlings','Volume']].iloc[0])
        
shipment_data = shipment.groupby("WB_ID", as_index=False).apply(aggregate)   
#shipment_data["Volume"]=shipment_data.groupby(["Traffic_Origin","Traffic_Destination"],as_index=False)["WB_ID"].transform("count")
shipment_data["OD"] = shipment_data.Traffic_Origin + shipment_data.Traffic_Destination
shipment_data= shipment_data.sort_values(by=['OD','Commodity_Cost_Impeded']).drop_duplicates(subset=['OD'],keep='first')
shipment_data.to_csv("ProcData\\shipment.csv", encoding='utf-8', index=False)
commodity=shipment_data.drop(['WB_ID'],axis=1).drop_duplicates(keep='first')
commodity.sort_index(inplace=True)
commodity.to_csv("ProcData\\commodity.csv", encoding='utf-8', index=False)

# save processed files to be utilized in other codes
links.to_csv("ProcData\\links.csv", encoding='utf-8', index=False)
nodes.to_csv("ProcData\\nodes.csv", encoding='utf-8', index=False)
traffic.to_csv("ProcData\\traffic.csv", encoding='utf-8', index=False)
blk_seq.to_csv("ProcData\\blkseq.csv", encoding='utf-8', index=False)
blocks.to_csv("ProcData\\blocks.csv", encoding='utf-8', index=False)