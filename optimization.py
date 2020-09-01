def func(no_newb = 20):
# load packages
    import pandas as pd
    from operator import add
    import pickle
    
# load and process input data
    # all stationsew33
    locations = pd.read_csv("ProcData\\nodes.csv")
    # all comodities: OD data
    commodity = pd.read_csv("ProcData\\commodity.csv")   
    commodity["OD"] = commodity.Traffic_Origin + commodity.Traffic_Destination
    # List of potential commodities to be modified: High volumes commodities
    with open("ProcData\\NBlocksP.txt", "rb") as myFile:
        NBlocksP = pickle.load(myFile)
    # filter commodities    
    commodity = commodity[commodity.OD.isin(NBlocksP.TOD)]
    # current block sequence for all commodities
    blk_seq = pd.read_csv("ProcData\\blkseq.csv")  
    # all traffics (WB's)
    traffic = pd.read_csv("ProcData\\traffic.csv")
    
    # sub-select blocks for filtered commodities
    blk_com = pd.merge(blk_seq,traffic[['WB_ID','Traffic_Origin_Location_ID','Traffic_Destination_Location_ID']],on=['WB_ID'],how='left')
    blk_com["BOD"] = blk_com.Block_Origin +"_"+blk_com.Block_Destination
    blk_com["TOD"] = blk_com.Traffic_Origin_Location_ID + blk_com.Traffic_Destination_Location_ID
    blk_com = blk_com.drop(['WB_ID', 'Block_Sequence_Number', 'Block_Origin', 'Block_Destination','Traffic_Origin_Location_ID', 'Traffic_Destination_Location_ID'], axis=1)
    blk_com = blk_com[blk_com.TOD.isin(commodity.OD)].reset_index(drop=True)
    blk_com = blk_com.drop_duplicates(subset=['BOD','TOD'],keep='first').reset_index(drop=True)
    
    blocks = pd.read_csv("ProcData\\blocks.csv")         
    blocks = pd.merge(blocks,locations[["Block_Destination","Intermediate_Handling_Cost"]],on="Block_Destination", how="left")    
    blocks["BOD"] = blocks.Block_Origin +"_"+ blocks.Block_Destination
    blocks = blocks[blocks.BOD.isin(blk_com.BOD)].reset_index()

    ##
    commodity["OD"] = commodity.Traffic_Origin +"_"+ commodity.Traffic_Destination  
    nc=len(commodity)  
    # list of potential blocks to add to the network 
    pblocks=pd.read_csv("ProcData\\pblocks.csv")
    # list of (potential + current) blocks
    Tblocks=pd.concat([blocks,pblocks],ignore_index=True,axis=0)   
    
    # sub-select stations that already have a block
    Oblocks = pd.DataFrame(Tblocks.Block_Origin.unique(),columns=["BOrigin"])
    Dblocks = pd.DataFrame(Tblocks.Block_Destination.unique(),columns=["BDestination"])   
    locations = locations[locations.Block_Destination.isin(Oblocks.BOrigin) | locations.Block_Destination.isin(Dblocks.BDestination)].reset_index(drop=True)
    nl=len(locations)
    npt=len(pblocks) 
    nt=len(Tblocks)     
    
    # all incoming blocks for each station
    node_inc_blocks={}
    for i in range(nl):
        node_inc_blocks[locations.iloc[i].Block_Destination] = list(Tblocks[Tblocks.Block_Destination==locations.iloc[i].Block_Destination].Block_Origin)
    # all outgoing blocks for each station  
    node_out_blocks={}
    for i in range(nl):
        node_out_blocks[locations.iloc[i].Block_Destination] = list(Tblocks[Tblocks.Block_Origin==locations.iloc[i].Block_Destination].Block_Destination)
    # impeded cost and handling costs    
    myBCosts = []
    myHCosts = []
    for i in range(nc):
        for j in range(nt):
            myBCosts.append(Tblocks.Block_ImpDist[j])
            myHCosts.append(Tblocks.Intermediate_Handling_Cost[j])           


## Optimization model: create variables, objective function, constrains, and solve##
    import cplex
    m = cplex.Cplex()
    m.objective.set_sense(m.objective.sense.minimize)
    
    # create variables
    myShipB = ["f_%s_%s" % (commodity.iloc[i].OD,Tblocks.iloc[j].BOD) for i in range(nc) for j in range(nt)]    
    myShipO = ["f_%s_%s" % ((commodity.iloc[i].Traffic_Origin+"_"+commodity.iloc[i].Traffic_Destination),("START    ST"+"_"+commodity.iloc[i].Traffic_Origin)) for i in range(nc)]
    myShipD = ["f_%s_%s" % ((commodity.iloc[i].Traffic_Origin+"_"+commodity.iloc[i].Traffic_Destination),(commodity.iloc[i].Traffic_Destination+"_"+"END      ED")) for i in range(nc)]
    myShipES = ["f_%s_%s" % (commodity.iloc[i].OD,"END      ED"+"_"+"START    ST") for i in range(nc)]
    myShip = myShipB + myShipO + myShipD +  myShipES    
    
    myABCosts = [0]*len(myShipO + myShipD +  myShipES)
    myAHCosts = [0]*len(myShipO + myShipD +  myShipES)
    myTBCosts = myBCosts + myABCosts
    myTHCosts = myHCosts + myAHCosts
    myTCosts=list(map(add, myTBCosts, myTHCosts))
    
    m.variables.add(obj=myTCosts,types=[m.variables.type.continuous]*len(myTCosts),names=myShip)
        
    myBlockB = ["x_%s_%s" % (Tblocks.iloc[i].Block_Origin,Tblocks.iloc[i].Block_Destination) for i in range(nt)] 
    myBlockO = ["x_%s_%s" % ("START    ST",Tblocks.iloc[i].Block_Origin) for i in range(nt)] 
    myBlockD = ["x_%s_%s" % (Tblocks.iloc[i].Block_Destination,"END      ED") for i in range(nt)]
    myBlockES = ["END      ED"+"_"+"START    ST"]
    myBlocks = myBlockB + myBlockO + myBlockD + myBlockES
    nmb = len(myBlocks)
   
    m.variables.add(obj=[0.0]*nmb,types=[m.variables.type.binary]*nmb,names=myBlocks)    

    # constraint for connectivity of the flow
    con1 = []   
    rhs1 = []       
    for k in range(nc):
        for j in range(nl):
            ind1 = []
            ind2 = []
            if commodity.iloc[k].Traffic_Origin==locations.iloc[j].Block_Destination:
                ind1.append("f_"+commodity.iloc[k].OD+"_"+"START    ST"+"_"+commodity.iloc[k].Traffic_Origin)
            if commodity.iloc[k].Traffic_Destination==locations.iloc[j].Block_Destination:
                ind2.append("f_"+commodity.iloc[k].OD+"_"+commodity.iloc[k].Traffic_Destination+"_"+"END      ED")            
            val1 = []
            val2 = []
            for i in range(len(node_inc_blocks[locations.iloc[j].Block_Destination])):                              
                ind1.append("f_"+commodity.iloc[k].OD+"_"+node_inc_blocks[locations.iloc[j].Block_Destination][i]+"_"+locations.iloc[j].Block_Destination)                                   
            for h in range(len(node_out_blocks[locations.iloc[j].Block_Destination])):
                ind2.append("f_"+commodity.iloc[k].OD+"_"+locations.iloc[j].Block_Destination+"_"+node_out_blocks[locations.iloc[j].Block_Destination][h])                    
            val1 = [1.0] * len(ind1)
            val2 = [-1.0] * len(ind2)
            val = val1 + val2
            ind = ind1 + ind2
            if len(ind)>0:
                con1.append([ind,val])
    rhs1 = [0.0] * len(con1)         
    m.linear_constraints.add(lin_expr = con1, senses = ["E"]*len(rhs1), rhs = rhs1)    
    
    # constrains to define the origin and destination for each shipment    
    O = []
    D = []
    V = []     
    for k in range(nc):
        O.append([["f_"+commodity.iloc[k].OD+"_START    ST_"+commodity.iloc[k].Traffic_Origin],[1]])
        D.append([["f_"+commodity.iloc[k].OD+"_"+commodity.iloc[k].Traffic_Destination+"_"+"END      ED"],[1]])
        V.append(commodity.iloc[k].Volume)   
    m.linear_constraints.add(lin_expr = O, senses=["E"]*nc, rhs=V)
    m.linear_constraints.add(lin_expr = D, senses=["E"]*nc, rhs=V)
    
    # constrain to make sure that we only allow flow on the chosen blocks 
    F = []        
    for i in range(nt):
        temp = "x_"+Tblocks.iloc[i].Block_Origin+"_"+Tblocks.iloc[i].Block_Destination
        for k in range(nc):
            F.append([["f_"+commodity.iloc[k].OD+"_"+Tblocks.iloc[i].Block_Origin+"_"+Tblocks.iloc[i].Block_Destination,temp],[1,-commodity.iloc[k].Volume]])                         
    rhs2 = [0] * len(F)            
    m.linear_constraints.add(lin_expr = F, senses=["L"]*len(F), rhs = rhs2) 
    
    # constraint only use if we want to force the model to remove current blocks
#    Xb = []
#    for i in range(nb):
#        Xb.append("x_"+blocks.iloc[i].Block_Origin+"_"+blocks.iloc[i].Block_Destination)
#    val6 = [1.0] * nb
#    con6=  [[Xb,val6]]   
#    m.linear_constraints.add(lin_expr = con6, senses=["E"], rhs = [nb]) 
        
    # costraint to choose desired number of new blocks 
    Xp = []
    for i in range(npt):
        Xp.append("x_"+pblocks.iloc[i].Block_Origin+"_"+pblocks.iloc[i].Block_Destination)
    val7 = [1.0] * npt
    con7=  [[Xp,val7]]   
    m.linear_constraints.add(lin_expr = con7, senses=["L"], rhs = [no_newb]) 
    
## solve and save the result
    m.solve()
    print("Solution status = ", m.solution.get_status())    
    print("Cost =  ", m.solution.get_objective_value())     
    variable_values = m.solution.get_values()             
    variable_names  = m.variables.get_names()
    
    with open('variable_values.txt', 'w') as filehandle:
        for n in variable_values:
            filehandle.write('%s\n' % n)

    with open('variable_names.txt', 'w') as filehandle:
        for n in variable_names:
            filehandle.write('%s\n' % n)

## read the saved results ##
    variable_values=[]
    with open('variable_values.txt', 'r') as filehandle:
        for line in filehandle:
            # remove linebreak which is the last character of the string
            currentPlace = line[:-1]
            # add item to the list
            variable_values.append(float(currentPlace))   

    variable_names=[]
    with open('variable_names.txt', 'r') as filehandle:
        for line in filehandle:
            # remove linebreak which is the last character of the string
            currentPlace = line[:-1]
            # add item to the list
            variable_names.append(currentPlace)  
            
## post processing ##    
    used_values = []
    used_names = []
    for i in range(len(variable_names)):
        if variable_values[i]>0.1:
            used_names.append(variable_names[i])
            used_values.append(variable_values[i])
    new_names = []
    new_values = []
    new_blocks = []
    aff_traffic = []
    for i in range(len(used_names)):
        if (used_names[i][26:] in list(pblocks.BOD)):
            new_names.append(used_names[i])
            new_values.append(used_values[i])
            new_blocks.append(used_names[i][26:])
            aff_traffic.append(used_names[i][2:25])            

    new_blocks = list(set(new_blocks))
    
    Results = pd.DataFrame(new_names,columns=["Names"])
    Results["Values"] = new_values    
    Results["New_Block"] = Results["Names"].str[26:]
    Results["OD"] = Results["Names"].str[2:13] + Results["Names"].str[14:25]
    Results= Results.drop(["Names","Values"],axis=1)
    
    with open("ProcData\\dfntwk.txt", "rb") as myFile:
            df = pickle.load(myFile)
    df = df[df.Volume>=20].reset_index(drop=True)
    
    Results = pd.merge(Results,df[['OD','Volume','Path','Commodity_Cost_Impeded']],on=['OD'],how='left') 
    Results = Results.rename(columns={'Commodity_Cost_Impeded': "Imp_Cost"}).reset_index(drop=True)
    
    bseq_atraff = []
    for i in range(len(used_names)):
        if used_names[i][2:25] in list(aff_traffic)and len(used_names[i])>40 :
            bseq_atraff.append(used_names[i])
    
    bseq_atraff = pd.DataFrame(bseq_atraff,columns=['Name'])
    bseq_atraff["OD"] = bseq_atraff["Name"].str[2:13] + bseq_atraff["Name"].str[14:25]
    bseq_atraff["TO"] = bseq_atraff["Name"].str[2:13] 
    bseq_atraff["TD"] = bseq_atraff["Name"].str[14:25]
    bseq_atraff["Block"] = bseq_atraff["Name"].str[26:]
    bseq_atraff["BO"] = bseq_atraff["Name"].str[26:37]
    bseq_atraff["BD"] = bseq_atraff["Name"].str[38:]
    bseq_atraff = bseq_atraff.drop(["Name"],axis=1)
    bseq_atraff =bseq_atraff[~bseq_atraff["Block"].str.contains("START    ST")]
    bseq_atraff =bseq_atraff[~bseq_atraff["Block"].str.contains("END      ED")] 
    bseq_atraff['Seq'] = 7    
    bseq_atraff.loc[bseq_atraff['BO'] == bseq_atraff['TO'], 'Seq'] = 0
    bseq_atraff.loc[bseq_atraff['BD'] == bseq_atraff['TD'], 'Seq'] = 10
    bseq_atraff = pd.merge(bseq_atraff,Tblocks[['BOD','Block_ImpDist','Intermediate_Handling_Cost']],
                           left_on=['Block'],right_on=['BOD'],how='left').reset_index(drop=True)
    bseq_atraff =pd.merge(bseq_atraff,Results,on=['OD'],how='left').reset_index(drop=True) 
    
    def aggregate(x): 
        x=x.sort_values(by=['Seq'],ascending=True).reset_index(drop=True)
        x.set_value(len(x)-1,"Seq",len(x)-1)
        x.loc[x['Seq'] == 7, 'Seq'] = len(x)-2      
        for i in range(0,len(x)-1):
            x=x.sort_values(by=['Seq'],ascending=True).reset_index(drop=True)               
            for j in range(i+1,len(x)-1):
                if x.iloc[i].BD==x.iloc[j].BO:
                    x.set_value(j,"Seq",x.iloc[i].Seq+1)                    
        x= x.sort_values(by=['Seq']) 
        path=""
        for i in range(len(x)):
            path=path+x.iloc[i].BO+": "
        path=path+x.iloc[-1].BD
        x["New_Path"]=path  
        x["New_Imp_Dist"] = sum(x.Block_ImpDist)
        x["New_Hand_Cost"] = sum(x.Intermediate_Handling_Cost[:-1])             
        x["New_Imp_Cost"] = x["New_Imp_Dist"] + x["New_Hand_Cost"]
        x["Order"] = x['Seq'] - x.index[x['Block'] == x['New_Block']].tolist()[0] 
        x["New_Block"]=x.iloc[0].New_Block
        x["Volume"]=x.iloc[0].Volume
        x["Path"]=x.iloc[0].Path
        x["Imp_Cost"]=x.iloc[0].Imp_Cost
        
        return(x[['OD','New_Path','Block','BO','BD','Seq','New_Imp_Cost','Order','New_Block',
                  'Volume','Path','Imp_Cost']])         
    
    bseq_atraff = bseq_atraff.groupby(["OD","New_Block"], as_index=False).apply(aggregate)
    bseq_atraff = bseq_atraff.reset_index(drop=True)    
    bseq_old = bseq_atraff.drop_duplicates(subset=['OD','New_Path','New_Imp_Cost','New_Block',
                                               'Volume','Path','Imp_Cost'],keep='first')
    blk_old = blk_com[blk_com.TOD.isin(bseq_old.OD)].reset_index(drop=True)
    bseq_old = pd.merge(blk_old,bseq_old,left_on=['TOD'],right_on=['OD'],how='left')
    bseq_old['Block'] = bseq_old['BOD']
    bseq_old['BO'] = bseq_old['BOD'].str[:11]
    bseq_old['BD'] = bseq_old['BOD'].str[12:]
    bseq_old = bseq_old.drop(["BOD","TOD"],axis=1)
    
    bseq_old['Seq'] = 7    
    bseq_old.loc[bseq_old['BO'] == bseq_old['OD'].str[:11], 'Seq'] = 0
    bseq_old.loc[bseq_old['BD'] == bseq_old['OD'].str[11:], 'Seq'] = 10
    
    def Paggregate(x):
#        For blocks changed in new path
        tempBB = bseq_atraff[bseq_atraff.OD==x.iloc[0].OD]
        Same = []
#        Till Here
        x=x.sort_values(by=['Seq'],ascending=True).reset_index(drop=True)
        x.set_value(len(x)-1,"Seq",len(x)-1)
        x.loc[x['Seq'] == 7, 'Seq'] = len(x)-2      
        for i in range(0,len(x)-1):
            x=x.sort_values(by=['Seq'],ascending=True).reset_index(drop=True)               
            for j in range(i+1,len(x)-1):
                if x.iloc[i].BD==x.iloc[j].BO:
                    x.set_value(j,"Seq",x.iloc[i].Seq+1) 
#           Also Here
            if x.iloc[i].Block in list(tempBB.Block):
                Same.append(0)
            else: 
                Same.append(1)
#           Till Here
        x= x.sort_values(by=['Seq'])  
        if x.iloc[len(x)-1].Block in list(tempBB.Block):
            Same.append(0)
        else:
            Same.append(1)
#        And this
        x["Same"] = Same
        
        return (x[['OD', 'New_Path', 'Block', 'BO', 'BD', 'Seq', 'New_Imp_Cost', 'Order', 'New_Block', 
                   'Volume', 'Path', 'Imp_Cost','Same']])
             
    bseq_old = bseq_old.groupby("OD", as_index=False).apply(Paggregate)
    bseq_old['Seq'] = bseq_old['Seq'] + 10
    bseq_old['Current'] = 1
    bseq_old['Order'] = bseq_old['Seq']
    bseq_atraff['Current'] = 0
    bseq_atraff['Same'] = 0
    
    bseq_atraff = pd.concat([bseq_atraff,bseq_old],ignore_index=True,axis=0) 
    
    newdf1 = bseq_atraff.copy()
    newdf1["Station"] = newdf1["BO"]    
    newdf2 = bseq_atraff.copy()
    newdf2["Station"] = newdf2["BD"]
    newdf = pd.concat((newdf1,newdf2), ignore_index = True)
    newdf = newdf.sort_values(by=['Current','OD','New_Block','Seq'],ascending=True).reset_index(drop=True)
    
    LatLong = pd.read_csv("ProcData\\ESDLatLong.csv") 
    newdf = pd.merge(newdf,LatLong,on=['Station'],how='left')    
    newdf.to_csv("ProcData\\Data_20_20.csv", encoding='utf-8', index=False)