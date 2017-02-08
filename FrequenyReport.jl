###############################Libraries################################################
using DataFrames, Distributions, Base.Dates
###############################Parameters###############################################

##Non-Kantar or Digital (Default not required)
#BinSize=["1" 1 1; "2" 2 2; "3" 3 3; "4" 4 4; "5" 5 5; "6" 6 6; "7" 7 7; "8" 8 8; "9" 9 9; "10" 10 10000000]

#Kantar
#BinSize= ["1" 1 1; "2-4" 2 4; "5-10" 5 10; "11+" 11 10000000] 

#TV Only
BinSize=["1" 1 1; "2" 2 2; "3" 3 3; "4" 4 4; "5" 5 5; "6" 6 6; "7" 7 7; "8" 8 8; "9" 9 9; "10-19" 10 19; "20-29" 20 29; "30-39" 30 39; "40-49" 40 49; "50+" 50 10000000]

#inPath=""
inPath=pwd()

#Exposure data
ExposureData="Exposure_Freq.csv"

#Purchase data
PurchaseData="Purchase_Freq.csv"


###############################Import Data###############################################

# Read exposure data
exp_data = readtable(string(inPath,"/",ExposureData),header=false)
names!(exp_data,[:ExposureID, :IRIWeek, :IMP, :ExposedFlag])
println(head(exp_data))
println(showcols(exp_data))
@printf("Exposure data imported= %d\n", size(exp_data,1))  
#Exposure data imported= 1540475

# Read purchase data
pur_data = readtable(string(inPath,"/",PurchaseData),header=false)
names!(pur_data,[:IRIWeek, :ExposureID])
println(head(pur_data))
println(showcols(pur_data))
@printf("Purchase data imported= %d\n", size(pur_data,1))  
#Purchase data imported= 549

##datetime2unix(dt::DateTime): Takes the given DateTime and returns the number of seconds since the unix epoch 1970-01-01T00:00:00 as a Float64

##Add additional information to exposure data
exp_data1=exp_data
exp_data1[ :time1]=  datetime2unix(DateTime(exp_data1[ :IRIWeek], DateFormat("mm/dd/yyyy  HH:MM:SS")))
exp_data1[ :timestamp]= DateTime(exp_data1[ :IRIWeek], DateFormat("mm/dd/yyyy  HH:MM:SS"))
println(head(exp_data1,10))
println(showcols(exp_data1))
by(exp_data1, [:IRIWeek], nrow)

##Add additional information to purchase data
pur_data1=pur_data
pur_data1[ :TRANS_DATE]=  rata2datetime(722694 + 7 * pur_data1[ :IRIWeek])
pur_data1[ :time1]=  datetime2unix(pur_data1[ :TRANS_DATE])
println(head(pur_data1,10))
println(showcols(pur_data1))
println(by(pur_data1, [:TRANS_DATE, :IRIWeek ], nrow))

###############################Generate Report###########################################

purch=sort!(pur_data1, cols = [order(:ExposureID), order(:IRIWeek), order(:TRANS_DATE)])
println(head(purch,10))

#Creating the first purchase and occasion
purcdate=purch[:,[:ExposureID, :TRANS_DATE ]]
purcdate=aggregate(purcdate, :ExposureID,  minimum)
names!(purcdate, [:ExposureID, :firstsale])
print(head(purcdate,10))

purchase=by(purch, [:ExposureID ],nrow)
names!(purchase, [:ExposureID, :occ])
print(head(purchase,10))

purch1=join(purch, purcdate, on = :ExposureID, kind = :inner)
print(head(purch1,10))
	   
#Reading the date values 
exp=sort!(exp_data1, cols = [order(:ExposureID), order(:timestamp), order(:time1), order(:IMP)])
println(head(exp,10))

#Creating the first exposure and last exposure before purchase
exp_temp=join(exp, purch1, on = :ExposureID, kind = :inner)
exp_temp=exp_temp[(exp_temp[:timestamp].<=  exp_temp[:firstsale]),:]
expdate_1=exp[:,[:ExposureID, :timestamp ]]
expdate_1=aggregate(expdate_1, :ExposureID,  [minimum])
names!(expdate_1, [:ExposureID, :firstexpo])
expdate_2=aggregate(exp_temp[:,[:ExposureID,:timestamp]], :ExposureID,  [maximum])
names!(expdate_2, [:ExposureID, :latestexpo])
expdate=join(expdate_1, expdate_2, on = :ExposureID, kind = :outer)

exp1=join(exp, expdate, on = :ExposureID, kind = :inner)
print(head(exp1,10))

#Calculating difference of firstsale and first exposure

#exp_data1[exp_data1[:ExposureID] .==1327551532,:]
#purch1[purch1[:ExposureID] .==1327551532,:]

lastexpo=join(exp1, purcdate, on = :ExposureID, kind = :inner)
lastexpo[:diff]=lastexpo[:firstsale]-lastexpo[:timestamp]
print(head(lastexpo,10))

#dt=DateTime(1970, 1, 1)
lastexpo1=lastexpo[(map(Int,lastexpo[:diff]).>=  0),:]
print(head(lastexpo1,10))


#Experian Ids and the first exposuredate/Impressions
expodate=expdate
print(head(expodate,10))
expocnt=by(exp1, [:ExposureID], exp1->sum(exp1[:IMP]))
names!(expocnt, [:ExposureID, :Exposures])
print(head(expocnt,10))

bef_Purch=lastexpo1[(lastexpo1[:timestamp] .== lastexpo1[:latestexpo] ) , [:ExposureID,:latestexpo]] 
bef_cnt=by(lastexpo1, [:ExposureID], lastexpo1->sum(lastexpo1[:IMP]))
names!(bef_cnt, [:ExposureID, :Exposures_To_1st_Buy])

#Merging first and last exposure, first purchase, purchase occasion information
combined=join(expodate[:,[:ExposureID,:firstexpo]],expocnt, on = :ExposureID, kind = :inner)
combined=join(combined,purcdate, on = :ExposureID, kind = :inner)
combined=join(combined,purchase, on = :ExposureID, kind = :left)
combined=join(combined,bef_Purch, on = :ExposureID, kind = :left)
combined=join(combined,bef_cnt, on = :ExposureID, kind = :left)

#Removing NAs from Exposures_To_1st_Buy
combined[isna(combined[:Exposures_To_1st_Buy]) ,  :Exposures_To_1st_Buy]=0

#Calculate days/weeks difference between first purchase and last exposure to purchase
combined[:day_gap]=0
combined[!isna(combined[:latestexpo]) ,  :day_gap] =map(Int,map(Int,(combined[!isna(combined[:latestexpo]) , :firstsale]-combined[!isna(combined[:latestexpo]) , :latestexpo])/86400000)+1)
combined[:day_gap_in_weeks]=combined[:day_gap]/7

combined[:week_gap]="Pre"
combined[(combined[:day_gap_in_weeks] .> 0) & (combined[:day_gap_in_weeks]  .< 2)  , :week_gap] = "1 Week (or less)"
combined[(combined[:day_gap_in_weeks] .>=2 ) & (combined[:day_gap_in_weeks]  .< 3)  , :week_gap] = "2 Weeks"
combined[(combined[:day_gap_in_weeks] .>=3) & (combined[:day_gap_in_weeks]  .< 4)  , :week_gap] = "3 Weeks"
combined[(combined[:day_gap_in_weeks] .>=4) & (combined[:day_gap_in_weeks]  .< 5)  , :week_gap] = "4 Weeks"
combined[(combined[:day_gap_in_weeks] .>=5) & (combined[:day_gap_in_weeks]  .< 6)  , :week_gap] = "5 Weeks"
combined[(combined[:day_gap_in_weeks] .>=6) & (combined[:day_gap_in_weeks]  .< 7)  , :week_gap] = "6 Weeks"
combined[(combined[:day_gap_in_weeks] .>=7) & (combined[:day_gap_in_weeks]  .< 8)  , :week_gap] = "7 Weeks"
combined[(combined[:day_gap_in_weeks] .>=8) & (combined[:day_gap_in_weeks]  .< 9)  , :week_gap] = "8 Weeks"
combined[(combined[:day_gap_in_weeks] .>=9) & (combined[:day_gap_in_weeks]  .< 10)  , :week_gap] = "9 Weeks"
combined[(combined[:day_gap_in_weeks] .>=10) , :week_gap] = "Over 10 Weeks"
print(head(combined,10))


#Reindexing and renaming columns in combined data
combined=sort!(combined, cols = [order(:ExposureID)])
combined=hcat(combined,collect(1:size(combined,1)))
combined=combined[:,[:x1, :ExposureID, :firstexpo, :occ, :firstsale, :Exposures, :latestexpo, :Exposures_To_1st_Buy, :day_gap, :day_gap_in_weeks, :week_gap]]
names!(combined, [:Obs, :ExposureID, :First_Exposure, :Purchase_Occasions, :First_Purchase_Weekending, :Exposures, :Date_last_exposure_before_1st_buy, :Number_exposure_before_1st_buy, :Days_between_last_exposure_first_buy, :Weeks_between_last_exposure_first_buy, :Time])
writetable(pwd()*"/IRI_Buyer_Report_Digital.csv",combined,header=true)
writetable(pwd()*"/IRI_Buyer_Report_Kantar_TV.csv",combined,header=true)

#combined[(combined[:ExposureID] .==1097028728) | (combined[:ExposureID] .==1121445480),:]
#combined[ (combined[:ExposureID] .==1033228881) | (combined[:ExposureID] .==1010736962) | (combined[:ExposureID] .==1090743949) ,:]


#exp[exp[:ExposureID] .==1097028728,:]
#purcdate[purcdate[:ExposureID] .==1097028728,:]
#combined[combined[:ExposureID] .==1097028728,:]



#Calculating experian by frequency of IMPs (Total_Freq)
expocnt_1 =deepcopy( expocnt)
expocnt_1[(expocnt_1[:Exposures] .>=10)   , :Exposures] = 10
Total_Freq=by(expocnt_1, [:Exposures], nrow)
names!(Total_Freq, [:Exposures, :HHs])
Total_Freq[:Percentage_of_Total_HHs]=Total_Freq[:HHs]/sum(Total_Freq[:HHs])
Total_Freq=hcat(Total_Freq,collect(1:size(Total_Freq,1)))
Total_Freq=Total_Freq[:,[:x1, :Exposures, :HHs, :Percentage_of_Total_HHs]]
names!(Total_Freq, [:Obs, :Exposures, :HHs, :Percentage_of_Total_HHs])
writetable(pwd()*"/Total_Freq_Digital.csv",Total_Freq,header=true)


#Calculating Maximum impression possible by frequency of IMPs (Cum IMPs)
size(expocnt)
Cum_IMPs=by(expocnt, [:Exposures], nrow)
names!(Cum_IMPs, [:Exposures, :HHs])
Cum_IMPs[:IMPs_Served]=map(Int,Cum_IMPs[:HHs] .* Cum_IMPs[:Exposures])
Cum_IMPs[:CUM_IMPs_Served]=cumsum( Cum_IMPs[:IMPs_Served])
Cum_IMPs=hcat(Cum_IMPs,collect(1:size(Cum_IMPs,1)))  
Cum_IMPs[ (Cum_IMPs[:x1] .>=Cum_IMPs[:Exposures]),[:x1,:Exposures ]]
Cum_IMPs[:IMPs_served_capped]=sum(Cum_IMPs[:HHs])
for row in 2:size(Cum_IMPs,1)
              Cum_IMPs[row,:IMPs_served_capped]=((Cum_IMPs[row,:Exposures])* sum(Cum_IMPs[row:size(Cum_IMPs,1),:HHs]))+Cum_IMPs[row-1,:CUM_IMPs_Served]
end
Cum_IMPs=Cum_IMPs[:,[:x1, :Exposures, :HHs, :IMPs_Served, :CUM_IMPs_Served, :IMPs_served_capped]]
names!(Cum_IMPs, [:Obs, :Exposures, :HHs, :IMPs_Served, :CUM_IMPs_Served, :IMPs_served_capped])
head(Cum_IMPs,10)
writetable(pwd()*"/Cum_IMPs_Digital.csv",Cum_IMPs,header=true)



#Calculate buyer frequency
buyer_freq=deepcopy(combined[:,[:Exposures]])
buyer_freq[buyer_freq[:Exposures] .>=10,:Exposures]=10
buyer_freq_1=by(buyer_freq, [:Exposures], nrow)
buyer_freq_1[:Obs]=collect(1:size(buyer_freq_1,1))
buyer_freq_1[:Percentage_of_buying_HHs]=buyer_freq_1[:x1]/sum(buyer_freq_1[:x1]) 
buyer_freq_1=buyer_freq_1[:,[:Obs,:Exposures,:x1,:Percentage_of_buying_HHs]]
names!(buyer_freq_1,[:Obs,:Exposures,:HHs,:Percentage_of_buying_HHs])
head(buyer_freq_1,10)
writetable(pwd()*"/Buyer_Frequency_Digital.csv",buyer_freq_1,header=true)



#Calculate Time - 1st buy & last exposure
buyer_exposure=deepcopy(combined[:,[:Time,:Number_exposure_before_1st_buy]])
buyer_exposure_1=by(buyer_exposure, [:Time], nrow)
buyer_exposure_1[:Obs]=collect(1:size(buyer_exposure_1,1))
buyer_exposure_1[:Percentage_of_total_buying_HHs]=0.0
buyer_exposure_1[buyer_exposure_1[:Time] .!="Pre",:Percentage_of_total_buying_HHs]=buyer_exposure_1[buyer_exposure_1[:Time] .!="Pre",:x1]/sum(buyer_exposure_1[buyer_exposure_1[:Time] .!="Pre",:x1])
buyer_exposure_1=hcat(buyer_exposure_1,by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[:Number_exposure_before_1st_buy])))
buyer_exposure=join(buyer_exposure,by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[:Number_exposure_before_1st_buy]) .+ 2.35 .* std(buyer_exposure[:Number_exposure_before_1st_buy]) ),on = :Time, kind = :left)
buyer_exposure_2=by(buyer_exposure, [:Time], buyer_exposure->mean(buyer_exposure[buyer_exposure[:Number_exposure_before_1st_buy] .<=buyer_exposure[:x1],:Number_exposure_before_1st_buy]))
buyer_exposure_final=join(buyer_exposure_1,buyer_exposure_2, on = :Time, kind = :left)
buyer_exposure_final=buyer_exposure_final[:,[:Obs, :Time, :x1, :Percentage_of_total_buying_HHs, :x1_1, :x1_2]]
names!(buyer_exposure_final,[:Obs,:Time,:Buying_HHs,:Percentage_of_total_buying_HHs, :Avg_Exposures_to_1st_buy, :Avg_Exposures_to_1st_buy_without_outliers])
head(buyer_exposure_final,10)
writetable(pwd()*"/Time_1st_buy_and_last_exposure_Digital.csv",buyer_exposure_final,header=true)
writetable(pwd()*"/Time_1st_buy_and_last_exposure_Kantar_TV.csv",buyer_exposure_final,header=true)



#Calculate 1st Buy by Frequency
Exposed_Buyer=deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!=0,[:Number_exposure_before_1st_buy]])
Exposed_Buyer[Exposed_Buyer[:Number_exposure_before_1st_buy] .>=10,:Number_exposure_before_1st_buy]=10
Exposed_Buyer_1=by(Exposed_Buyer, [:Number_exposure_before_1st_buy], nrow)
Exposed_Buyer_1[:Cum_1st_purchases_capped]=cumsum( Exposed_Buyer_1[:x1])
Exposed_Buyer_1[:Percentage_of_total_1st_purchases]=Exposed_Buyer_1[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1[:x1])
Exposed_Buyer_1[:Obs]=collect(1:size(Exposed_Buyer_1,1))
Exposed_Buyer_final=Exposed_Buyer_1[:,[:Obs, :Number_exposure_before_1st_buy, :x1, :Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]]
names!(Exposed_Buyer_final,[:Obs,:Frequency,:Buying_HHs,:Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases])
head(Exposed_Buyer_final,10)
writetable(pwd()*"/1st_Buy_by_Frequency_Digital.csv",Exposed_Buyer_final,header=true)

##################################Kantar or TV Only#################################

#Calculating experian by frequency of IMPs (Total_Freq)
expocnt_1_1=deepcopy(expocnt)
expocnt_1_1[:Frequency] =""
for i  in 1:length(BinSize[:, 1])
    expocnt_1_1[(expocnt_1_1[:Exposures] .>=BinSize[ i , 2]) & (expocnt_1_1[:Exposures] .<= BinSize[ i , 3])   , :Frequency] = BinSize[ i , 1]
end
expocnt_1_2=expocnt_1_1 |> groupby(:Frequency) |> [minimum, length] 
Total_Freq_1_1=sort!(expocnt_1_2[:,[:Exposures_minimum, :Frequency, :ExposureID_length]], cols = [order(:Exposures_minimum)])
Total_Freq_1_1[:Obs]=collect(1:size(Total_Freq_1_1,1))
Total_Freq_1_1=Total_Freq_1_1[:,[:Obs, :Frequency, :ExposureID_length]]
names!(Total_Freq_1_1, [:Obs, :Frequency, :HHs])
Total_Freq_1_1[:Percentage_of_Total_HHs]=Total_Freq_1_1[:HHs]/sum(Total_Freq_1_1[:HHs])
head(Total_Freq_1_1,20)
writetable(pwd()*"/Total_Freq_Kantar_TV.csv",Total_Freq_1_1,header=true)



#Calculating Maximum impression possible by frequency of IMPs (Cum IMPs)
Cum_IMPs_1_1=deepcopy(Cum_IMPs)
Cum_IMPs_1_1[:Frequency] =""
for i  in 1:length(BinSize[:, 1])
    Cum_IMPs_1_1[(Cum_IMPs_1_1[:Exposures] .>=BinSize[ i , 2]) & (Cum_IMPs_1_1[:Exposures] .<= BinSize[ i , 3])   , :Frequency] = BinSize[ i , 1]
end
Cum_IMPs_1_2=Cum_IMPs_1_1 |> groupby(:Frequency) |> [maximum,sum] 
Cum_IMPs_1_2=sort!(Cum_IMPs_1_2[:,[:Frequency, :Obs_maximum, :HHs_sum, :IMPs_Served_sum, :CUM_IMPs_Served_maximum, :IMPs_served_capped_maximum]], cols = [order(:Obs_maximum)])
Cum_IMPs_1_2[:Obs]=collect(1:size(Cum_IMPs_1_2,1))
Cum_IMPs_1_2=Cum_IMPs_1_2[:,[:Obs,:Frequency, :HHs_sum, :IMPs_Served_sum, :CUM_IMPs_Served_maximum, :IMPs_served_capped_maximum]]
names!(Cum_IMPs_1_2, [:Obs, :Frequency, :HHs, :IMPs_Served, :CUM_IMPs_Served, :IMPs_served_capped])
writetable(pwd()*"/Cum_IMPs_Kantar_TV.csv",Cum_IMPs_1_2,header=true)



#Calculate buyer frequency
buyer_freq_1_1=deepcopy(combined[:,[:Exposures]])
buyer_freq_1_1[:Frequency] =""
for i  in 1:length(BinSize[:, 1])
    buyer_freq_1_1[(buyer_freq_1_1[:Exposures] .>=BinSize[ i , 2]) & (buyer_freq_1_1[:Exposures] .<= BinSize[ i , 3])   , :Frequency] = BinSize[ i , 1]
end
buyer_freq_1_2=buyer_freq_1_1 |> groupby(:Frequency) |> [maximum,length] 
buyer_freq_1_2=sort!(buyer_freq_1_2, cols = [order(:Exposures_maximum)])
buyer_freq_1_2[:Percentage_of_buying_HHs]=buyer_freq_1_2[:Exposures_length]/sum(buyer_freq_1_2[:Exposures_length]) 
buyer_freq_1_2[:Obs]=collect(1:size(buyer_freq_1_2,1))
buyer_freq_1_2=buyer_freq_1_2[:,[:Obs,:Frequency,:Exposures_length,:Percentage_of_buying_HHs]]
names!(buyer_freq_1_2,[:Obs,:Exposures,:HHs,:Percentage_of_buying_HHs])
head(buyer_freq_1_2,20)
writetable(pwd()*"/Buyer_Frequency_Kantar_TV.csv",buyer_freq_1_2,header=true)




#Calculate 1st Buy by Frequency
Exposed_Buyer_1_1=deepcopy(combined[combined[:Number_exposure_before_1st_buy] .!=0,[:Number_exposure_before_1st_buy]])
Exposed_Buyer_1_1[:Frequency] =""
for i  in 1:length(BinSize[:, 1])
    Exposed_Buyer_1_1[(Exposed_Buyer_1_1[:Number_exposure_before_1st_buy] .>=BinSize[ i , 2]) & (Exposed_Buyer_1_1[:Number_exposure_before_1st_buy] .<= BinSize[ i , 3])   , :Frequency] = BinSize[ i , 1]
end
Exposed_Buyer_1_2=Exposed_Buyer_1_1 |> groupby(:Frequency) |> [maximum,length] 
Exposed_Buyer_1_2=sort!(Exposed_Buyer_1_2, cols = [order(:Number_exposure_before_1st_buy_maximum)])
Exposed_Buyer_1_2[:Cum_1st_purchases_capped]=cumsum( Exposed_Buyer_1_2[:Number_exposure_before_1st_buy_length])
Exposed_Buyer_1_2[:Percentage_of_total_1st_purchases]=Exposed_Buyer_1_2[:Cum_1st_purchases_capped]/sum(Exposed_Buyer_1_2[:Number_exposure_before_1st_buy_length])
Exposed_Buyer_1_2[:Obs]=collect(1:size(Exposed_Buyer_1_2,1))
Exposed_Buyer_Final_1=Exposed_Buyer_1_2[:,[:Obs, :Frequency,:Number_exposure_before_1st_buy_length,:Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases]]
names!(Exposed_Buyer_Final_1,[:Obs,:Frequency,:Buying_HHs,:Cum_1st_purchases_capped, :Percentage_of_total_1st_purchases])
head(Exposed_Buyer_Final_1,20)
writetable(pwd()*"/1st_Buy_by_Frequency_Kantar_TV.csv",Exposed_Buyer_Final_1,header=true)

println("Frequency report generaged\n")

