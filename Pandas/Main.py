from Jobs.ProcessData import *

processData = ProcessData()

joinedTablesResult = processData.joiningTables(processData.getData('customers'),processData.getData('sales'),processData.getData('orders'),processData.getData('items'))       

aggJoinedTablesResult = joinedTablesResult.groupby(['customer_id', 'age','item_name']).agg(Quantity=('quantity', 'sum')).reset_index()

fitleredAggJoinedTablesResult = aggJoinedTablesResult[aggJoinedTablesResult['age'].between(18, 35)]

finalDataSet = processData.assignNamingAndDatatypes(fitleredAggJoinedTablesResult)

finalDataSet.to_csv(processData.getFilename(), sep=Seperator,index=False)

