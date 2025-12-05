const ONLY_SCAN_THROUGH=[NUMBER];

function checkBucketTables(){
  const persistenceId = PropertiesService.getScriptProperties().getProperty('persistenceId');
  const folderIdsList = JSON.parse(DriveApp.getFileById(persistenceId).getBlob().getDataAsString());
  Logger.log(folderIdsList);
  for(let i = 0; i < Math.min(Math.ceil(reorgFullTableSize/reorgCsvRowCount),ONLY_SCAN_THROUGH); i+=1){
    Logger.log(i)
    const job = {
      configuration:{
        query: {
          destinationTable: {
            projectId: projectName,
            datasetId: dataSet,
            tableId: filesBucketTablePrefix + String(i).padStart(3,'0'),
          },
          createDisposition: 'CREATE_IF_NEEDED',
          writeDisposition: 'WRITE_TRUNCATE',
          allowLargeResults: true,
          useLegacySql: false,
          query: `WITH batch as (
  select row_number, id, parent
  from \`${projectName}.${dataSet}.${filesExtractTable}\`
  where row_number BETWEEN ${i*reorgCsvRowCount+1} AND ${(i+1)*reorgCsvRowCount}
)
select 
  batch.*,
  inventory.parent
from \`${projectName}.${dataSet}.${filesExtractTable}\` as inventory
  inner join batch on inventory.id = batch.id
WHERE 
  inventory.parent=batch.parent
  OR
  (inventory.parent is NULL and batch.parent is NULL)
order by 
  row_number`
        }
      }
    }
    Logger.log(job)
    // uncomment to run.
    // let jobResult = BigQuery.Jobs.insert(job, projectId);
    // Logger.log(jobResult.jobReference.jobId)
  }
}
