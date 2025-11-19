function listCombinedSizeStatistics(){
  for(let i = 0; i < Math.ceil(reorgFullTableSize/reorgCsvRowCount); i+=1){
    const queryConfig = {
      query:`Select SUM(size_bytes) as size from ${projectName}.${dataSet}.${filesBucketTablePrefix + String(i).padStart(3,'0')}`,
      useLegacySql:false,
      }
    let jobResult = BigQuery.Jobs.query(queryConfig,projectId);
    Logger.log(`${filesBucketTablePrefix + String(i).padStart(3,'0')} | ${jobResult.rows[0].f[0].v/1000/1000/1000/1000} TB`);
    // break;
  }
}

function deleteOldTables(){
  // delete the duplicates tables
  for(let i = 0; i < Math.ceil(fullTableSize/csvRowCount); i+=1){
    BigQuery.Tables.remove(projectName,dataSet,tempTablePrefix + '-' + String(i).padStart(3,'0'))
  }
  
  // delete the bucket tables
  for(let i = 0; i < Math.ceil(reorgFullTableSize/reorgCsvRowCount); i+=1){
    BigQuery.Tables.remove(projectName,dataSet,filesBucketTablePrefix + String(i).padStart(3,'0'))
  }
}

function materializeDocsEditorsFiles(){
    const job = {
      configuration:{
        query: {
          destinationTable: {
            projectId: projectName,
            datasetId: dataSet,
            tableId: 'editorsFiles',
          },
          createDisposition: 'CREATE_IF_NEEDED',
          writeDisposition: 'WRITE_TRUNCATE',
          allowLargeResults: true,
          useLegacySql: false,
          query: `SELECT
      id,
      if(owner.user.email is not null,owner.user.email,'SHARED DRIVE') as email,
      title,
      size_bytes,
      mime_type,
      last_modified_time_micros,
      parent
    FROM
      \`${projectName}.${dataSet}.${inventoryTable}\` as inventory
    WHERE 
      mime_type!='application/vnd.google-apps.folder' 
      AND mime_type != 'application/vnd.google-apps.shortcut'
      AND md5_checksum IS NULL
      AND owner.shared_drive is null;`
        }
      }
    }
  Logger.log(job)
  let jobResult = BigQuery.Jobs.insert(job, projectId);
  Logger.log(jobResult.jobReference.jobId)

}

function extractDocsEditorsFiles(){
  const extractJob = {
      configuration: {
        extract: {
          sourceTable: {
            projectId:projectName,
            datasetId:dataSet,
            tableId: 'editorsFiles'
          },
          destinationUri: gcsBucket+'editorsFiles.csv', // Single, full file path in GCS
          destinationFormat: 'CSV',
          printHeader: true, // Include the header row
          fieldDelimiter: ','
        },
      }
    }
    Logger.log(extractJob);
    let jobResult = BigQuery.Jobs.insert(extractJob, projectId);
    Logger.log(jobResult.jobReference.jobId)
}
