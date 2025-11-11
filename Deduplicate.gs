/**
 * Intended function:
 * 1. identify duplicated files from drive inventory report by their md5 checksum, 
 * omitting one copy of the file per duplicated entry using most recent upload as "authoritative"
 * 2. write the result of 1. to a table with row numbers (destinationMasterTable)
 * 3. create subtables windowing the table in 2 into tables bucketed into row count csvRowCount
 * 4. trigger bq exports for the bucketed tables to GCS
 */


/** BEFORE RUNNING:
 * 1. Create a storage bucket in google cloud in the same region as your bigquery dataset:
 *  gcloud storage buckets create gs://[YOUR_UNIQUE_BUCKET_NAME] \
 *  --location=[REGION_OR_MULTI_REGION] \
 *  --project=[YOUR_GCP_PROJECT_ID]
 * 2. Set the parameters below to match your dataset and extract requirements
 * TO USE:
 * 1. Execute createDuplicatesTable
 * 2. wait until the table shows as created in cloud console
 * 3. Execute createTempTables
 * 4. Wait until the temp tables are all visible in cloud console
 * 5. Execute initiateExtracts
 * 6. Wait for the csv files to appear in GCS storage in cloud console
 * 7. Use gcloud cli to download the csv's:
 *  gcloud config set account [YOUR EMAIL]
 *  gcloud auth login
 *  gcloud storage cp --recursive gs://[GCS_BUCKET]/ ~/Downloads
 */


const projectId=[NUMBER];
const projectName ='[YOUR PROJECT NAME]';
const dataSet = '[DATASET NAME]';
const inventoryTable ='[TABLE NAME]';
const destinationMasterTable = 'duplicates-master'; // default, change if you want
const tempTablePrefix = 'dupes-subset' // default, change if you want
const gcsBucket = 'gs://[YOUR BUCKET NAME]/' // What you selected in step 1, terminated with a slash
const csvRowCount = 400000;
const fullTableSize = 8300000; // expected/calculated size of the duplicates table 

function createDuplicatesTable() {
    const job = {
      configuration:{
        query: {
          destinationTable: {
            projectId: projectName,
            datasetId: dataSet,
            tableId: destinationMasterTable,
          },
          createDisposition: 'CREATE_IF_NEEDED',
          writeDisposition: 'WRITE_TRUNCATE',
          allowLargeResults: true,
          useLegacySql: false,
          query: `WITH dupes AS (
      SELECT
        md5_checksum,
        count(id) as cnt,
        (count(id)>1) as has_dupes,
        max(last_modified_time_micros) as most_recent_upload
      FROM
        \`${projectName}.${dataSet}.${inventoryTable}\`
      WHERE
        md5_checksum is not null AND owner.shared_drive.id is null
      GROUP BY
        md5_checksum
      HAVING 
      cnt>1
    )
    SELECT
      ROW_NUMBER() OVER (ORDER BY size_bytes DESC, inventory.md5_checksum) as row_number,
      id,
      if(owner.user.email is not null,owner.user.email,'SHARED DRIVE') as email,
      title,
      size_bytes,
      mime_type,
      dupes.md5_checksum,
      last_modified_time_micros,
      parent
    FROM
      \`${projectName}.${dataSet}.${inventoryTable}\` as inventory
      LEFT OUTER JOIN dupes
      on dupes.md5_checksum=inventory.md5_checksum
    WHERE 
      dupes.has_dupes
      and owner.shared_drive.id is null
      and last_modified_time_micros != dupes.most_recent_upload
    ORDER BY size_bytes DESC, dupes.md5_checksum`,
        }
      }
    }
  Logger.log(job)
  let jobResult = BigQuery.Jobs.insert(job, projectId);
  Logger.log(jobResult.jobReference.jobId)
}

function createTempTables(){

  for(let i = 0; i < Math.ceil(fullTableSize/csvRowCount); i+=1){
    Logger.log(i)
    const job = {
      configuration:{
        query: {
          destinationTable: {
            projectId: projectName,
            datasetId: dataSet,
            tableId: tempTablePrefix + '-' + String(i).padStart(3,'0'),
          },
          createDisposition: 'CREATE_IF_NEEDED',
          writeDisposition: 'WRITE_TRUNCATE',
          allowLargeResults: true,
          useLegacySql: false,
          query: `SELECT * FROM \`${projectName}.${dataSet}.${destinationMasterTable}\`
          WHERE row_number BETWEEN ${i*csvRowCount+1} AND ${(i+1)*csvRowCount}
          ORDER BY row_number`
        }
      }
    }
    Logger.log(job)
    let jobResult = BigQuery.Jobs.insert(job, projectId);
    Logger.log(jobResult.jobReference.jobId)
  }
}

function initiateExtracts(){
  for(let i = 0; i < Math.ceil(fullTableSize/csvRowCount); i+=1){
    const fileName=`inventory_extract_part_${String(i).padStart(3,'0')}.csv`
    const extractJob = {
      configuration: {
        extract: {
          sourceTable: {
            projectId:projectName,
            datasetId:dataSet,
            tableId: tempTablePrefix + '-' + String(i).padStart(3,'0')
          },
          destinationUri: gcsBucket+fileName, // Single, full file path in GCS
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
}
